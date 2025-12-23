//! Resolved query components collected from a QueryRequest.
//!
//! This module provides the intermediate representation between
//! the raw QueryRequest and the final query plan.

use std::collections::HashMap;

use crate::error::{Result, SemaflowError};
use crate::flows::{
    Aggregation, Filter, FlowJoin, Measure, QueryRequest, SemanticFlow, SemanticTable,
};
use crate::registry::FlowRegistry;
use crate::sql_ast::{OrderItem, SqlExpr, TableRef};

/// Strategy for how a measure should be handled in multi-grain queries.
///
/// Different aggregation types require different re-aggregation strategies
/// when pre-aggregating to a join grain and then re-aggregating to the
/// requested dimension grain.
#[derive(Clone, Debug, PartialEq)]
pub enum MeasureStrategy {
    /// SUM, COUNT - pre-aggregate then SUM in final query.
    /// SUM(SUM(x)) = SUM(x), SUM(COUNT(x)) = COUNT(x)
    PreAggregatable,

    /// MIN, MAX - associative, same function works for re-aggregation.
    /// MIN(MIN(x)) = MIN(x), MAX(MAX(x)) = MAX(x)
    Associative,

    /// AVG - needs sum and count tracked separately, then ratio in final.
    /// Emits: SUM(x) AS m__sum, COUNT(x) AS m__count
    /// Final: SUM(m__sum) / SUM(m__count)
    WeightedAverage,

    /// COUNT DISTINCT - skip CTE pre-aggregation, calculate in final query.
    /// DISTINCT naturally handles fanout duplication.
    DistinctSafe,

    /// MEDIAN, STDDEV, VARIANCE, etc. - cannot be decomposed.
    /// Will error if multi-grain query is required.
    NonDecomposable,
}

/// Classify an aggregation type into its re-aggregation strategy.
pub fn classify_aggregation(agg: &Aggregation) -> MeasureStrategy {
    match agg {
        // Pre-aggregatable: SUM of SUMs = total SUM, SUM of COUNTs = total COUNT
        Aggregation::Sum | Aggregation::Count => MeasureStrategy::PreAggregatable,

        // Associative: MIN/MAX of MIN/MAX = overall MIN/MAX
        Aggregation::Min | Aggregation::Max => MeasureStrategy::Associative,

        // Weighted average: need sum and count separately
        Aggregation::Avg => MeasureStrategy::WeightedAverage,

        // Distinct-safe: DISTINCT handles fanout, skip CTE
        Aggregation::CountDistinct | Aggregation::ApproxCountDistinct => {
            MeasureStrategy::DistinctSafe
        }

        // Non-decomposable: cannot be re-aggregated correctly
        Aggregation::Median
        | Aggregation::Stddev
        | Aggregation::StddevSamp
        | Aggregation::Variance
        | Aggregation::VarianceSamp
        | Aggregation::StringAgg { .. }
        | Aggregation::ArrayAgg
        | Aggregation::First
        | Aggregation::Last => MeasureStrategy::NonDecomposable,
    }
}

/// Classify a measure's strategy based on its type.
/// Simple measures use their aggregation; formula measures are non-decomposable.
pub fn classify_measure(measure: &Measure) -> MeasureStrategy {
    if let Some(agg) = &measure.agg {
        classify_aggregation(agg)
    } else {
        // Formula measures are non-decomposable - they need full formula evaluation
        MeasureStrategy::NonDecomposable
    }
}

use super::measures::{
    apply_measure_filter, collect_measure_refs, formula_to_sql, validate_no_measure_refs,
};
use super::render::expr_to_sql;
use super::resolve::{
    build_alias_map, resolve_dimension, resolve_field_expression, resolve_measure, FieldKind,
};
use crate::expr_parser::parse_formula;

/// A resolved dimension ready for SQL generation.
#[derive(Clone, Debug)]
pub struct ResolvedDimension {
    pub name: String,
    pub alias: String,
    pub expr: SqlExpr,
}

/// A resolved measure with its base expression and metadata.
#[derive(Clone, Debug)]
pub struct ResolvedMeasure {
    pub name: String,
    pub alias: String,
    pub measure: Measure,
    pub base_expr: Option<SqlExpr>,
    pub requested: bool,
    /// Strategy for re-aggregation in multi-grain queries.
    pub strategy: MeasureStrategy,
}

/// A resolved filter ready for SQL generation.
#[derive(Clone, Debug)]
pub struct ResolvedFilter {
    pub filter: Filter,
    pub expr: SqlExpr,
    pub alias: Option<String>,
}

/// All resolved components needed to build a query.
#[derive(Clone, Debug)]
pub struct QueryComponents {
    pub base_alias: String,
    pub base_table: TableRef,
    pub base_semantic_table: SemanticTable,
    pub dimensions: Vec<ResolvedDimension>,
    pub measures: Vec<ResolvedMeasure>,
    pub base_measure_exprs: HashMap<String, SqlExpr>,
    pub filters: Vec<ResolvedFilter>,
    pub order: Vec<OrderItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub alias_to_table: HashMap<String, SemanticTable>,
    pub join_lookup: HashMap<String, FlowJoin>,
}

/// Resolve all components from a query request.
pub fn resolve_components(
    flow: &SemanticFlow,
    registry: &FlowRegistry,
    request: &QueryRequest,
    supports_filtered_aggregates: bool,
) -> Result<QueryComponents> {
    let alias_to_table_refs = build_alias_map(flow, registry)?;
    let base_alias = flow.base_table.alias.clone();
    let base_semantic_table = alias_to_table_refs.get(&base_alias).ok_or_else(|| {
        SemaflowError::Validation(format!(
            "missing base table alias {}",
            flow.base_table.alias
        ))
    })?;

    // Build owned copies for the components struct
    let alias_to_table: HashMap<String, SemanticTable> = alias_to_table_refs
        .iter()
        .map(|(k, v)| (k.clone(), (*v).clone()))
        .collect();

    let join_lookup: HashMap<String, FlowJoin> = flow
        .joins
        .values()
        .map(|j| (j.alias.clone(), j.clone()))
        .collect();

    // Resolve dimensions
    let dimensions =
        resolve_dimensions_from_request(request, flow, registry, &alias_to_table_refs)?;

    // Resolve measures
    let (measures, base_measure_exprs) = resolve_measures_from_request(
        request,
        flow,
        registry,
        &alias_to_table_refs,
        supports_filtered_aggregates,
    )?;

    // Resolve filters
    let filters = resolve_filters_from_request(request, flow, registry, &alias_to_table_refs)?;

    // Resolve order items
    let order = resolve_order_from_request(request, flow, registry, &alias_to_table_refs)?;

    let base_table = TableRef {
        name: base_semantic_table.table.clone(),
        alias: Some(base_alias.clone()),
        subquery: None,
    };

    Ok(QueryComponents {
        base_alias,
        base_table,
        base_semantic_table: (*base_semantic_table).clone(),
        dimensions,
        measures,
        base_measure_exprs,
        filters,
        order,
        limit: request.limit.map(|v| v as u64),
        offset: request.offset.map(|v| v as u64),
        alias_to_table,
        join_lookup,
    })
}

fn resolve_dimensions_from_request(
    request: &QueryRequest,
    flow: &SemanticFlow,
    registry: &FlowRegistry,
    alias_to_table: &HashMap<String, &SemanticTable>,
) -> Result<Vec<ResolvedDimension>> {
    let mut resolved = Vec::new();
    for dim_name in &request.dimensions {
        let (_table, alias, dimension) =
            resolve_dimension(dim_name, flow, registry, alias_to_table)?;
        resolved.push(ResolvedDimension {
            name: dim_name.clone(),
            alias: alias.clone(),
            expr: expr_to_sql(&dimension.expr, &alias),
        });
    }
    Ok(resolved)
}

fn resolve_measures_from_request(
    request: &QueryRequest,
    flow: &SemanticFlow,
    registry: &FlowRegistry,
    alias_to_table: &HashMap<String, &SemanticTable>,
    supports_filtered_aggregates: bool,
) -> Result<(Vec<ResolvedMeasure>, HashMap<String, SqlExpr>)> {
    let mut measures: Vec<ResolvedMeasure> = Vec::new();

    // First pass: resolve requested measures
    for measure_name in &request.measures {
        let (_table, alias, measure) =
            resolve_measure(measure_name, flow, registry, alias_to_table)?;
        let strategy = classify_measure(measure);
        measures.push(ResolvedMeasure {
            name: measure_name.clone(),
            alias,
            measure: measure.clone(),
            base_expr: None,
            requested: true,
            strategy,
        });
    }

    // Auto-include dependent measures referenced by post_expr
    let mut added: Vec<String> = Vec::new();
    for m in &measures {
        if let Some(post) = &m.measure.post_expr {
            collect_measure_refs(post, &mut added);
        }
    }

    let mut seen_extra: std::collections::HashSet<String> = std::collections::HashSet::new();
    for dep in added {
        if request.measures.contains(&dep) || seen_extra.contains(&dep) {
            continue;
        }
        if let Ok((_table, alias, measure)) = resolve_measure(&dep, flow, registry, alias_to_table)
        {
            let strategy = classify_measure(measure);
            measures.push(ResolvedMeasure {
                name: dep.clone(),
                alias: alias.clone(),
                measure: measure.clone(),
                base_expr: None,
                requested: false,
                strategy,
            });
            seen_extra.insert(dep);
        }
    }

    // Build base measure expressions for simple measures
    let mut base_measure_exprs: HashMap<String, SqlExpr> = HashMap::new();
    for m in &mut measures {
        if let Some(filter) = &m.measure.filter {
            validate_no_measure_refs(filter)?;
        }

        // Only process simple measures (those with expr + agg) that don't have post_expr
        // Formula measures will be handled separately after the parser is implemented
        if m.measure.is_simple() && m.measure.post_expr.is_none() {
            let expr = m
                .measure
                .expr
                .as_ref()
                .expect("simple measure must have expr");
            let base_expr = expr_to_sql(expr, &m.alias);
            let agg_expr = apply_measure_filter(
                &m.measure,
                base_expr,
                &m.alias,
                supports_filtered_aggregates,
            )?;
            m.base_expr = Some(agg_expr.clone());

            // Insert user-supplied name (could be qualified like "o.order_total")
            base_measure_exprs.insert(m.name.clone(), agg_expr.clone());

            // Extract and insert unqualified name for post_expr references
            let unqualified = extract_unqualified_name(&m.name);
            base_measure_exprs
                .entry(unqualified.clone())
                .or_insert_with(|| agg_expr.clone());

            // Insert fully qualified version
            let qualified = format!("{}.{}", m.alias, unqualified);
            base_measure_exprs.entry(qualified).or_insert(agg_expr);
        }
    }

    // Second pass: Handle formula measures (self-contained with inline aggregations)
    for m in &mut measures {
        if m.measure.is_formula() {
            let formula = m
                .measure
                .formula
                .as_ref()
                .expect("formula measure must have formula");

            // Parse the formula string into AST
            let ast = parse_formula(&formula.raw).map_err(|e| {
                SemaflowError::Validation(format!(
                    "Formula parse error in measure '{}': {}",
                    m.name, e
                ))
            })?;

            // Formula measures should use inline aggregations (e.g., sum(amount) / count(id)),
            // not reference other measures. The resolver rejects any measure references.
            let measure_name = m.name.clone();
            let mut resolver = |ref_name: &str| -> Result<SqlExpr> {
                Err(SemaflowError::Validation(format!(
                    "Formula measure '{}' cannot reference other measures (found '{}'). \
                     Use inline aggregations instead, e.g., 'sum(amount) / count(id)'.",
                    measure_name, ref_name
                )))
            };

            // Generate SQL from the formula AST
            let formula_expr = formula_to_sql(&ast, &m.alias, &mut resolver)?;
            m.base_expr = Some(formula_expr.clone());

            // Insert into base_measure_exprs
            let unqualified = extract_unqualified_name(&m.name);
            base_measure_exprs.insert(m.name.clone(), formula_expr.clone());
            base_measure_exprs
                .entry(unqualified.clone())
                .or_insert_with(|| formula_expr.clone());
            let qualified = format!("{}.{}", m.alias, unqualified);
            base_measure_exprs.entry(qualified).or_insert(formula_expr);
        }
    }

    Ok((measures, base_measure_exprs))
}

fn resolve_filters_from_request(
    request: &QueryRequest,
    flow: &SemanticFlow,
    registry: &FlowRegistry,
    alias_to_table: &HashMap<String, &SemanticTable>,
) -> Result<Vec<ResolvedFilter>> {
    let mut resolved = Vec::new();
    for filter in &request.filters {
        let (expr, kind, alias) =
            resolve_field_expression(&filter.field, flow, registry, alias_to_table)?;
        if matches!(kind, FieldKind::Measure) {
            return Err(SemaflowError::Validation(
                "filters on measures are not supported (row-level filters only)".to_string(),
            ));
        }
        resolved.push(ResolvedFilter {
            filter: filter.clone(),
            expr,
            alias,
        });
    }
    Ok(resolved)
}

fn resolve_order_from_request(
    request: &QueryRequest,
    flow: &SemanticFlow,
    registry: &FlowRegistry,
    alias_to_table: &HashMap<String, &SemanticTable>,
) -> Result<Vec<OrderItem>> {
    let mut order_items = Vec::new();
    for item in &request.order {
        let (expr, _, _alias) =
            resolve_field_expression(&item.column, flow, registry, alias_to_table)?;
        order_items.push(OrderItem {
            expr,
            direction: item.direction.clone(),
        });
    }
    Ok(order_items)
}

impl QueryComponents {
    /// Get aliases of all dimensions not on the base table.
    pub fn joined_dimension_aliases(&self) -> std::collections::HashSet<String> {
        self.dimensions
            .iter()
            .filter(|d| d.alias != self.base_alias)
            .map(|d| d.alias.clone())
            .collect()
    }

    /// Get aliases of all filters not on the base table.
    pub fn joined_filter_aliases(&self) -> std::collections::HashSet<String> {
        self.filters
            .iter()
            .filter_map(|f| f.alias.as_ref())
            .filter(|a| *a != &self.base_alias)
            .cloned()
            .collect()
    }

    /// Check if all measures are on the base table.
    pub fn all_measures_on_base(&self) -> bool {
        self.measures.iter().all(|m| m.alias == self.base_alias)
    }

    /// Check if measures come from multiple different tables.
    /// Returns None if all measures are from one table, or Some with the table aliases
    /// if measures span multiple tables.
    pub fn multi_table_measure_aliases(&self) -> Option<Vec<String>> {
        let unique_aliases: std::collections::HashSet<_> =
            self.measures.iter().map(|m| m.alias.clone()).collect();

        if unique_aliases.len() > 1 {
            let mut aliases: Vec<_> = unique_aliases.into_iter().collect();
            aliases.sort();
            Some(aliases)
        } else {
            None
        }
    }

    /// Check if there are any filters on joined tables.
    pub fn has_join_filters(&self) -> bool {
        self.filters
            .iter()
            .any(|f| f.alias.as_ref() != Some(&self.base_alias))
    }

    /// Check if there are any joins in the flow.
    pub fn has_joins(&self) -> bool {
        !self.join_lookup.is_empty()
    }
}

/// Extract the unqualified name from a potentially qualified name like "alias.column".
fn extract_unqualified_name(name: &str) -> String {
    if let Some(pos) = name.find('.') {
        name[pos + 1..].to_string()
    } else {
        name.to_string()
    }
}
