use std::collections::{HashMap, HashSet};

use crate::data_sources::ConnectionManager;
use crate::error::{Result, SemaflowError};
use crate::flows::QueryRequest;
use crate::registry::FlowRegistry;
use crate::sql_ast::{OrderItem, SelectItem, SelectQuery, SqlRenderer, TableRef};

mod filters;
mod joins;
mod measures;
mod render;
mod resolve;

use filters::render_filter_expr;
use joins::{build_join, select_required_joins};
use measures::{
    apply_measure_filter, collect_measure_refs, resolve_measure_with_posts,
    validate_no_measure_refs,
};
use render::expr_to_sql;
use resolve::{
    build_alias_map, resolve_dimension, resolve_field_expression, resolve_measure, FieldKind,
};

pub struct SqlBuilder;

impl Default for SqlBuilder {
    fn default() -> Self {
        Self
    }
}

impl SqlBuilder {
    /// Build SQL using a provided dialect (useful for tests).
    pub fn build_with_dialect(
        &self,
        registry: &FlowRegistry,
        request: &QueryRequest,
        dialect: &dyn crate::dialect::Dialect,
    ) -> Result<String> {
        let flow = registry
            .get_flow(&request.flow)
            .ok_or_else(|| SemaflowError::Validation(format!("unknown flow {}", request.flow)))?;

        let alias_to_table = build_alias_map(flow, registry)?;
        let mut required_aliases: HashSet<String> = HashSet::new();

        let base_table = alias_to_table.get(&flow.base_table.alias).ok_or_else(|| {
            SemaflowError::Validation(format!(
                "missing base table alias {}",
                flow.base_table.alias
            ))
        })?;
        required_aliases.insert(flow.base_table.alias.clone());

        let mut query = SelectQuery::default();
        query.from = TableRef {
            name: base_table.table.clone(),
            alias: Some(flow.base_table.alias.clone()),
        };

        for dim_name in &request.dimensions {
            let (_table, alias, dimension) =
                resolve_dimension(dim_name, flow, registry, &alias_to_table)?;
            required_aliases.insert(alias.clone());
            let expr = expr_to_sql(&dimension.expression, &alias);
            query.group_by.push(expr.clone());
            query.select.push(SelectItem {
                expr,
                alias: Some(dim_name.clone()),
            });
        }

        let mut measure_defs: Vec<(String, String, &crate::flows::Measure, bool)> = Vec::new();
        for measure_name in &request.measures {
            let (_table, alias, measure) =
                resolve_measure(measure_name, flow, registry, &alias_to_table)?;
            required_aliases.insert(alias.clone());
            measure_defs.push((measure_name.clone(), alias, measure, true));
        }

        let supports_filtered_aggregates = dialect.supports_filtered_aggregates();

        // Auto-include dependent measures referenced by post_expr.
        let mut added: Vec<String> = Vec::new();
        for (_, _, measure, _) in &measure_defs {
            if let Some(post) = &measure.post_expr {
                collect_measure_refs(post, &mut added);
            }
        }
        let mut seen_extra: HashSet<String> = HashSet::new();
        for dep in added {
            if request.measures.contains(&dep) || seen_extra.contains(&dep) {
                continue;
            }
            if let Ok((_table, alias, measure)) =
                resolve_measure(&dep, flow, registry, &alias_to_table)
            {
                measure_defs.push((dep.clone(), alias.clone(), measure, false));
                required_aliases.insert(alias);
                seen_extra.insert(dep);
            }
        }

        let mut base_measure_exprs: HashMap<String, crate::sql_ast::SqlExpr> = HashMap::new();
        for (name, alias, measure, _) in &measure_defs {
            if let Some(filter) = &measure.filter {
                validate_no_measure_refs(filter)?;
            }
            if measure.post_expr.is_none() {
                let base_expr = expr_to_sql(&measure.expr, alias);
                let agg_expr =
                    apply_measure_filter(measure, base_expr, alias, supports_filtered_aggregates)?;
                base_measure_exprs.insert(name.clone(), agg_expr.clone());
                let qualified = format!("{}.{}", alias, name);
                base_measure_exprs.insert(qualified, agg_expr);
            }
        }

        if !request.filters.is_empty() {
            for filter in &request.filters {
                let (expr, kind, alias) =
                    resolve_field_expression(&filter.field, flow, registry, &alias_to_table)?;
                if matches!(kind, FieldKind::Measure) {
                    return Err(SemaflowError::Validation(
                        "filters on measures are not supported (row-level filters only)"
                            .to_string(),
                    ));
                }
                if let Some(alias) = alias {
                    required_aliases.insert(alias);
                }
                query.filters.push(render_filter_expr(expr, filter));
            }
        }

        if !request.order.is_empty() {
            for item in &request.order {
                let (expr, _, alias) =
                    resolve_field_expression(&item.column, flow, registry, &alias_to_table)?;
                if let Some(alias) = alias {
                    required_aliases.insert(alias);
                }
                query.order_by.push(OrderItem {
                    expr,
                    direction: item.direction.clone(),
                });
            }
        }

        query.limit = request.limit.map(|v| v as u64);
        query.offset = request.offset.map(|v| v as u64);

        let required_joins = select_required_joins(flow, &required_aliases, &alias_to_table)?;
        for join in required_joins {
            query.joins.push(build_join(join, &alias_to_table)?);
        }

        // Build final measure expressions (with dependency resolution for post_expr).
        let mut measure_lookup: HashMap<String, (&str, &crate::flows::Measure)> = HashMap::new();
        for (name, alias, measure, _) in &measure_defs {
            measure_lookup.insert(name.clone(), (alias.as_str(), *measure));
            let qualified = format!("{}.{}", alias, name);
            measure_lookup
                .entry(qualified)
                .or_insert((alias.as_str(), *measure));
        }
        let mut resolved_cache: HashMap<String, crate::sql_ast::SqlExpr> = HashMap::new();
        let mut stack: Vec<String> = Vec::new();
        for (name, _, _measure, requested) in &measure_defs {
            let expr = resolve_measure_with_posts(
                name,
                &measure_lookup,
                &base_measure_exprs,
                &mut resolved_cache,
                &mut stack,
            )?;
            if *requested {
                query.select.push(SelectItem {
                    expr,
                    alias: Some(name.clone()),
                });
            }
        }

        if query.select.is_empty() {
            return Err(SemaflowError::Validation(
                "query requires at least one dimension or measure".to_string(),
            ));
        }

        let renderer = SqlRenderer::new(dialect);
        Ok(renderer.render_select(&query))
    }

    /// Build SQL by resolving the flow's data source to choose a dialect.
    pub fn build_for_request(
        &self,
        registry: &FlowRegistry,
        connections: &ConnectionManager,
        request: &QueryRequest,
    ) -> Result<String> {
        let flow = registry
            .get_flow(&request.flow)
            .ok_or_else(|| SemaflowError::Validation(format!("unknown flow {}", request.flow)))?;
        let base_table = registry
            .get_table(&flow.base_table.semantic_table)
            .ok_or_else(|| {
                SemaflowError::Validation(format!(
                    "flow {} base table {} not found",
                    flow.name, flow.base_table.semantic_table
                ))
            })?;
        let data_source = connections.get(&base_table.data_source).ok_or_else(|| {
            SemaflowError::Validation(format!(
                "data source {} not registered",
                base_table.data_source
            ))
        })?;
        self.build_with_dialect(registry, request, data_source.dialect())
    }
}
