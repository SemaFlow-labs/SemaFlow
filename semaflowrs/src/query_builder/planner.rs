//! Query planner orchestration.
//!
//! This module coordinates the query building process using a unified flow
//! that decides between flat and pre-aggregated strategies based on fanout analysis.

use std::collections::HashSet;

use crate::error::{Result, SemaflowError};
use crate::flows::{QueryRequest, SemanticFlow};
use crate::registry::FlowRegistry;
use crate::sql_ast::{SelectItem, SelectQuery, SqlExpr, TableRef};

use super::analysis::{analyze_multi_grain, MultiGrainAnalysis};
use super::builders::{
    build_dimension_select, build_join, build_measure_selects, build_order_items,
    build_preagg_measure_selects, build_preagg_order_items, validate_non_empty_select,
};
use super::components::{resolve_components, QueryComponents};
use super::filters::render_filter_expr;
use super::joins::select_required_joins;
use super::plan::{
    CteJoin, FinalQueryPlan, FlatPlan, GrainedAggPlan, MultiGrainPlan, QueryPlan,
};

/// Build a query from a flow and request.
///
/// This is the main entry point for query building. It:
/// 1. Resolves all components from the request
/// 2. Analyzes for multi-grain pre-aggregation needs
/// 3. Builds flat, multi-grain, or legacy pre-aggregated plan
/// 4. Converts the plan to a SelectQuery
pub fn build_query(
    flow: &SemanticFlow,
    registry: &FlowRegistry,
    request: &QueryRequest,
    supports_filtered_aggregates: bool,
) -> Result<SelectQuery> {
    // Step 1: Resolve all components
    let components = resolve_components(flow, registry, request, supports_filtered_aggregates)?;

    // Step 2: Analyze for multi-grain pre-aggregation needs
    // This handles both multi-table measures AND single-table fanout risk
    let mg_analysis = analyze_multi_grain(&components, flow)?;

    // Step 3: Build appropriate plan
    let plan = if mg_analysis.needs_multi_grain {
        // Use new multi-grain path for both multi-table and single-table preagg
        build_multi_grain_plan(&components, &mg_analysis, flow, registry)?
    } else {
        build_flat_plan(&components, flow, registry)?
    };

    // Step 4: Convert to SelectQuery
    Ok(plan.to_select_query())
}

/// Build a flat query plan (standard SELECT with JOINs).
fn build_flat_plan(
    components: &QueryComponents,
    flow: &SemanticFlow,
    registry: &FlowRegistry,
) -> Result<QueryPlan> {
    let mut plan = FlatPlan::new(components.base_table.clone());

    // Collect required aliases for join pruning
    let mut required_aliases: HashSet<String> = HashSet::new();
    required_aliases.insert(components.base_alias.clone());

    // Add dimension selects and group by
    for dim in &components.dimensions {
        required_aliases.insert(dim.alias.clone());
        plan.select.push(build_dimension_select(dim));
        plan.group_by.push(dim.expr.clone());
    }

    // Add filter expressions
    for f in &components.filters {
        if let Some(alias) = &f.alias {
            required_aliases.insert(alias.clone());
        }
        plan.filters.push(render_filter_expr(f.expr.clone(), &f.filter));
    }

    // Add order by (also track aliases)
    for item in &components.order {
        // Extract alias from the expression if it's a column
        if let SqlExpr::Column { table: Some(t), .. } = &item.expr {
            required_aliases.insert(t.clone());
        }
    }
    plan.order_by = build_order_items(components);
    plan.limit = components.limit;
    plan.offset = components.offset;

    // Build required joins with pruning
    let alias_to_table_refs: std::collections::HashMap<String, &crate::flows::SemanticTable> =
        super::resolve::build_alias_map(flow, registry)?;
    let required_joins = select_required_joins(flow, &required_aliases, &alias_to_table_refs)?;
    for join in required_joins {
        plan.joins.push(build_join(join, &components.alias_to_table)?);
    }

    // Add measure selects
    let measure_selects = build_measure_selects(
        &components.measures,
        &components.base_measure_exprs,
        true, // only requested
    )?;
    plan.select.extend(measure_selects);

    validate_non_empty_select(&plan.select)?;

    Ok(QueryPlan::Flat(plan))
}

// ============================================================================
// Multi-Grain Plan Building (unified pre-aggregation for 1+ tables)
// ============================================================================

/// Build a multi-grain query plan.
///
/// This creates one CTE per table with measures, each aggregated to a common grain,
/// then joins them together in the final query.
fn build_multi_grain_plan(
    components: &QueryComponents,
    analysis: &MultiGrainAnalysis,
    flow: &SemanticFlow,
    registry: &FlowRegistry,
) -> Result<QueryPlan> {
    let base_alias = &components.base_alias;

    // Group measures by their table alias
    let mut measures_by_alias: std::collections::HashMap<String, Vec<_>> = std::collections::HashMap::new();
    for m in &components.measures {
        measures_by_alias
            .entry(m.alias.clone())
            .or_default()
            .push(m);
    }

    // Group dimensions by their table alias
    let mut dimensions_by_alias: std::collections::HashMap<String, Vec<_>> = std::collections::HashMap::new();
    for d in &components.dimensions {
        dimensions_by_alias
            .entry(d.alias.clone())
            .or_default()
            .push(d);
    }

    // Build a CTE for each table with measures
    let mut ctes = Vec::new();
    let mut cte_aliases = Vec::new();

    for (alias, grain) in &analysis.table_grains {
        let table = components.alias_to_table.get(alias).ok_or_else(|| {
            SemaflowError::Validation(format!("missing semantic table for alias {}", alias))
        })?;

        let from = TableRef {
            name: table.table.clone(),
            alias: Some(alias.clone()),
            subquery: None,
        };

        let mut cte = GrainedAggPlan::new(format!("{}_agg", alias), from);

        // Track columns already added to avoid duplicates
        let mut added_columns: HashSet<String> = HashSet::new();

        // Add grain columns to SELECT and GROUP BY
        for col_name in &grain.grain_columns {
            let col_expr = SqlExpr::Column {
                table: Some(alias.clone()),
                name: col_name.clone(),
            };
            cte.select.push(SelectItem {
                expr: col_expr.clone(),
                alias: Some(col_name.clone()),
            });
            cte.group_by.push(col_expr);
            added_columns.insert(col_name.clone());
        }

        // Add dimensions for this table to the CTE
        if let Some(table_dims) = dimensions_by_alias.get(alias) {
            for dim in table_dims {
                let col_name = extract_column_name(&dim.expr);
                if !added_columns.contains(&col_name) {
                    cte.select.push(SelectItem {
                        expr: dim.expr.clone(),
                        alias: Some(col_name.clone()),
                    });
                    cte.group_by.push(dim.expr.clone());
                    added_columns.insert(col_name);
                }
            }
        }

        // Add measures for this table
        if let Some(table_measures) = measures_by_alias.get(alias) {
            for m in table_measures {
                if m.measure.post_expr.is_none() {
                    if let Some(base_expr) = &m.base_expr {
                        // Extract unqualified measure name for CTE column
                        let measure_col_name = extract_unqualified_name(&m.name);
                        cte.select.push(SelectItem {
                            expr: base_expr.clone(),
                            alias: Some(measure_col_name),
                        });
                    }
                }
            }
        }

        // Add filters for this table
        for f in &components.filters {
            if f.alias.as_deref() == Some(alias) {
                cte.filters.push(render_filter_expr(f.expr.clone(), &f.filter));
            } else if alias == base_alias && f.alias.is_none() {
                // Base table gets unqualified filters
                cte.filters.push(render_filter_expr(f.expr.clone(), &f.filter));
            }
        }

        cte_aliases.push(cte.alias.clone());
        ctes.push(cte);
    }

    // Build final query
    let base_cte_alias = format!("{}_agg", base_alias);
    let mut final_query = FinalQueryPlan::new(base_cte_alias.clone());

    // Build CTE joins (uses join type from flow definition)
    for spec in &analysis.cte_join_specs {
        let from_cte_alias = format!("{}_agg", spec.from_alias);
        let to_cte_alias = format!("{}_agg", spec.to_alias);

        // Only add join if both CTEs exist (i.e., both tables have measures)
        if cte_aliases.contains(&from_cte_alias) && cte_aliases.contains(&to_cte_alias) {
            final_query.cte_joins.push(CteJoin {
                cte_alias: from_cte_alias,
                to_cte_alias,
                join_type: spec.join_type.clone(),
                on: spec.join_keys.clone(),
            });
        }
    }

    // Add dimension selects to final query
    // Base dimensions come from base CTE, joined dimensions need dimension table joins
    let mut dimension_join_aliases: HashSet<String> = HashSet::new();

    for dim in &components.dimensions {
        if analysis.table_grains.contains_key(&dim.alias) {
            // Dimension is on a table with measures - reference from its CTE
            let cte_alias = format!("{}_agg", dim.alias);
            let col_name = extract_column_name(&dim.expr);
            final_query.select.push(SelectItem {
                expr: SqlExpr::Column {
                    table: Some(cte_alias),
                    name: col_name,
                },
                alias: Some(dim.name.clone()),
            });
        } else {
            // Dimension is on a dimension-only table - need to join to it
            dimension_join_aliases.insert(dim.alias.clone());
            final_query.select.push(SelectItem {
                expr: dim.expr.clone(),
                alias: Some(dim.name.clone()),
            });
        }
    }

    // Add dimension table joins (tables without measures)
    if !dimension_join_aliases.is_empty() {
        let alias_to_table_refs = super::resolve::build_alias_map(flow, registry)?;
        let required_joins = select_required_joins(flow, &dimension_join_aliases, &alias_to_table_refs)?;
        for join in required_joins {
            // Remap join to reference CTE instead of base table
            let remapped_join = remap_join_to_cte(join, &base_cte_alias, base_alias, components)?;
            final_query.dimension_joins.push(remapped_join);
        }
    }

    // Add filters for dimension-only tables to the final query
    for f in &components.filters {
        let filter_alias = f.alias.as_deref();
        // Check if this filter is NOT on a table with measures (i.e., not in a CTE)
        if let Some(alias) = filter_alias {
            if !analysis.table_grains.contains_key(alias) {
                // Filter on dimension-only table - add to final query
                final_query.filters.push(render_filter_expr(f.expr.clone(), &f.filter));
            }
        }
    }

    // Add measure selects to final query
    for m in &components.measures {
        if m.requested {
            let cte_alias = format!("{}_agg", m.alias);
            if m.measure.post_expr.is_some() {
                // Post-expr measures need all measures from the same table for resolution
                let table_measures: Vec<_> = components
                    .measures
                    .iter()
                    .filter(|other| other.alias == m.alias)
                    .cloned()
                    .collect();
                let measure_selects = build_preagg_measure_selects(
                    &table_measures,
                    &cte_alias,
                    &components.base_measure_exprs,
                )?;
                // Only add the requested measure's select item
                for sel in measure_selects {
                    if sel.alias.as_ref() == Some(&m.name) {
                        final_query.select.push(sel);
                        break;
                    }
                }
            } else {
                // Use unqualified column name to reference CTE column
                let col_name = extract_unqualified_name(&m.name);
                final_query.select.push(SelectItem {
                    expr: SqlExpr::Column {
                        table: Some(cte_alias),
                        name: col_name,
                    },
                    alias: Some(m.name.clone()),
                });
            }
        }
    }

    // Add order by, limit, offset
    final_query.order_by = build_preagg_order_items(components);
    final_query.limit = components.limit;
    final_query.offset = components.offset;

    validate_non_empty_select(&final_query.select)?;

    Ok(QueryPlan::MultiGrain(MultiGrainPlan { ctes, final_query }))
}

/// Remap a join to reference a CTE instead of the base table.
fn remap_join_to_cte(
    join: &crate::flows::FlowJoin,
    cte_alias: &str,
    base_alias: &str,
    components: &QueryComponents,
) -> Result<crate::sql_ast::Join> {
    let join_table = components.alias_to_table.get(&join.alias).ok_or_else(|| {
        SemaflowError::Validation(format!("missing semantic table for alias {}", join.alias))
    })?;

    // Build ON clause - remap base table references to CTE
    let on_clause: Vec<SqlExpr> = join
        .join_keys
        .iter()
        .map(|k| {
            let left_table = if &join.to_table == base_alias {
                cte_alias.to_string()
            } else {
                join.to_table.clone()
            };
            SqlExpr::BinaryOp {
                op: crate::sql_ast::SqlBinaryOperator::Eq,
                left: Box::new(SqlExpr::Column {
                    table: Some(left_table),
                    name: k.left.clone(),
                }),
                right: Box::new(SqlExpr::Column {
                    table: Some(join.alias.clone()),
                    name: k.right.clone(),
                }),
            }
        })
        .collect();

    Ok(crate::sql_ast::Join {
        join_type: join.join_type.clone().into(),
        table: TableRef {
            name: join_table.table.clone(),
            alias: Some(join.alias.clone()),
            subquery: None,
        },
        on: on_clause,
    })
}

/// Extract the column name from a SQL expression.
/// For Column expressions, returns the name. For others, returns a fallback.
fn extract_column_name(expr: &SqlExpr) -> String {
    match expr {
        SqlExpr::Column { name, .. } => name.clone(),
        _ => "expr".to_string(),
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
