//! Query planner orchestration.
//!
//! This module coordinates the query building process using a unified flow
//! that decides between flat and pre-aggregated strategies based on fanout analysis.

use std::collections::HashSet;

use crate::error::{Result, SemaflowError};
use crate::flows::{Aggregation, QueryRequest, SemanticFlow};
use crate::registry::FlowRegistry;
use crate::sql_ast::{SelectItem, SelectQuery, SqlExpr, SqlJoinType, TableRef};

use super::analysis::{analyze_multi_grain, MultiGrainAnalysis};
use super::builders::{
    build_dimension_select, build_join, build_measure_selects, build_order_items,
    build_preagg_measure_selects, build_preagg_order_items, validate_non_empty_select,
};
use super::components::{resolve_components, MeasureStrategy, QueryComponents};
use super::filters::render_filter_expr;
use super::joins::select_required_joins;
use super::plan::{CteJoin, FinalQueryPlan, FlatPlan, GrainedAggPlan, MultiGrainPlan, QueryPlan};
use super::render::expr_to_sql;

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
    Ok(plan.into_select_query())
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
        plan.filters
            .push(render_filter_expr(f.expr.clone(), &f.filter));
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
        plan.joins
            .push(build_join(join, &components.alias_to_table)?);
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

    // Build a lookup from alias -> join type for filter placement decisions
    // Base table has no join type; joined tables have INNER or LEFT
    let join_type_lookup: std::collections::HashMap<String, SqlJoinType> = analysis
        .cte_join_specs
        .iter()
        .map(|spec| (spec.from_alias.clone(), spec.join_type))
        .collect();

    // Group measures by their table alias
    let mut measures_by_alias: std::collections::HashMap<String, Vec<_>> =
        std::collections::HashMap::new();
    for m in &components.measures {
        measures_by_alias
            .entry(m.alias.clone())
            .or_default()
            .push(m);
    }

    // Group dimensions by their table alias
    let mut dimensions_by_alias: std::collections::HashMap<String, Vec<_>> =
        std::collections::HashMap::new();
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

        // Add measures for this table based on their re-aggregation strategy
        if let Some(table_measures) = measures_by_alias.get(alias) {
            for m in table_measures {
                if m.measure.post_expr.is_some() {
                    continue; // Post-expr measures handled differently
                }

                let measure_col_name = extract_unqualified_name(&m.name);

                match &m.strategy {
                    MeasureStrategy::NonDecomposable => {
                        // Error: can't use non-decomposable aggregations in multi-grain
                        return Err(SemaflowError::Validation(format!(
                            "Measure '{}' uses {:?} aggregation which cannot be re-aggregated \
                             in a multi-grain query. Consider using a flat query or a \
                             different aggregation type.",
                            m.name, m.measure.agg
                        )));
                    }
                    MeasureStrategy::DistinctSafe => {
                        // Skip CTE - will be calculated directly in final query
                        // DISTINCT naturally handles fanout duplication
                        continue;
                    }
                    MeasureStrategy::WeightedAverage => {
                        // AVG needs SUM and COUNT tracked separately
                        // Final query will compute SUM(sum) / SUM(count)
                        let expr = m.measure.expr.as_ref().expect("AVG measure must have expr");
                        let inner_expr = expr_to_sql(expr, alias);

                        // Emit SUM(expr) AS measure__sum
                        cte.select.push(SelectItem {
                            expr: SqlExpr::Aggregate {
                                agg: Aggregation::Sum,
                                expr: Box::new(inner_expr.clone()),
                            },
                            alias: Some(format!("{}__sum", measure_col_name)),
                        });

                        // Emit COUNT(expr) AS measure__count
                        cte.select.push(SelectItem {
                            expr: SqlExpr::Aggregate {
                                agg: Aggregation::Count,
                                expr: Box::new(inner_expr),
                            },
                            alias: Some(format!("{}__count", measure_col_name)),
                        });
                    }
                    MeasureStrategy::PreAggregatable | MeasureStrategy::Associative => {
                        // Use the existing base_expr (already has correct aggregation)
                        if let Some(base_expr) = &m.base_expr {
                            cte.select.push(SelectItem {
                                expr: base_expr.clone(),
                                alias: Some(measure_col_name),
                            });
                        }
                    }
                }
            }
        }

        // Add filters for this table - but only if it's the base table or uses INNER join.
        // LEFT join filters must go to outer query to preserve correct semantics.
        for f in &components.filters {
            let is_base_table = alias == base_alias;
            let is_inner_join = join_type_lookup
                .get(alias)
                .is_some_and(|jt| matches!(jt, SqlJoinType::Inner));

            // Only add filter to CTE if base table or INNER join (early filter = optimization)
            if is_base_table || is_inner_join {
                if f.alias.as_deref() == Some(alias) {
                    cte.filters
                        .push(render_filter_expr(f.expr.clone(), &f.filter));
                } else if is_base_table && f.alias.is_none() {
                    // Base table gets unqualified filters
                    cte.filters
                        .push(render_filter_expr(f.expr.clone(), &f.filter));
                }
            }
            // LEFT join filters are handled later in the outer query
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
                join_type: spec.join_type,
                on: spec.join_keys.clone(),
            });
        }
    }

    // Add dimension selects to final query AND GROUP BY
    // Base dimensions come from base CTE, joined dimensions need dimension table joins
    let mut dimension_join_aliases: HashSet<String> = HashSet::new();

    for dim in &components.dimensions {
        if analysis.table_grains.contains_key(&dim.alias) {
            // Dimension is on a table with measures - reference from its CTE
            let cte_alias = format!("{}_agg", dim.alias);
            let col_name = extract_column_name(&dim.expr);
            let dim_expr = SqlExpr::Column {
                table: Some(cte_alias),
                name: col_name,
            };
            final_query.select.push(SelectItem {
                expr: dim_expr.clone(),
                alias: Some(dim.name.clone()),
            });
            // Add to GROUP BY for re-aggregation
            final_query.group_by.push(dim_expr);
        } else {
            // Dimension is on a dimension-only table - need to join to it
            dimension_join_aliases.insert(dim.alias.clone());
            final_query.select.push(SelectItem {
                expr: dim.expr.clone(),
                alias: Some(dim.name.clone()),
            });
            // Add to GROUP BY for re-aggregation
            final_query.group_by.push(dim.expr.clone());
        }
    }

    // Add dimension table joins (tables without measures)
    if !dimension_join_aliases.is_empty() {
        let alias_to_table_refs = super::resolve::build_alias_map(flow, registry)?;
        let required_joins =
            select_required_joins(flow, &dimension_join_aliases, &alias_to_table_refs)?;
        for join in required_joins {
            // Remap join to reference CTE instead of base table
            let remapped_join = remap_join_to_cte(join, &base_cte_alias, base_alias, components)?;
            final_query.dimension_joins.push(remapped_join);
        }
    }

    // Add filters to the final query:
    // 1. LEFT join table filters (must filter after join for correct semantics)
    // 2. Dimension-only table filters (tables without measures, not in CTEs)
    for f in &components.filters {
        let filter_alias = f.alias.as_deref();

        if let Some(alias) = filter_alias {
            let is_left_join = join_type_lookup
                .get(alias)
                .is_some_and(|jt| matches!(jt, SqlJoinType::Left));
            let is_in_cte = analysis.table_grains.contains_key(alias);

            if is_left_join && is_in_cte {
                // LEFT join table with measures - remap to CTE alias
                let remapped_expr = remap_expr_to_cte(&f.expr, alias);
                final_query
                    .filters
                    .push(render_filter_expr(remapped_expr, &f.filter));
            } else if !is_in_cte {
                // Dimension-only table - use original expression
                final_query
                    .filters
                    .push(render_filter_expr(f.expr.clone(), &f.filter));
            }
            // Base table and INNER join filters already handled in CTEs
        }
    }

    // Add measure selects to final query with proper re-aggregation
    for m in &components.measures {
        if !m.requested {
            continue;
        }

        let cte_alias = format!("{}_agg", m.alias);
        let col_name = extract_unqualified_name(&m.name);

        // Handle post_expr measures separately (they have their own logic)
        if m.measure.post_expr.is_some() {
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
            for sel in measure_selects {
                if sel.alias.as_ref() == Some(&m.name) {
                    final_query.select.push(sel);
                    break;
                }
            }
            continue;
        }

        // Build re-aggregation expression based on strategy
        let select_expr = match &m.strategy {
            MeasureStrategy::PreAggregatable => {
                // SUM/COUNT → re-aggregate with SUM
                SqlExpr::Aggregate {
                    agg: Aggregation::Sum,
                    expr: Box::new(SqlExpr::Column {
                        table: Some(cte_alias),
                        name: col_name.clone(),
                    }),
                }
            }
            MeasureStrategy::Associative => {
                // MIN/MAX → re-aggregate with same function
                let agg = m
                    .measure
                    .agg
                    .as_ref()
                    .expect("Associative measure must have agg");
                SqlExpr::Aggregate {
                    agg: agg.clone(),
                    expr: Box::new(SqlExpr::Column {
                        table: Some(cte_alias),
                        name: col_name.clone(),
                    }),
                }
            }
            MeasureStrategy::WeightedAverage => {
                // AVG → SUM(sum) / SUM(count)
                let sum_col = SqlExpr::Column {
                    table: Some(cte_alias.clone()),
                    name: format!("{}__sum", col_name),
                };
                let count_col = SqlExpr::Column {
                    table: Some(cte_alias),
                    name: format!("{}__count", col_name),
                };
                SqlExpr::BinaryOp {
                    op: crate::sql_ast::SqlBinaryOperator::Divide,
                    left: Box::new(SqlExpr::Aggregate {
                        agg: Aggregation::Sum,
                        expr: Box::new(sum_col),
                    }),
                    right: Box::new(SqlExpr::Aggregate {
                        agg: Aggregation::Sum,
                        expr: Box::new(count_col),
                    }),
                }
            }
            MeasureStrategy::DistinctSafe => {
                // COUNT DISTINCT - calculate directly on original table
                // For now, use the base_expr which has the full aggregation
                // TODO: This needs joining to the original table
                if let Some(base_expr) = &m.base_expr {
                    base_expr.clone()
                } else {
                    continue;
                }
            }
            MeasureStrategy::NonDecomposable => {
                // Should have errored earlier, but just in case
                return Err(SemaflowError::Validation(format!(
                    "Measure '{}' cannot be re-aggregated",
                    m.name
                )));
            }
        };

        final_query.select.push(SelectItem {
            expr: select_expr,
            alias: Some(m.name.clone()),
        });
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
            let left_table = if join.to_table == base_alias {
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

/// Remap a SQL expression to reference a CTE alias instead of the original table alias.
/// For example: `customers.country` -> `customers_agg.country`
fn remap_expr_to_cte(expr: &SqlExpr, original_alias: &str) -> SqlExpr {
    match expr {
        SqlExpr::Column { table, name } => {
            let new_table = table.as_ref().map(|t| {
                if t == original_alias {
                    format!("{}_agg", t)
                } else {
                    t.clone()
                }
            });
            SqlExpr::Column {
                table: new_table,
                name: name.clone(),
            }
        }
        // For other expression types, return as-is (filters are typically simple column refs)
        other => other.clone(),
    }
}
