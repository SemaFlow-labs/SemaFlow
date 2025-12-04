use std::collections::{HashMap, HashSet};

use crate::error::{Result, SemaflowError};
use crate::flows::{Filter, QueryRequest, SemanticFlow, SemanticTable};
use crate::registry::FlowRegistry;
use crate::sql_ast::{OrderItem, SelectItem, SelectQuery, SqlExpr, TableRef};

use super::filters::render_filter_expr;
use super::joins::{build_join, select_required_joins};
use super::measures::{
    apply_measure_filter, collect_measure_refs, resolve_measure_with_posts,
    validate_no_measure_refs,
};
use super::render::expr_to_sql;
use super::resolve::{
    build_alias_map, resolve_dimension, resolve_field_expression, resolve_measure, FieldKind,
};

#[derive(Clone)]
pub(crate) struct ResolvedDimension {
    pub name: String,
    pub alias: String,
    pub expr: SqlExpr,
}

#[derive(Clone)]
pub(crate) struct ResolvedFilter {
    pub filter: Filter,
    pub expr: SqlExpr,
    pub alias: Option<String>,
}

pub(crate) fn build_query(
    flow: &SemanticFlow,
    registry: &FlowRegistry,
    request: &QueryRequest,
    supports_filtered_aggregates: bool,
) -> Result<SelectQuery> {
    let alias_to_table = build_alias_map(flow, registry)?;
    let base_alias = flow.base_table.alias.clone();
    let base_table = alias_to_table.get(&base_alias).ok_or_else(|| {
        SemaflowError::Validation(format!(
            "missing base table alias {}",
            flow.base_table.alias
        ))
    })?;

    let resolved_dimensions = resolve_dimensions(request, flow, registry, &alias_to_table)?;
    let (measure_defs, base_measure_exprs) = resolve_measures(
        request,
        flow,
        registry,
        &alias_to_table,
        supports_filtered_aggregates,
    )?;
    let resolved_filters = resolve_filters(request, flow, registry, &alias_to_table)?;

    let join_lookup: HashMap<&str, &crate::flows::FlowJoin> =
        flow.joins.values().map(|j| (j.alias.as_str(), j)).collect();

    let has_joins = !flow.joins.is_empty();
    let measures_on_base = measure_defs
        .iter()
        .all(|(_, alias, _, _)| *alias == base_alias);
    let filters_on_join = resolved_filters
        .iter()
        .any(|f| f.alias.as_deref() != Some(base_alias.as_str()));
    let join_compatible = resolved_dimensions
        .iter()
        .filter(|d| d.alias != base_alias)
        .all(|d| {
            join_lookup
                .get(d.alias.as_str())
                .map(|j| j.to_table == base_alias)
                .unwrap_or(false)
        })
        && resolved_filters
            .iter()
            .filter(|f| f.alias.as_deref() != Some(base_alias.as_str()))
            .all(|f| {
                f.alias
                    .as_ref()
                    .and_then(|a| join_lookup.get(a.as_str()))
                    .map(|j| j.to_table == base_alias)
                    .unwrap_or(false)
            });

    let use_preagg = has_joins && measures_on_base && filters_on_join && join_compatible;

    let query = if use_preagg {
        build_preagg_query(
            flow,
            base_table,
            &base_alias,
            &resolved_dimensions,
            &measure_defs,
            &base_measure_exprs,
            &resolved_filters,
            request,
            &alias_to_table,
        )?
    } else {
        build_flat_query(
            flow,
            base_table,
            &base_alias,
            &resolved_dimensions,
            &measure_defs,
            &base_measure_exprs,
            &resolved_filters,
            request,
            registry,
            &alias_to_table,
        )?
    };

    Ok(query)
}

fn resolve_dimensions(
    request: &QueryRequest,
    flow: &SemanticFlow,
    registry: &FlowRegistry,
    alias_to_table: &HashMap<String, &SemanticTable>,
) -> Result<Vec<ResolvedDimension>> {
    let mut resolved_dimensions = Vec::new();
    for dim_name in &request.dimensions {
        let (_table, alias, dimension) =
            resolve_dimension(dim_name, flow, registry, alias_to_table)?;
        resolved_dimensions.push(ResolvedDimension {
            name: dim_name.clone(),
            alias: alias.clone(),
            expr: expr_to_sql(&dimension.expression, &alias),
        });
    }
    Ok(resolved_dimensions)
}

fn resolve_measures<'a>(
    request: &'a QueryRequest,
    flow: &'a SemanticFlow,
    registry: &'a FlowRegistry,
    alias_to_table: &'a HashMap<String, &'a SemanticTable>,
    supports_filtered_aggregates: bool,
) -> Result<(
    Vec<(String, String, &'a crate::flows::Measure, bool)>,
    HashMap<String, SqlExpr>,
)> {
    let mut measure_defs: Vec<(String, String, &crate::flows::Measure, bool)> = Vec::new();
    for measure_name in &request.measures {
        let (_table, alias, measure) =
            resolve_measure(measure_name, flow, registry, alias_to_table)?;
        measure_defs.push((measure_name.clone(), alias, measure, true));
    }

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
        if let Ok((_table, alias, measure)) = resolve_measure(&dep, flow, registry, alias_to_table)
        {
            measure_defs.push((dep.clone(), alias.clone(), measure, false));
            seen_extra.insert(dep);
        }
    }

    let mut base_measure_exprs: HashMap<String, SqlExpr> = HashMap::new();
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

    Ok((measure_defs, base_measure_exprs))
}

fn resolve_filters(
    request: &QueryRequest,
    flow: &SemanticFlow,
    registry: &FlowRegistry,
    alias_to_table: &HashMap<String, &SemanticTable>,
) -> Result<Vec<ResolvedFilter>> {
    let mut resolved_filters = Vec::new();
    for filter in &request.filters {
        let (expr, kind, alias) =
            resolve_field_expression(&filter.field, flow, registry, alias_to_table)?;
        if matches!(kind, FieldKind::Measure) {
            return Err(SemaflowError::Validation(
                "filters on measures are not supported (row-level filters only)".to_string(),
            ));
        }
        resolved_filters.push(ResolvedFilter {
            filter: filter.clone(),
            expr,
            alias,
        });
    }
    Ok(resolved_filters)
}

fn build_flat_query(
    flow: &SemanticFlow,
    base_table: &SemanticTable,
    base_alias: &str,
    dimensions: &[ResolvedDimension],
    measure_defs: &[(String, String, &crate::flows::Measure, bool)],
    base_measure_exprs: &HashMap<String, SqlExpr>,
    filters: &[ResolvedFilter],
    request: &QueryRequest,
    registry: &FlowRegistry,
    alias_to_table: &HashMap<String, &SemanticTable>,
) -> Result<SelectQuery> {
    let mut required_aliases: HashSet<String> = HashSet::new();
    required_aliases.insert(base_alias.to_string());

    let mut query = SelectQuery::default();
    query.from = TableRef {
        name: base_table.table.clone(),
        alias: Some(base_alias.to_string()),
        subquery: None,
    };

    for dim in dimensions {
        required_aliases.insert(dim.alias.clone());
        query.group_by.push(dim.expr.clone());
        query.select.push(SelectItem {
            expr: dim.expr.clone(),
            alias: Some(dim.name.clone()),
        });
    }

    for f in filters {
        if let Some(alias) = &f.alias {
            required_aliases.insert(alias.clone());
        }
        query
            .filters
            .push(render_filter_expr(f.expr.clone(), &f.filter));
    }

    if !request.order.is_empty() {
        for item in &request.order {
            let (expr, _, alias) =
                resolve_field_expression(&item.column, flow, registry, alias_to_table)?;
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

    let required_joins = select_required_joins(flow, &required_aliases, alias_to_table)?;
    for join in required_joins {
        query.joins.push(build_join(join, alias_to_table)?);
    }

    let mut measure_lookup: HashMap<String, (&str, &crate::flows::Measure)> = HashMap::new();
    for (name, alias, measure, _) in measure_defs {
        measure_lookup.insert(name.clone(), (alias.as_str(), *measure));
        let qualified = format!("{}.{}", alias, name);
        measure_lookup
            .entry(qualified)
            .or_insert((alias.as_str(), *measure));
    }
    let mut resolved_cache: HashMap<String, SqlExpr> = HashMap::new();
    let mut stack: Vec<String> = Vec::new();
    for (name, _, _measure, requested) in measure_defs {
        let expr = resolve_measure_with_posts(
            name,
            &measure_lookup,
            base_measure_exprs,
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

    Ok(query)
}

fn build_preagg_query(
    flow: &SemanticFlow,
    base_table: &SemanticTable,
    base_alias: &str,
    dimensions: &[ResolvedDimension],
    measure_defs: &[(String, String, &crate::flows::Measure, bool)],
    base_measure_exprs: &HashMap<String, SqlExpr>,
    filters: &[ResolvedFilter],
    request: &QueryRequest,
    alias_to_table: &HashMap<String, &SemanticTable>,
) -> Result<SelectQuery> {
    let preagg_alias = "fact_preagg".to_string();
    let mut needed_join_aliases: HashSet<String> = HashSet::new();
    for dim in dimensions {
        if dim.alias != base_alias {
            needed_join_aliases.insert(dim.alias.clone());
        }
    }
    for f in filters {
        if let Some(alias) = &f.alias {
            if alias != base_alias {
                needed_join_aliases.insert(alias.clone());
            }
        }
    }

    let mut join_by_alias: HashMap<&str, &crate::flows::FlowJoin> = HashMap::new();
    for join in flow.joins.values() {
        join_by_alias.insert(join.alias.as_str(), join);
    }

    let mut join_key_aliases: HashMap<String, Vec<(String, String, String)>> = HashMap::new();
    for alias in &needed_join_aliases {
        let join = join_by_alias.get(alias.as_str()).ok_or_else(|| {
            SemaflowError::Validation(format!("missing join definition for alias {alias}"))
        })?;
        if join.to_table != base_alias {
            return Err(SemaflowError::Validation(format!(
                "pre-aggregation currently supports only single-hop joins to the base; {} joins to {}",
                alias, join.to_table
            )));
        }
        let entries = join_key_aliases
            .entry(alias.clone())
            .or_insert_with(Vec::new);
        for key in &join.join_keys {
            let col_alias = format!("{}__{}", alias, key.left);
            entries.push((col_alias, key.left.clone(), key.right.clone()));
        }
    }

    let mut preagg = SelectQuery::default();
    preagg.from = TableRef {
        name: base_table.table.clone(),
        alias: Some(base_alias.to_string()),
        subquery: None,
    };

    for dim in dimensions {
        if dim.alias == base_alias {
            preagg.group_by.push(dim.expr.clone());
            preagg.select.push(SelectItem {
                expr: dim.expr.clone(),
                alias: Some(dim.name.clone()),
            });
        }
    }

    for (_alias, keys) in &join_key_aliases {
        for (col_alias, base_col, _right_col) in keys {
            let col_expr = SqlExpr::Column {
                table: Some(base_alias.to_string()),
                name: base_col.clone(),
            };
            preagg.group_by.push(col_expr.clone());
            preagg.select.push(SelectItem {
                expr: col_expr,
                alias: Some(col_alias.clone()),
            });
        }
    }

    for f in filters {
        match f.alias.as_deref() {
            Some(alias) if alias != base_alias => {
                let join = join_by_alias.get(alias).ok_or_else(|| {
                    SemaflowError::Validation(format!(
                        "missing join definition for alias {}",
                        alias
                    ))
                })?;
                let join_table = alias_to_table.get(alias).ok_or_else(|| {
                    SemaflowError::Validation(format!("missing semantic table for alias {}", alias))
                })?;
                let mut sub = SelectQuery::default();
                sub.from = TableRef {
                    name: join_table.table.clone(),
                    alias: Some(alias.to_string()),
                    subquery: None,
                };
                for key in &join.join_keys {
                    sub.filters.push(SqlExpr::BinaryOp {
                        op: crate::sql_ast::SqlBinaryOperator::Eq,
                        left: Box::new(SqlExpr::Column {
                            table: Some(base_alias.to_string()),
                            name: key.left.clone(),
                        }),
                        right: Box::new(SqlExpr::Column {
                            table: Some(alias.to_string()),
                            name: key.right.clone(),
                        }),
                    });
                }
                sub.filters
                    .push(render_filter_expr(f.expr.clone(), &f.filter));
                sub.select.push(SelectItem {
                    expr: SqlExpr::Literal(serde_json::Value::Bool(true)),
                    alias: None,
                });
                preagg.filters.push(SqlExpr::Exists {
                    subquery: Box::new(sub),
                });
            }
            _ => {
                preagg
                    .filters
                    .push(render_filter_expr(f.expr.clone(), &f.filter));
            }
        }
    }

    for (name, _alias, measure, _) in measure_defs {
        if measure.post_expr.is_none() {
            if let Some(expr) = base_measure_exprs.get(name) {
                preagg.select.push(SelectItem {
                    expr: expr.clone(),
                    alias: Some(name.clone()),
                });
            }
        }
    }

    let mut outer = SelectQuery::default();
    outer.from = TableRef {
        name: String::new(),
        alias: Some(preagg_alias.clone()),
        subquery: Some(Box::new(preagg)),
    };

    let mut added_joins: HashSet<String> = HashSet::new();
    for dim in dimensions {
        if dim.alias == base_alias {
            outer.select.push(SelectItem {
                expr: SqlExpr::Column {
                    table: Some(preagg_alias.clone()),
                    name: dim.name.clone(),
                },
                alias: Some(dim.name.clone()),
            });
        } else {
            if added_joins.insert(dim.alias.clone()) {
                let join = join_by_alias.get(dim.alias.as_str()).ok_or_else(|| {
                    SemaflowError::Validation(format!(
                        "missing join definition for alias {}",
                        dim.alias
                    ))
                })?;
                let join_table = alias_to_table.get(&dim.alias).ok_or_else(|| {
                    SemaflowError::Validation(format!(
                        "missing semantic table for alias {}",
                        dim.alias
                    ))
                })?;
                let keys = join_key_aliases.get(&dim.alias).ok_or_else(|| {
                    SemaflowError::Validation(format!("missing join keys for {}", dim.alias))
                })?;
                let mut on_clause = Vec::new();
                for (col_alias, _left_col, right_col) in keys {
                    on_clause.push(SqlExpr::BinaryOp {
                        op: crate::sql_ast::SqlBinaryOperator::Eq,
                        left: Box::new(SqlExpr::Column {
                            table: Some(preagg_alias.clone()),
                            name: col_alias.clone(),
                        }),
                        right: Box::new(SqlExpr::Column {
                            table: Some(dim.alias.clone()),
                            name: right_col.clone(),
                        }),
                    });
                }
                outer.joins.push(crate::sql_ast::Join {
                    join_type: match join.join_type {
                        crate::flows::JoinType::Inner => crate::sql_ast::SqlJoinType::Inner,
                        crate::flows::JoinType::Left => crate::sql_ast::SqlJoinType::Left,
                        crate::flows::JoinType::Right => crate::sql_ast::SqlJoinType::Right,
                        crate::flows::JoinType::Full => crate::sql_ast::SqlJoinType::Full,
                    },
                    table: TableRef {
                        name: join_table.table.clone(),
                        alias: Some(dim.alias.clone()),
                        subquery: None,
                    },
                    on: on_clause,
                });
            }

            outer.select.push(SelectItem {
                expr: dim.expr.clone(),
                alias: Some(dim.name.clone()),
            });
        }
    }

    let mut outer_base_exprs: HashMap<String, SqlExpr> = HashMap::new();
    for (name, _alias, measure, _) in measure_defs {
        if measure.post_expr.is_none() {
            let col = SqlExpr::Column {
                table: Some(preagg_alias.clone()),
                name: name.clone(),
            };
            outer_base_exprs.insert(name.clone(), col.clone());
            let qualified = format!("{}.{}", preagg_alias, name);
            outer_base_exprs.insert(qualified, col);
        }
    }

    let mut measure_lookup: HashMap<String, (&str, &crate::flows::Measure)> = HashMap::new();
    for (name, alias, measure, _) in measure_defs {
        measure_lookup.insert(name.clone(), (alias.as_str(), *measure));
        let qualified = format!("{}.{}", alias, name);
        measure_lookup
            .entry(qualified)
            .or_insert((alias.as_str(), *measure));
    }

    let mut resolved_cache: HashMap<String, SqlExpr> = HashMap::new();
    let mut stack: Vec<String> = Vec::new();
    for (name, _, _measure, requested) in measure_defs {
        let expr = resolve_measure_with_posts(
            name,
            &measure_lookup,
            &outer_base_exprs,
            &mut resolved_cache,
            &mut stack,
        )?;
        if *requested {
            outer.select.push(SelectItem {
                expr,
                alias: Some(name.clone()),
            });
        }
    }

    outer.limit = request.limit.map(|v| v as u64);
    outer.offset = request.offset.map(|v| v as u64);

    if !request.order.is_empty() {
        for item in &request.order {
            outer.order_by.push(OrderItem {
                expr: SqlExpr::Column {
                    table: None,
                    name: item.column.clone(),
                },
                direction: item.direction.clone(),
            });
        }
    }

    if outer.select.is_empty() {
        return Err(SemaflowError::Validation(
            "query requires at least one dimension or measure".to_string(),
        ));
    }

    Ok(outer)
}
