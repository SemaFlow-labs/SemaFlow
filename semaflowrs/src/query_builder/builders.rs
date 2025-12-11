//! Shared building helpers for query construction.
//!
//! This module provides reusable functions for building common query elements
//! like SELECT items, JOINs, and ORDER BY clauses.

use std::collections::HashMap;

use crate::error::{Result, SemaflowError};
use crate::flows::{FlowJoin, JoinType, Measure, SemanticTable};
use crate::sql_ast::{
    Join, OrderItem, SelectItem, SqlBinaryOperator, SqlExpr, SqlJoinType, TableRef,
};

use super::components::{QueryComponents, ResolvedDimension, ResolvedMeasure};
use super::measures::resolve_measure_with_posts;

/// Convert a semantic JoinType to SQL JoinType.
impl From<JoinType> for SqlJoinType {
    fn from(jt: JoinType) -> Self {
        match jt {
            JoinType::Inner => SqlJoinType::Inner,
            JoinType::Left => SqlJoinType::Left,
            JoinType::Right => SqlJoinType::Right,
            JoinType::Full => SqlJoinType::Full,
        }
    }
}

/// Build a SELECT item from a resolved dimension.
pub fn build_dimension_select(dim: &ResolvedDimension) -> SelectItem {
    SelectItem {
        expr: dim.expr.clone(),
        alias: Some(dim.name.clone()),
    }
}

/// Build measure SELECT items by resolving post expressions.
pub fn build_measure_selects(
    measures: &[ResolvedMeasure],
    base_exprs: &HashMap<String, SqlExpr>,
    only_requested: bool,
) -> Result<Vec<SelectItem>> {
    let mut measure_lookup: HashMap<String, (&str, &Measure)> = HashMap::new();
    for m in measures {
        // Insert the user-supplied name (could be qualified like "o.order_total")
        measure_lookup.insert(m.name.clone(), (m.alias.as_str(), &m.measure));

        // Extract and insert unqualified name for post_expr references
        let unqualified = extract_unqualified_name(&m.name);
        measure_lookup
            .entry(unqualified.clone())
            .or_insert((m.alias.as_str(), &m.measure));

        // Also insert fully qualified version if not already present
        let qualified = format!("{}.{}", m.alias, unqualified);
        measure_lookup
            .entry(qualified)
            .or_insert((m.alias.as_str(), &m.measure));
    }

    let mut resolved_cache: HashMap<String, SqlExpr> = HashMap::new();
    let mut stack: Vec<String> = Vec::new();
    let mut selects = Vec::new();

    for m in measures {
        let expr = resolve_measure_with_posts(
            &m.name,
            &measure_lookup,
            base_exprs,
            &mut resolved_cache,
            &mut stack,
        )?;
        if !only_requested || m.requested {
            selects.push(SelectItem {
                expr,
                alias: Some(m.name.clone()),
            });
        }
    }

    Ok(selects)
}

/// Build a JOIN clause from a FlowJoin.
pub fn build_join(
    join: &FlowJoin,
    alias_to_table: &HashMap<String, SemanticTable>,
) -> Result<Join> {
    let join_table = alias_to_table.get(&join.alias).ok_or_else(|| {
        SemaflowError::Validation(format!(
            "missing semantic table for join alias {}",
            join.alias
        ))
    })?;

    let on_clause: Vec<SqlExpr> = join
        .join_keys
        .iter()
        .map(|k| SqlExpr::BinaryOp {
            op: SqlBinaryOperator::Eq,
            left: Box::new(SqlExpr::Column {
                table: Some(join.to_table.clone()),
                name: k.left.clone(),
            }),
            right: Box::new(SqlExpr::Column {
                table: Some(join.alias.clone()),
                name: k.right.clone(),
            }),
        })
        .collect();

    Ok(Join {
        join_type: join.join_type.clone().into(),
        table: TableRef {
            name: join_table.table.clone(),
            alias: Some(join.alias.clone()),
            subquery: None,
        },
        on: on_clause,
    })
}

/// Build column references for measures from pre-aggregated results.
pub fn build_preagg_measure_selects(
    measures: &[ResolvedMeasure],
    preagg_alias: &str,
    _base_exprs: &HashMap<String, SqlExpr>,
) -> Result<Vec<SelectItem>> {
    // For pre-agg outer query, base measures become simple column references
    let mut outer_base_exprs: HashMap<String, SqlExpr> = HashMap::new();
    for m in measures {
        if m.measure.post_expr.is_none() {
            // The CTE column uses unqualified name
            let unqualified = extract_unqualified_name(&m.name);
            let col = SqlExpr::Column {
                table: Some(preagg_alias.to_string()),
                name: unqualified.clone(),
            };

            // Insert user-supplied name
            outer_base_exprs.insert(m.name.clone(), col.clone());

            // Insert unqualified name for post_expr references
            outer_base_exprs
                .entry(unqualified.clone())
                .or_insert_with(|| col.clone());

            // Insert fully qualified version
            let qualified = format!("{}.{}", m.alias, unqualified);
            outer_base_exprs.entry(qualified).or_insert(col);
        }
    }

    build_measure_selects(measures, &outer_base_exprs, true)
}

/// Validate that the query has at least one select item.
pub fn validate_non_empty_select(selects: &[SelectItem]) -> Result<()> {
    if selects.is_empty() {
        return Err(SemaflowError::Validation(
            "query requires at least one dimension or measure".to_string(),
        ));
    }
    Ok(())
}

/// Build ORDER BY items, converting column names to expressions.
pub fn build_order_items(components: &QueryComponents) -> Vec<OrderItem> {
    components.order.clone()
}

/// Build ORDER BY items for the outer query of a pre-aggregated plan.
/// Uses unqualified column names since they reference the SELECT aliases.
pub fn build_preagg_order_items(components: &QueryComponents) -> Vec<OrderItem> {
    components
        .order
        .iter()
        .map(|item| {
            // For preagg outer query, order by the alias name
            let name = extract_order_column_name(&item.expr);
            OrderItem {
                expr: SqlExpr::Column {
                    table: None,
                    name,
                },
                direction: item.direction.clone(),
            }
        })
        .collect()
}

/// Extract a column name from an order expression for aliasing.
fn extract_order_column_name(expr: &SqlExpr) -> String {
    match expr {
        SqlExpr::Column { name, .. } => name.clone(),
        _ => "unknown".to_string(),
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
