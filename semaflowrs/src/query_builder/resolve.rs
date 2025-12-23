use std::collections::HashMap;

use crate::error::{Result, SemaflowError};
use crate::flows::{SemanticFlow, SemanticTable};
use crate::registry::FlowRegistry;
use crate::sql_ast::SqlExpr;

use super::render::expr_to_sql;

#[derive(Debug, Clone, Copy)]
pub(crate) enum FieldKind {
    Dimension,
    Measure,
}

pub(crate) fn build_alias_map<'a>(
    flow: &'a SemanticFlow,
    registry: &'a FlowRegistry,
) -> Result<HashMap<String, &'a SemanticTable>> {
    let mut map = HashMap::new();
    let base = registry
        .get_table(&flow.base_table.semantic_table)
        .ok_or_else(|| {
            SemaflowError::Validation(format!(
                "unknown semantic table {}",
                flow.base_table.semantic_table
            ))
        })?;
    map.insert(flow.base_table.alias.clone(), base);

    for join in flow.joins.values() {
        let table = registry.get_table(&join.semantic_table).ok_or_else(|| {
            SemaflowError::Validation(format!("unknown semantic table {}", join.semantic_table))
        })?;
        map.insert(join.alias.clone(), table);
    }
    Ok(map)
}

pub(crate) fn resolve_dimension<'a>(
    name: &str,
    flow: &'a SemanticFlow,
    registry: &'a FlowRegistry,
    alias_map: &HashMap<String, &'a SemanticTable>,
) -> Result<(&'a SemanticTable, String, &'a crate::flows::Dimension)> {
    match resolve_dimension_inner(name, flow, registry, alias_map)? {
        Some(found) => Ok(found),
        None => Err(SemaflowError::Validation(format!(
            "unknown dimension {name}"
        ))),
    }
}

pub(crate) fn resolve_measure<'a>(
    name: &str,
    flow: &'a SemanticFlow,
    registry: &'a FlowRegistry,
    alias_map: &HashMap<String, &'a SemanticTable>,
) -> Result<(&'a SemanticTable, String, &'a crate::flows::Measure)> {
    match resolve_measure_inner(name, flow, registry, alias_map)? {
        Some(found) => Ok(found),
        None => Err(SemaflowError::Validation(format!("unknown measure {name}"))),
    }
}

pub(crate) fn resolve_dimension_inner<'a>(
    name: &str,
    flow: &'a SemanticFlow,
    registry: &'a FlowRegistry,
    alias_map: &HashMap<String, &'a SemanticTable>,
) -> Result<Option<(&'a SemanticTable, String, &'a crate::flows::Dimension)>> {
    if let Some((alias, field)) = parse_qualified(name) {
        if alias == flow.base_table.alias {
            if let Some(base_table) = registry.get_table(&flow.base_table.semantic_table) {
                if let Some(dim) = base_table.dimensions.get(field) {
                    return Ok(Some((base_table, alias.to_string(), dim)));
                }
            }
        }
        if let Some(table) = alias_map.get(alias) {
            if let Some(dim) = table.dimensions.get(field) {
                return Ok(Some((table, alias.to_string(), dim)));
            }
        }
        return Ok(None);
    }

    let mut matches = Vec::new();
    if let Some(base_table) = registry.get_table(&flow.base_table.semantic_table) {
        if let Some(dim) = base_table.dimensions.get(name) {
            matches.push((base_table, flow.base_table.alias.clone(), dim));
        }
    }
    for join in flow.joins.values() {
        if let Some(table) = alias_map.get(&join.alias) {
            if let Some(dim) = table.dimensions.get(name) {
                matches.push((*table, join.alias.clone(), dim));
            }
        }
    }

    if matches.len() > 1 {
        let aliases: Vec<String> = matches.iter().map(|(_, alias, _)| alias.clone()).collect();
        return Err(SemaflowError::Validation(format!(
            "ambiguous dimension {name}; found on aliases {}",
            aliases.join(", ")
        )));
    }

    Ok(matches.into_iter().next())
}

pub(crate) fn resolve_measure_inner<'a>(
    name: &str,
    flow: &'a SemanticFlow,
    registry: &'a FlowRegistry,
    alias_map: &HashMap<String, &'a SemanticTable>,
) -> Result<Option<(&'a SemanticTable, String, &'a crate::flows::Measure)>> {
    if let Some((alias, field)) = parse_qualified(name) {
        if alias == flow.base_table.alias {
            if let Some(base_table) = registry.get_table(&flow.base_table.semantic_table) {
                if let Some(measure) = base_table.measures.get(field) {
                    return Ok(Some((base_table, alias.to_string(), measure)));
                }
            }
        }
        if let Some(table) = alias_map.get(alias) {
            if let Some(measure) = table.measures.get(field) {
                return Ok(Some((table, alias.to_string(), measure)));
            }
        }
        return Ok(None);
    }

    let mut matches = Vec::new();
    if let Some(base_table) = registry.get_table(&flow.base_table.semantic_table) {
        if let Some(measure) = base_table.measures.get(name) {
            matches.push((base_table, flow.base_table.alias.clone(), measure));
        }
    }
    for join in flow.joins.values() {
        if let Some(table) = alias_map.get(&join.alias) {
            if let Some(measure) = table.measures.get(name) {
                matches.push((*table, join.alias.clone(), measure));
            }
        }
    }

    if matches.len() > 1 {
        let aliases: Vec<String> = matches.iter().map(|(_, alias, _)| alias.clone()).collect();
        return Err(SemaflowError::Validation(format!(
            "ambiguous measure {name}; found on aliases {}",
            aliases.join(", ")
        )));
    }

    Ok(matches.into_iter().next())
}

pub(crate) fn resolve_field_expression(
    name: &str,
    flow: &SemanticFlow,
    registry: &FlowRegistry,
    alias_map: &HashMap<String, &SemanticTable>,
) -> Result<(SqlExpr, FieldKind, Option<String>)> {
    if let Some((_, alias, dim)) = resolve_dimension_inner(name, flow, registry, alias_map)? {
        let expr = expr_to_sql(&dim.expr, &alias);
        return Ok((expr, FieldKind::Dimension, Some(alias)));
    }
    if let Some((_, alias, _)) = resolve_measure_inner(name, flow, registry, alias_map)? {
        return Ok((
            SqlExpr::Column {
                table: None,
                name: name.to_string(),
            },
            FieldKind::Measure,
            Some(alias),
        ));
    }
    Err(SemaflowError::Validation(format!(
        "field {name} not found in flow {}",
        flow.name
    )))
}

pub(crate) fn parse_qualified(name: &str) -> Option<(&str, &str)> {
    let (alias, field) = name.split_once('.')?;

    if alias.is_empty() || field.is_empty() {
        return None;
    }
    Some((alias, field))
}
