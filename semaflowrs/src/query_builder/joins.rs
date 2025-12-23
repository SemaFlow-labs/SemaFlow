use std::collections::{HashMap, HashSet};

use crate::error::{Result, SemaflowError};
use crate::flows::{FlowJoin, JoinType, SemanticFlow, SemanticTable};

pub(crate) fn select_required_joins<'a>(
    flow: &'a SemanticFlow,
    required_aliases: &HashSet<String>,
    alias_to_table: &HashMap<String, &'a SemanticTable>,
) -> Result<Vec<&'a FlowJoin>> {
    let base_alias = &flow.base_table.alias;
    let mut join_by_alias: HashMap<&str, &FlowJoin> = HashMap::new();
    for join in flow.joins.values() {
        join_by_alias.insert(join.alias.as_str(), join);
    }

    let mut needed: HashSet<String> = HashSet::new();
    let mut stack: Vec<String> = required_aliases
        .iter()
        .filter(|a| *a != base_alias)
        .cloned()
        .collect();
    // Always include joins that are not safe to prune (e.g., inner or unknown cardinality).
    for join in flow.joins.values() {
        if !safe_to_prune(join, alias_to_table) && join.alias != *base_alias {
            stack.push(join.alias.clone());
        }
    }
    while let Some(alias) = stack.pop() {
        if !needed.insert(alias.clone()) {
            continue;
        }
        let join = join_by_alias.get(alias.as_str()).ok_or_else(|| {
            SemaflowError::Validation(format!("missing join definition for alias {}", alias))
        })?;
        if join.to_table != *base_alias {
            stack.push(join.to_table.clone());
        }
    }

    let mut ordered = Vec::new();
    let mut visited: HashSet<String> = HashSet::new();
    for join in flow.joins.values() {
        if needed.contains(&join.alias) {
            visit_join(
                &join.alias,
                base_alias,
                &join_by_alias,
                &mut visited,
                &mut ordered,
            )?;
        }
    }
    Ok(ordered)
}

fn safe_to_prune(join: &FlowJoin, alias_to_table: &HashMap<String, &SemanticTable>) -> bool {
    if join.join_type != JoinType::Left {
        return false;
    }
    if let Some(table) = alias_to_table.get(&join.alias) {
        // Safe to prune if join keys exactly match the primary keys
        let join_right_keys: std::collections::HashSet<_> =
            join.join_keys.iter().map(|k| &k.right).collect();
        let pk_set: std::collections::HashSet<_> = table.primary_keys.iter().collect();
        if !pk_set.is_empty() && join_right_keys == pk_set {
            return true;
        }
    }
    false
}

fn visit_join<'a>(
    alias: &str,
    base_alias: &str,
    join_by_alias: &HashMap<&'a str, &'a FlowJoin>,
    visited: &mut HashSet<String>,
    ordered: &mut Vec<&'a FlowJoin>,
) -> Result<()> {
    if visited.contains(alias) {
        return Ok(());
    }
    let join = *join_by_alias.get(alias).ok_or_else(|| {
        SemaflowError::Validation(format!("missing join definition for alias {}", alias))
    })?;
    if join.to_table != base_alias {
        visit_join(&join.to_table, base_alias, join_by_alias, visited, ordered)?;
    }
    visited.insert(alias.to_string());
    ordered.push(join);
    Ok(())
}
