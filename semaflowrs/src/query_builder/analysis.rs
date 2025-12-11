//! Query analysis for determining optimal query strategy.
//!
//! This module analyzes query components to determine whether a flat query
//! or pre-aggregated query should be used to avoid fanout issues.
//!
//! Uses cardinality inference from `grain.rs` to make smarter decisions
//! about when pre-aggregation is truly needed.

use std::collections::{HashMap, HashSet};

use crate::error::{Result, SemaflowError};
use crate::flows::{FlowJoin, JoinType, SemanticFlow};
use crate::sql_ast::SqlJoinType;

use super::components::QueryComponents;
use super::grain::{infer_join_cardinality, Cardinality, Grain};

/// Result of analyzing a query for fanout risk.
#[derive(Debug, Clone)]
pub struct FanoutAnalysis {
    /// Whether pre-aggregation is needed to avoid fanout.
    pub needs_preagg: bool,
    /// Mapping of join alias to join key info for pre-aggregation.
    /// Each entry is (aliased_column_name, base_column, right_column).
    pub join_key_mappings: HashMap<String, Vec<(String, String, String)>>,
}

impl FanoutAnalysis {
    /// Create an analysis result indicating no pre-aggregation needed.
    pub fn flat() -> Self {
        Self {
            needs_preagg: false,
            join_key_mappings: HashMap::new(),
        }
    }
}

/// Expand a set of needed aliases to include all joins in their chains back to base.
///
/// For example, if we need alias "r" and the join chain is:
///   base "o" → "c" → "r"
/// This function returns {"c", "r"} - all non-base aliases in the path.
fn expand_join_chains(
    direct_aliases: &HashSet<String>,
    base_alias: &str,
    join_lookup: &HashMap<String, FlowJoin>,
) -> HashSet<String> {
    let mut all_aliases = HashSet::new();

    for alias in direct_aliases {
        // Trace chain back to base
        let mut current = alias.clone();
        let mut visited = HashSet::new();

        while current != base_alias && !visited.contains(&current) {
            visited.insert(current.clone());
            all_aliases.insert(current.clone());

            // Find the join for this alias and move to its parent
            if let Some(join) = join_lookup.get(&current) {
                current = join.to_table.clone();
            } else {
                break; // Join not found, stop tracing
            }
        }
    }

    all_aliases
}

/// Analyze the query components to determine if pre-aggregation is needed.
///
/// Pre-aggregation is used when:
/// 1. The flow has joins
/// 2. All measures are on the base table
/// 3. There are filters on joined tables (not the base)
/// 4. The joined dimensions/filters are compatible (single-hop to base)
/// 5. At least one join could cause fanout (using cardinality inference)
///
/// This avoids the "fanout problem" where joining to a many-side table
/// would multiply base rows before aggregation.
pub fn analyze_fanout_risk(
    components: &QueryComponents,
    _flow: &SemanticFlow,
) -> FanoutAnalysis {
    // No joins means no fanout risk
    if !components.has_joins() {
        return FanoutAnalysis::flat();
    }

    // Check if all measures are on base table
    let measures_on_base = components.all_measures_on_base();
    if !measures_on_base {
        return FanoutAnalysis::flat();
    }

    // Check if there are filters on joined tables
    let has_join_filters = components.has_join_filters();
    if !has_join_filters {
        return FanoutAnalysis::flat();
    }

    // Check join compatibility - all joined dimensions/filters must be
    // direct joins to the base table (single-hop)
    let base_alias = &components.base_alias;

    // Collect all aliases we need from joins (direct references)
    let mut direct_aliases: HashSet<String> = HashSet::new();
    direct_aliases.extend(components.joined_dimension_aliases());
    direct_aliases.extend(components.joined_filter_aliases());

    // Expand to include all aliases in join chains back to base
    let needed_join_aliases = expand_join_chains(&direct_aliases, base_alias, &components.join_lookup);

    // Check that all joins in the chain are valid and analyze cardinality
    let mut joins_compatible = true;
    let mut has_fanout_risk = false;

    for alias in &needed_join_aliases {
        if let Some(join) = components.join_lookup.get(alias) {
            // Get the table we're joining from
            let from_alias = &join.to_table;

            // Get primary keys for both sides
            let left_pk: Grain = if from_alias == base_alias {
                components.base_semantic_table.primary_keys.iter().cloned().collect()
            } else if let Some(from_table) = components.alias_to_table.get(from_alias) {
                from_table.primary_keys.iter().cloned().collect()
            } else {
                joins_compatible = false;
                break;
            };

            // Get joined table's primary keys
            if let Some(joined_table) = components.alias_to_table.get(alias) {
                let right_pk: Grain = joined_table.primary_keys.iter().cloned().collect();

                // Infer cardinality for this hop (user hint takes precedence)
                let hint = join.cardinality.map(|c| c.into());
                let cardinality = infer_join_cardinality(join, &left_pk, &right_pk, hint);

                // Check if this join could cause fanout
                if could_cause_fanout_for_filter(cardinality, join) {
                    has_fanout_risk = true;
                }
            }
        } else {
            joins_compatible = false;
            break;
        }
    }

    if !joins_compatible {
        return FanoutAnalysis::flat();
    }

    // If no fanout risk detected, we can use a flat query
    // (all joins are ManyToOne or OneToOne)
    if !has_fanout_risk {
        return FanoutAnalysis::flat();
    }

    // Build join key mappings for pre-aggregation
    let mut join_key_mappings: HashMap<String, Vec<(String, String, String)>> = HashMap::new();
    for alias in &needed_join_aliases {
        if let Some(join) = components.join_lookup.get(alias) {
            let mappings: Vec<_> = join
                .join_keys
                .iter()
                .map(|k| {
                    let col_alias = format!("{}__{}", alias, k.left);
                    (col_alias, k.left.clone(), k.right.clone())
                })
                .collect();
            join_key_mappings.insert(alias.clone(), mappings);
        }
    }

    FanoutAnalysis {
        needs_preagg: true,
        join_key_mappings,
    }
}

/// Determine if a join with the given cardinality could cause fanout
/// when filtering on the joined table's dimensions.
///
/// - ManyToOne: Safe - each base row maps to at most one joined row
/// - OneToOne: Safe - 1:1 relationship
/// - OneToMany: Fanout risk - base row could match multiple joined rows
/// - ManyToMany: Fanout risk
/// - Unknown: Treat as fanout risk (conservative)
fn could_cause_fanout_for_filter(cardinality: Cardinality, join: &FlowJoin) -> bool {
    match cardinality {
        Cardinality::ManyToOne | Cardinality::OneToOne => false,
        Cardinality::OneToMany | Cardinality::ManyToMany => true,
        Cardinality::Unknown => {
            // For unknown cardinality, use join type as heuristic
            // LEFT joins to dimension tables are typically ManyToOne
            // but we can't be sure, so be conservative unless it's
            // a single-key join to the target's PK (checked elsewhere)
            !matches!(join.join_type, JoinType::Left)
        }
    }
}

// ============================================================================
// Multi-Grain Analysis (for multi-table measures)
// ============================================================================

/// Result of analyzing a query for multi-grain pre-aggregation needs.
#[derive(Debug, Clone)]
pub struct MultiGrainAnalysis {
    /// Whether multi-grain pre-aggregation is needed.
    pub needs_multi_grain: bool,
    /// Grain specification per table alias.
    pub table_grains: HashMap<String, TableGrain>,
    /// Specifications for joining CTEs together.
    pub cte_join_specs: Vec<CteJoinSpec>,
}

/// Grain specification for a single table.
#[derive(Debug, Clone)]
pub struct TableGrain {
    /// Columns that define the grain (GROUP BY columns).
    pub grain_columns: Vec<String>,
}

/// Specification for joining two CTEs.
#[derive(Debug, Clone)]
pub struct CteJoinSpec {
    /// Alias of the CTE being joined.
    pub from_alias: String,
    /// Alias of the CTE to join to.
    pub to_alias: String,
    /// Join type (matches the flow join type).
    pub join_type: SqlJoinType,
    /// Join keys: (from_column, to_column).
    pub join_keys: Vec<(String, String)>,
}

impl MultiGrainAnalysis {
    /// Create an analysis result indicating no multi-grain needed.
    pub fn flat() -> Self {
        Self {
            needs_multi_grain: false,
            table_grains: HashMap::new(),
            cte_join_specs: Vec::new(),
        }
    }
}

/// Analyze query components to determine if multi-grain pre-aggregation is needed.
///
/// Multi-grain is needed when:
/// 1. Measures come from multiple tables, OR
/// 2. Single-table measures with fanout risk (current preagg trigger)
///
/// This function unifies the old PreAggPlan logic into the new MultiGrainPlan
/// with N=1 CTE for single-table cases.
pub fn analyze_multi_grain(
    components: &QueryComponents,
    flow: &SemanticFlow,
) -> Result<MultiGrainAnalysis> {
    // Check for multi-table measures
    let multi_table = components.multi_table_measure_aliases();

    // If measures from multiple tables, we need multi-grain
    if let Some(table_aliases) = multi_table {
        return analyze_multi_table_measures(components, flow, &table_aliases);
    }

    // Otherwise, check if single-table fanout risk triggers preagg
    let fanout = analyze_fanout_risk(components, flow);
    if fanout.needs_preagg {
        return analyze_single_table_preagg(components, &fanout);
    }

    Ok(MultiGrainAnalysis::flat())
}

/// Analyze multi-table measure requirements.
fn analyze_multi_table_measures(
    components: &QueryComponents,
    _flow: &SemanticFlow,
    table_aliases: &[String],
) -> Result<MultiGrainAnalysis> {
    let base_alias = &components.base_alias;
    let mut table_grains = HashMap::new();
    let mut cte_join_specs = Vec::new();

    // For multi-table measures, we need a common grain (join point).
    // Each CTE should GROUP BY the columns needed to join to other CTEs.
    //
    // For a join: base (orders) ←→ joined (customers)
    //   join_keys.left  = column on base (e.g., customer_id)
    //   join_keys.right = column on joined (e.g., id)
    //
    // - Base table grain: the FK columns (join_keys.left)
    // - Joined table grain: the PK columns (join_keys.right)

    // First pass: collect grain for joined tables with measures
    for alias in table_aliases {
        if alias != base_alias {
            if let Some(join) = components.join_lookup.get(alias) {
                let cardinality = infer_cardinality_for_join(join, components)?;

                // For joined tables, grain is always join_keys.right (column on THIS table)
                // The cardinality just tells us if this is safe
                if matches!(cardinality, Cardinality::ManyToMany | Cardinality::Unknown) {
                    return Err(SemaflowError::Validation(format!(
                        "Multi-table measures require cardinality hint for join '{}' → '{}'. \
                         Add `cardinality: many_to_one` (or appropriate value) to the join definition.",
                        alias, join.to_table
                    )));
                }

                // Grain for this table = the columns on THIS table used in the join
                let grain_columns: Vec<String> =
                    join.join_keys.iter().map(|k| k.right.clone()).collect();

                table_grains.insert(
                    alias.clone(),
                    TableGrain { grain_columns },
                );

                // CTE join spec: joined CTE joins to base CTE
                // The join is: base_cte.left_col = joined_cte.right_col
                cte_join_specs.push(CteJoinSpec {
                    from_alias: alias.clone(),
                    to_alias: join.to_table.clone(),
                    join_type: join.join_type.clone().into(),
                    join_keys: join
                        .join_keys
                        .iter()
                        .map(|k| (k.left.clone(), k.right.clone()))
                        .collect(),
                });
            } else {
                return Err(SemaflowError::Validation(format!(
                    "No join definition found for table alias '{}'",
                    alias
                )));
            }
        }
    }

    // Second pass: base table grain = FK columns to other measure tables
    if table_aliases.contains(&base_alias.to_string()) {
        let mut base_grain_columns: Vec<String> = Vec::new();

        // Collect all FK columns from joins to tables with measures
        for alias in table_aliases {
            if alias != base_alias {
                if let Some(join) = components.join_lookup.get(alias) {
                    for k in &join.join_keys {
                        if !base_grain_columns.contains(&k.left) {
                            base_grain_columns.push(k.left.clone());
                        }
                    }
                }
            }
        }

        table_grains.insert(
            base_alias.clone(),
            TableGrain {
                grain_columns: base_grain_columns,
            },
        );
    }

    Ok(MultiGrainAnalysis {
        needs_multi_grain: true,
        table_grains,
        cte_join_specs,
    })
}

/// Analyze single-table preagg requirements (legacy path).
fn analyze_single_table_preagg(
    components: &QueryComponents,
    fanout: &FanoutAnalysis,
) -> Result<MultiGrainAnalysis> {
    let base_alias = &components.base_alias;

    // Single table with fanout risk - grain is the join keys needed for filtering
    let mut grain_columns: Vec<String> = Vec::new();
    for (_, mappings) in &fanout.join_key_mappings {
        for (_, base_col, _) in mappings {
            if !grain_columns.contains(base_col) {
                grain_columns.push(base_col.clone());
            }
        }
    }

    let mut table_grains = HashMap::new();
    table_grains.insert(
        base_alias.clone(),
        TableGrain { grain_columns },
    );

    // Build CTE join specs for dimension tables
    let mut cte_join_specs = Vec::new();
    for (alias, mappings) in &fanout.join_key_mappings {
        if let Some(join) = components.join_lookup.get(alias) {
            cte_join_specs.push(CteJoinSpec {
                from_alias: alias.clone(),
                to_alias: join.to_table.clone(),
                join_type: join.join_type.clone().into(),
                join_keys: mappings
                    .iter()
                    .map(|(_, base_col, right_col)| (base_col.clone(), right_col.clone()))
                    .collect(),
            });
        }
    }

    Ok(MultiGrainAnalysis {
        needs_multi_grain: true,
        table_grains,
        cte_join_specs,
    })
}

/// Infer cardinality for a join, using hints or PK-based inference.
fn infer_cardinality_for_join(
    join: &FlowJoin,
    components: &QueryComponents,
) -> Result<Cardinality> {
    // User hint takes precedence
    if let Some(hint) = join.cardinality {
        return Ok(hint.into());
    }

    // Get PKs for both sides
    let from_alias = &join.to_table;
    let to_alias = &join.alias;

    let left_pk: Grain = if from_alias == &components.base_alias {
        components
            .base_semantic_table
            .primary_keys
            .iter()
            .cloned()
            .collect()
    } else if let Some(table) = components.alias_to_table.get(from_alias) {
        table.primary_keys.iter().cloned().collect()
    } else {
        HashSet::new()
    };

    let right_pk: Grain = if let Some(table) = components.alias_to_table.get(to_alias) {
        table.primary_keys.iter().cloned().collect()
    } else {
        HashSet::new()
    };

    Ok(infer_join_cardinality(join, &left_pk, &right_pk, None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flows::{JoinKey, JoinType};

    fn make_join(alias: &str, to_table: &str, join_type: JoinType) -> FlowJoin {
        FlowJoin {
            semantic_table: "test_table".to_string(),
            alias: alias.to_string(),
            to_table: to_table.to_string(),
            join_type,
            join_keys: vec![JoinKey {
                left: "id".to_string(),
                right: "id".to_string(),
            }],
            cardinality: None,
            description: None,
        }
    }

    #[test]
    fn expand_join_chains_single_hop() {
        // o (base) <- c
        let mut join_lookup = HashMap::new();
        join_lookup.insert("c".to_string(), make_join("c", "o", JoinType::Left));

        let direct: HashSet<String> = ["c".to_string()].into_iter().collect();
        let expanded = expand_join_chains(&direct, "o", &join_lookup);

        assert_eq!(expanded.len(), 1);
        assert!(expanded.contains("c"));
    }

    #[test]
    fn expand_join_chains_multi_hop() {
        // o (base) <- c <- r
        let mut join_lookup = HashMap::new();
        join_lookup.insert("c".to_string(), make_join("c", "o", JoinType::Left));
        join_lookup.insert("r".to_string(), make_join("r", "c", JoinType::Left));

        // Only ask for "r", should expand to include "c" too
        let direct: HashSet<String> = ["r".to_string()].into_iter().collect();
        let expanded = expand_join_chains(&direct, "o", &join_lookup);

        assert_eq!(expanded.len(), 2);
        assert!(expanded.contains("c"));
        assert!(expanded.contains("r"));
    }

    #[test]
    fn expand_join_chains_avoids_cycles() {
        // Circular reference: c -> r -> c (invalid but shouldn't hang)
        let mut join_lookup = HashMap::new();
        join_lookup.insert("c".to_string(), make_join("c", "r", JoinType::Left));
        join_lookup.insert("r".to_string(), make_join("r", "c", JoinType::Left));

        let direct: HashSet<String> = ["c".to_string()].into_iter().collect();
        let expanded = expand_join_chains(&direct, "o", &join_lookup);

        // Should terminate without hanging
        assert!(expanded.contains("c"));
        assert!(expanded.contains("r"));
    }
}
