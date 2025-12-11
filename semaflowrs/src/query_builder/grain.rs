//! Grain and cardinality inference for query planning.
//!
//! This module provides types and functions for understanding the cardinality
//! relationships between joined tables. This information is used to make smart
//! decisions about query strategies (flat vs pre-aggregated).

use std::collections::HashSet;

use crate::flows::{FlowJoin, JoinCardinality};

/// Cardinality of a relationship between two tables.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cardinality {
    /// Each row on the left maps to many rows on the right (1:N)
    OneToMany,
    /// Each row on the left maps to exactly one row on the right (1:1)
    OneToOne,
    /// Many rows on the left can map to many rows on the right (M:N)
    ManyToMany,
    /// Many rows on the left map to one row on the right (N:1)
    ManyToOne,
    /// Cardinality cannot be determined
    Unknown,
}

/// Convert user-facing JoinCardinality to internal Cardinality.
impl From<JoinCardinality> for Cardinality {
    fn from(jc: JoinCardinality) -> Self {
        match jc {
            JoinCardinality::ManyToOne => Cardinality::ManyToOne,
            JoinCardinality::OneToMany => Cardinality::OneToMany,
            JoinCardinality::OneToOne => Cardinality::OneToOne,
            JoinCardinality::ManyToMany => Cardinality::ManyToMany,
        }
    }
}

/// The grain of a table - the set of columns that uniquely identify a row.
pub type Grain = HashSet<String>;

/// Infer the cardinality of a join relationship.
///
/// Logic:
/// - If right join keys == right table's PK → Many-to-One (each left row maps to at most one right row)
/// - If left join keys == left table's PK → One-to-Many (each right row maps to at most one left row)
/// - If both → One-to-One
/// - Otherwise → Unknown (potentially Many-to-Many)
///
/// A hint can override the inference if cardinality is known from domain knowledge.
pub fn infer_join_cardinality(
    join: &FlowJoin,
    left_pk: &Grain,
    right_pk: &Grain,
    hint: Option<Cardinality>,
) -> Cardinality {
    // Explicit hint overrides inference
    if let Some(h) = hint {
        return h;
    }

    let join_keys_right: HashSet<String> = join.join_keys.iter().map(|k| k.right.clone()).collect();
    let join_keys_left: HashSet<String> = join.join_keys.iter().map(|k| k.left.clone()).collect();

    let right_is_pk = !right_pk.is_empty() && join_keys_right == *right_pk;
    let left_is_pk = !left_pk.is_empty() && join_keys_left == *left_pk;

    match (left_is_pk, right_is_pk) {
        (true, true) => Cardinality::OneToOne,
        (true, false) => Cardinality::OneToMany,
        (false, true) => Cardinality::ManyToOne,
        (false, false) => Cardinality::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flows::{JoinKey, JoinType};

    fn make_join(left_col: &str, right_col: &str) -> FlowJoin {
        FlowJoin {
            semantic_table: "target".to_string(),
            alias: "t".to_string(),
            to_table: "source".to_string(),
            join_type: JoinType::Left,
            join_keys: vec![JoinKey {
                left: left_col.to_string(),
                right: right_col.to_string(),
            }],
            cardinality: None,
            description: None,
        }
    }

    #[test]
    fn infers_many_to_one_when_right_is_pk() {
        let join = make_join("customer_id", "id");
        let left_pk: Grain = ["order_id"].iter().map(|s| s.to_string()).collect();
        let right_pk: Grain = ["id"].iter().map(|s| s.to_string()).collect();

        let cardinality = infer_join_cardinality(&join, &left_pk, &right_pk, None);
        assert_eq!(cardinality, Cardinality::ManyToOne);
    }

    #[test]
    fn infers_one_to_many_when_left_is_pk() {
        let join = make_join("id", "customer_id");
        let left_pk: Grain = ["id"].iter().map(|s| s.to_string()).collect();
        let right_pk: Grain = ["order_id"].iter().map(|s| s.to_string()).collect();

        let cardinality = infer_join_cardinality(&join, &left_pk, &right_pk, None);
        assert_eq!(cardinality, Cardinality::OneToMany);
    }

    #[test]
    fn infers_one_to_one_when_both_pk() {
        let join = make_join("id", "id");
        let left_pk: Grain = ["id"].iter().map(|s| s.to_string()).collect();
        let right_pk: Grain = ["id"].iter().map(|s| s.to_string()).collect();

        let cardinality = infer_join_cardinality(&join, &left_pk, &right_pk, None);
        assert_eq!(cardinality, Cardinality::OneToOne);
    }

    #[test]
    fn infers_unknown_when_neither_pk() {
        let join = make_join("tag_id", "item_id");
        let left_pk: Grain = ["id"].iter().map(|s| s.to_string()).collect();
        let right_pk: Grain = ["id"].iter().map(|s| s.to_string()).collect();

        let cardinality = infer_join_cardinality(&join, &left_pk, &right_pk, None);
        assert_eq!(cardinality, Cardinality::Unknown);
    }

    #[test]
    fn hint_overrides_inference() {
        let join = make_join("tag_id", "item_id");
        let left_pk = Grain::new();
        let right_pk = Grain::new();

        let cardinality =
            infer_join_cardinality(&join, &left_pk, &right_pk, Some(Cardinality::ManyToOne));
        assert_eq!(cardinality, Cardinality::ManyToOne);
    }
}
