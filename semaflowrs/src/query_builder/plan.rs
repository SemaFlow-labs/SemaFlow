//! Query plan intermediate representation.
//!
//! The query plan IR separates the decision of what query structure to use
//! from the actual SQL generation. This allows for:
//! - Clear separation between planning and rendering
//! - Easier testing of plan generation
//! - Future optimizations at the plan level

use crate::sql_ast::{Join, OrderItem, SelectItem, SelectQuery, SqlExpr, SqlJoinType, TableRef};

/// The top-level query plan, either flat or multi-grain pre-aggregated.
#[derive(Debug, Clone)]
pub enum QueryPlan {
    /// Standard flat query with direct joins and GROUP BY.
    Flat(FlatPlan),
    /// Multi-grain pre-aggregated query (1+ CTEs joined together).
    MultiGrain(MultiGrainPlan),
}

/// A flat query plan - standard SELECT with JOINs and GROUP BY.
#[derive(Debug, Clone)]
pub struct FlatPlan {
    pub from: TableRef,
    pub select: Vec<SelectItem>,
    pub joins: Vec<Join>,
    pub filters: Vec<SqlExpr>,
    pub group_by: Vec<SqlExpr>,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

// ============================================================================
// Multi-Grain Plan (unified pre-aggregation for 1+ tables with measures)
// ============================================================================

/// Unified plan for pre-aggregation (1 or more tables).
/// Each table with measures gets its own CTE, aggregated to a common grain.
#[derive(Debug, Clone)]
pub struct MultiGrainPlan {
    /// One CTE per table with measures.
    pub ctes: Vec<GrainedAggPlan>,
    /// Final query that joins CTEs and dimension tables.
    pub final_query: FinalQueryPlan,
}

/// Single-table aggregation CTE with its grain (GROUP BY columns).
#[derive(Debug, Clone)]
pub struct GrainedAggPlan {
    /// Table alias (e.g., "o" for orders).
    pub alias: String,
    /// The source table.
    pub from: TableRef,
    /// Grain columns + aggregated measures.
    pub select: Vec<SelectItem>,
    /// WHERE filters for this table.
    pub filters: Vec<SqlExpr>,
    /// The grain columns (GROUP BY).
    pub group_by: Vec<SqlExpr>,
}

/// The final query that joins CTEs and dimension tables.
#[derive(Debug, Clone)]
pub struct FinalQueryPlan {
    /// Alias of the base CTE (first CTE in joins).
    pub base_cte_alias: String,
    /// SELECT items for the final output.
    pub select: Vec<SelectItem>,
    /// Joins between CTEs.
    pub cte_joins: Vec<CteJoin>,
    /// Joins to dimension tables (for non-measure dimensions).
    pub dimension_joins: Vec<Join>,
    /// WHERE filters on the final query (e.g., filters on dimension-only tables).
    pub filters: Vec<SqlExpr>,
    /// ORDER BY clause.
    pub order_by: Vec<OrderItem>,
    /// LIMIT clause.
    pub limit: Option<u64>,
    /// OFFSET clause.
    pub offset: Option<u64>,
}

/// A join between two CTEs in a multi-grain plan.
#[derive(Debug, Clone)]
pub struct CteJoin {
    /// Alias of the CTE being joined.
    pub cte_alias: String,
    /// Alias of the CTE being joined to.
    pub to_cte_alias: String,
    /// Join type (matches the flow join type).
    pub join_type: SqlJoinType,
    /// Join keys: (left_col, right_col).
    pub on: Vec<(String, String)>,
}

impl QueryPlan {
    /// Convert the plan to a SelectQuery for rendering.
    pub fn to_select_query(self) -> SelectQuery {
        match self {
            QueryPlan::Flat(flat) => flat.to_select_query(),
            QueryPlan::MultiGrain(mg) => mg.to_select_query(),
        }
    }
}

impl FlatPlan {
    /// Create a new empty flat plan with the given FROM clause.
    pub fn new(from: TableRef) -> Self {
        Self {
            from,
            select: Vec::new(),
            joins: Vec::new(),
            filters: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        }
    }

    /// Convert to SelectQuery for rendering.
    pub fn to_select_query(self) -> SelectQuery {
        SelectQuery {
            select: self.select,
            from: self.from,
            joins: self.joins,
            filters: self.filters,
            group_by: self.group_by,
            order_by: self.order_by,
            limit: self.limit,
            offset: self.offset,
        }
    }
}

impl MultiGrainPlan {
    /// Convert to SelectQuery for rendering.
    ///
    /// Creates nested subqueries: first CTE becomes the FROM clause,
    /// subsequent CTEs become subquery joins, then dimension tables join.
    pub fn to_select_query(self) -> SelectQuery {
        use std::collections::HashMap;

        assert!(!self.ctes.is_empty(), "MultiGrainPlan must have at least one CTE");

        // Build a lookup from alias to CTE subquery
        let mut cte_map: HashMap<String, SelectQuery> = self
            .ctes
            .into_iter()
            .map(|cte| {
                let alias = cte.alias.clone();
                let query = SelectQuery {
                    select: cte.select,
                    from: cte.from,
                    joins: Vec::new(),
                    filters: cte.filters,
                    group_by: cte.group_by,
                    order_by: Vec::new(),
                    limit: None,
                    offset: None,
                };
                (alias, query)
            })
            .collect();

        // Extract the base CTE
        let base_alias = &self.final_query.base_cte_alias;
        let base_query = cte_map
            .remove(base_alias)
            .expect("Base CTE alias not found in CTEs");

        let base_from = TableRef {
            name: String::new(),
            alias: Some(base_alias.clone()),
            subquery: Some(Box::new(base_query)),
        };

        let mut joins = Vec::new();

        // Add CTE joins with their subqueries
        for cte_join in self.final_query.cte_joins {
            let subquery = cte_map.remove(&cte_join.cte_alias);

            joins.push(Join {
                join_type: cte_join.join_type,
                table: TableRef {
                    name: String::new(),
                    alias: Some(cte_join.cte_alias.clone()),
                    subquery: subquery.map(Box::new),
                },
                on: cte_join
                    .on
                    .into_iter()
                    .map(|(left, right)| SqlExpr::BinaryOp {
                        op: crate::sql_ast::SqlBinaryOperator::Eq,
                        left: Box::new(SqlExpr::Column {
                            table: Some(cte_join.to_cte_alias.clone()),
                            name: left,
                        }),
                        right: Box::new(SqlExpr::Column {
                            table: Some(cte_join.cte_alias.clone()),
                            name: right,
                        }),
                    })
                    .collect(),
            });
        }

        // Add dimension joins
        joins.extend(self.final_query.dimension_joins);

        SelectQuery {
            select: self.final_query.select,
            from: base_from,
            joins,
            filters: self.final_query.filters,
            group_by: Vec::new(),
            order_by: self.final_query.order_by,
            limit: self.final_query.limit,
            offset: self.final_query.offset,
        }
    }
}

impl GrainedAggPlan {
    /// Create a new empty grained aggregation plan.
    pub fn new(alias: String, from: TableRef) -> Self {
        Self {
            alias,
            from,
            select: Vec::new(),
            filters: Vec::new(),
            group_by: Vec::new(),
        }
    }
}

impl FinalQueryPlan {
    /// Create a new empty final query plan.
    pub fn new(base_cte_alias: String) -> Self {
        Self {
            base_cte_alias,
            select: Vec::new(),
            cte_joins: Vec::new(),
            dimension_joins: Vec::new(),
            filters: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flat_plan_converts_to_select_query() {
        let mut plan = FlatPlan::new(TableRef {
            name: "orders".to_string(),
            alias: Some("o".to_string()),
            subquery: None,
        });
        plan.select.push(SelectItem {
            expr: SqlExpr::Column {
                table: Some("o".to_string()),
                name: "country".to_string(),
            },
            alias: Some("country".to_string()),
        });
        plan.group_by.push(SqlExpr::Column {
            table: Some("o".to_string()),
            name: "country".to_string(),
        });
        plan.limit = Some(10);

        let query = plan.to_select_query();
        assert_eq!(query.select.len(), 1);
        assert_eq!(query.group_by.len(), 1);
        assert_eq!(query.limit, Some(10));
    }

    #[test]
    fn multi_grain_plan_creates_nested_query() {
        let cte = GrainedAggPlan::new(
            "o_agg".to_string(),
            TableRef {
                name: "orders".to_string(),
                alias: Some("o".to_string()),
                subquery: None,
            },
        );
        let final_query = FinalQueryPlan::new("o_agg".to_string());
        let plan = MultiGrainPlan {
            ctes: vec![cte],
            final_query,
        };

        let query = plan.to_select_query();
        assert!(query.from.subquery.is_some());
        assert_eq!(query.from.alias, Some("o_agg".to_string()));
    }
}
