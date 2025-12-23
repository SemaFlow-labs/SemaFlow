//! Integration tests for SQL AST rendering.
//!
//! These tests exercise the SqlRenderer with various query structures.

use semaflow::dialect::DuckDbDialect;
#[cfg(feature = "postgres")]
use semaflow::dialect::PostgresDialect;
use semaflow::flows::{Aggregation, Function, SortDirection, TimeGrain};
use semaflow::sql_ast::{
    Join, OrderItem, SelectItem, SelectQuery, SqlBinaryOperator, SqlExpr, SqlJoinType, SqlRenderer,
    TableRef,
};

fn col(table: &str, name: &str) -> SqlExpr {
    SqlExpr::Column {
        table: Some(table.to_string()),
        name: name.to_string(),
    }
}

fn order_asc(name: &str) -> OrderItem {
    OrderItem {
        expr: SqlExpr::Column {
            table: None,
            name: name.to_string(),
        },
        direction: SortDirection::Asc,
    }
}

#[test]
fn renders_join_group_order_and_aggregates() {
    let dialect = DuckDbDialect;
    let mut query = SelectQuery::default();
    query.from = TableRef {
        name: "orders".to_string(),
        alias: Some("o".to_string()),
        subquery: None,
    };
    query.select = vec![
        SelectItem {
            expr: col("o", "country"),
            alias: Some("country".to_string()),
        },
        SelectItem {
            expr: SqlExpr::Aggregate {
                agg: Aggregation::CountDistinct,
                expr: Box::new(col("o", "customer_id")),
            },
            alias: Some("distinct_customers".to_string()),
        },
    ];
    query.group_by.push(col("o", "country"));
    query.joins.push(Join {
        join_type: SqlJoinType::Left,
        table: TableRef {
            name: "customers".to_string(),
            alias: Some("c".to_string()),
            subquery: None,
        },
        on: vec![SqlExpr::BinaryOp {
            op: SqlBinaryOperator::Eq,
            left: Box::new(col("o", "customer_id")),
            right: Box::new(col("c", "id")),
        }],
    });
    query.filters.push(SqlExpr::BinaryOp {
        op: SqlBinaryOperator::Eq,
        left: Box::new(col("o", "country")),
        right: Box::new(SqlExpr::Literal(serde_json::json!("US"))),
    });
    query.order_by.push(order_asc("country"));
    query.limit = Some(10);
    query.offset = Some(5);

    let sql = SqlRenderer::new(&dialect).render_select(&query);
    assert!(sql.contains("FROM \"orders\" \"o\""));
    assert!(sql.contains("LEFT JOIN \"customers\" \"c\" ON (\"o\".\"customer_id\" = \"c\".\"id\")"));
    assert!(sql.contains("COUNT(DISTINCT \"o\".\"customer_id\") AS \"distinct_customers\""));
    assert!(sql.contains("WHERE (\"o\".\"country\" = 'US')"));
    assert!(sql.contains("GROUP BY \"o\".\"country\""));
    assert!(sql.contains("ORDER BY \"country\" ASC"));
    assert!(sql.contains("LIMIT 10"));
    assert!(sql.contains("OFFSET 5"));
}

#[test]
fn renders_functions_and_not_in_list() {
    let dialect = DuckDbDialect;
    let mut query = SelectQuery::default();
    query.from = TableRef {
        name: "orders".to_string(),
        alias: Some("o".to_string()),
        subquery: None,
    };
    query.select.push(SelectItem {
        expr: SqlExpr::Function {
            func: Function::DateTrunc(TimeGrain::Month),
            args: vec![col("o", "created_at")],
        },
        alias: Some("month".to_string()),
    });
    query.filters.push(SqlExpr::InList {
        expr: Box::new(col("o", "country")),
        list: vec![
            SqlExpr::Literal(serde_json::json!("US")),
            SqlExpr::Literal(serde_json::json!("UK")),
        ],
        negated: true,
    });
    query.limit = Some(5);
    query.offset = Some(10);

    let sql = SqlRenderer::new(&dialect).render_select(&query);
    assert!(sql.contains("date_trunc('month', \"o\".\"created_at\") AS \"month\""));
    assert!(sql.contains("\"o\".\"country\" NOT IN ('US', 'UK')"));
    assert!(sql.ends_with("LIMIT 5 OFFSET 10"));
}

#[test]
fn renders_filtered_aggregate_when_supported() {
    let dialect = DuckDbDialect;
    let mut query = SelectQuery::default();
    query.from = TableRef {
        name: "orders".to_string(),
        alias: Some("o".to_string()),
        subquery: None,
    };
    query.select.push(SelectItem {
        expr: SqlExpr::FilteredAggregate {
            agg: Aggregation::Sum,
            expr: Box::new(col("o", "amount")),
            filter: Box::new(SqlExpr::BinaryOp {
                op: SqlBinaryOperator::Eq,
                left: Box::new(col("o", "country")),
                right: Box::new(SqlExpr::Literal(serde_json::json!("US"))),
            }),
        },
        alias: Some("us_amount".to_string()),
    });

    let sql = SqlRenderer::new(&dialect).render_select(&query);
    assert!(sql.contains("SUM(\"o\".\"amount\") FILTER (WHERE (\"o\".\"country\" = 'US'))"));
}

// ============================================================================
// PostgreSQL Dialect Tests
// ============================================================================

#[cfg(feature = "postgres")]
mod postgres_tests {
    use super::*;

    #[test]
    fn renders_postgres_date_trunc() {
        let dialect = PostgresDialect::new("public");
        let mut query = SelectQuery::default();
        query.from = TableRef {
            name: "orders".to_string(),
            alias: Some("o".to_string()),
            subquery: None,
        };
        query.select.push(SelectItem {
            expr: SqlExpr::Function {
                func: Function::DateTrunc(TimeGrain::Month),
                args: vec![col("o", "created_at")],
            },
            alias: Some("month".to_string()),
        });

        let sql = SqlRenderer::new(&dialect).render_select(&query);
        // PostgreSQL uses same date_trunc syntax as DuckDB
        assert!(sql.contains("date_trunc('month', \"o\".\"created_at\") AS \"month\""));
    }

    #[test]
    fn renders_postgres_filtered_aggregate() {
        let dialect = PostgresDialect::new("public");
        let mut query = SelectQuery::default();
        query.from = TableRef {
            name: "orders".to_string(),
            alias: Some("o".to_string()),
            subquery: None,
        };
        query.select.push(SelectItem {
            expr: SqlExpr::FilteredAggregate {
                agg: Aggregation::Sum,
                expr: Box::new(col("o", "amount")),
                filter: Box::new(SqlExpr::BinaryOp {
                    op: SqlBinaryOperator::Eq,
                    left: Box::new(col("o", "country")),
                    right: Box::new(SqlExpr::Literal(serde_json::json!("US"))),
                }),
            },
            alias: Some("us_amount".to_string()),
        });

        let sql = SqlRenderer::new(&dialect).render_select(&query);
        // PostgreSQL 9.4+ supports FILTER syntax, with FLOAT8 cast for numeric safety
        assert!(
            sql.contains("SUM(\"o\".\"amount\")::FLOAT8 FILTER (WHERE (\"o\".\"country\" = 'US'))"),
            "sql={sql}"
        );
    }

    #[test]
    fn renders_postgres_median_as_percentile_cont() {
        let dialect = PostgresDialect::new("public");
        let mut query = SelectQuery::default();
        query.from = TableRef {
            name: "orders".to_string(),
            alias: Some("o".to_string()),
            subquery: None,
        };
        query.select.push(SelectItem {
            expr: SqlExpr::Aggregate {
                agg: Aggregation::Median,
                expr: Box::new(col("o", "amount")),
            },
            alias: Some("median_amount".to_string()),
        });

        let sql = SqlRenderer::new(&dialect).render_select(&query);
        // PostgreSQL uses PERCENTILE_CONT for MEDIAN
        assert!(
            sql.contains("PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY \"o\".\"amount\")"),
            "PostgreSQL should render MEDIAN as PERCENTILE_CONT; sql={sql}"
        );
    }

    #[test]
    fn renders_postgres_first_as_array_agg() {
        let dialect = PostgresDialect::new("public");
        let mut query = SelectQuery::default();
        query.from = TableRef {
            name: "orders".to_string(),
            alias: Some("o".to_string()),
            subquery: None,
        };
        query.select.push(SelectItem {
            expr: SqlExpr::Aggregate {
                agg: Aggregation::First,
                expr: Box::new(col("o", "amount")),
            },
            alias: Some("first_amount".to_string()),
        });

        let sql = SqlRenderer::new(&dialect).render_select(&query);
        // PostgreSQL uses (array_agg(x))[1] for FIRST
        assert!(
            sql.contains("(array_agg(\"o\".\"amount\"))[1]"),
            "PostgreSQL should render FIRST as array_agg[1]; sql={sql}"
        );
    }

    fn col(table: &str, name: &str) -> SqlExpr {
        SqlExpr::Column {
            table: Some(table.to_string()),
            name: name.to_string(),
        }
    }
}
