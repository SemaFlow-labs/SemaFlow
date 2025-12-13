//! Integration tests for the query builder.
//!
//! These tests exercise the public API: SqlBuilder, FlowRegistry, QueryRequest.

use semaflow::dialect::DuckDbDialect;
#[cfg(feature = "postgres")]
use semaflow::dialect::PostgresDialect;
use semaflow::flows::{
    Aggregation, BinaryOp, Expr, FlowJoin, FlowTableRef, Function, JoinKey, JoinType, Measure,
    QueryRequest, SemanticFlow, SemanticTable,
};
use semaflow::query_builder::SqlBuilder;
use semaflow::registry::FlowRegistry;
use semaflow::SemaflowError;

// ============================================================================
// Test fixtures
// ============================================================================

mod fixtures {
    use super::*;
    use semaflow::flows::{Dimension, TimeGrain};

    pub fn simple_orders_registry() -> FlowRegistry {
        let table = SemanticTable {
            data_source: "ds1".to_string(),
            name: "orders".to_string(),
            table: "orders".to_string(),
            primary_keys: vec!["id".to_string()],
            time_dimension: Some("created_at".to_string()),
            smallest_time_grain: None,
            dimensions: [
                (
                    "country".to_string(),
                    Dimension {
                        expression: Expr::Column {
                            column: "country".to_string(),
                        },
                        data_type: None,
                        description: None,
                    },
                ),
                (
                    "month".to_string(),
                    Dimension {
                        expression: Expr::Func {
                            func: Function::DateTrunc(TimeGrain::Month),
                            args: vec![Expr::Column {
                                column: "created_at".to_string(),
                            }],
                        },
                        data_type: None,
                        description: None,
                    },
                ),
            ]
            .into_iter()
            .collect(),
            measures: [
                (
                    "order_total".to_string(),
                    Measure {
                        expr: Expr::Column {
                            column: "amount".to_string(),
                        },
                        agg: Aggregation::Sum,
                        filter: None,
                        post_expr: None,
                        data_type: None,
                        description: None,
                    },
                ),
                (
                    "distinct_customers".to_string(),
                    Measure {
                        expr: Expr::Column {
                            column: "customer_id".to_string(),
                        },
                        agg: Aggregation::CountDistinct,
                        filter: None,
                        post_expr: None,
                        data_type: None,
                        description: None,
                    },
                ),
            ]
            .into_iter()
            .collect(),
            description: None,
        };

        let flow = SemanticFlow {
            name: "sales".to_string(),
            base_table: FlowTableRef {
                semantic_table: "orders".to_string(),
                alias: "o".to_string(),
            },
            joins: std::collections::BTreeMap::new(),
            description: None,
        };

        FlowRegistry::from_parts(vec![table], vec![flow])
    }

    pub fn orders_with_customers_registry() -> FlowRegistry {
        let orders = SemanticTable {
            data_source: "ds1".to_string(),
            name: "orders".to_string(),
            table: "orders".to_string(),
            primary_keys: vec!["id".to_string()],
            time_dimension: None,
            smallest_time_grain: None,
            dimensions: [(
                "amount".to_string(),
                Dimension {
                    expression: Expr::Column {
                        column: "amount".to_string(),
                    },
                    data_type: None,
                    description: None,
                },
            )]
            .into_iter()
            .collect(),
            measures: [(
                "order_total".to_string(),
                Measure {
                    expr: Expr::Column {
                        column: "amount".to_string(),
                    },
                    agg: Aggregation::Sum,
                    filter: None,
                    post_expr: None,
                    data_type: None,
                    description: None,
                },
            )]
            .into_iter()
            .collect(),
            description: None,
        };

        let customers = SemanticTable {
            data_source: "ds1".to_string(),
            name: "customers".to_string(),
            table: "customers".to_string(),
            primary_keys: vec!["id".to_string()],
            time_dimension: None,
            smallest_time_grain: None,
            dimensions: [(
                "country".to_string(),
                Dimension {
                    expression: Expr::Column {
                        column: "country".to_string(),
                    },
                    data_type: None,
                    description: None,
                },
            )]
            .into_iter()
            .collect(),
            measures: Default::default(),
            description: None,
        };

        let flow = SemanticFlow {
            name: "sales".to_string(),
            base_table: FlowTableRef {
                semantic_table: "orders".to_string(),
                alias: "o".to_string(),
            },
            joins: [(
                "customers".to_string(),
                FlowJoin {
                    semantic_table: "customers".to_string(),
                    alias: "c".to_string(),
                    to_table: "o".to_string(),
                    join_type: JoinType::Left,
                    join_keys: vec![JoinKey {
                        left: "id".to_string(),
                        right: "id".to_string(),
                    }],
                    cardinality: None,
                    description: None,
                },
            )]
            .into_iter()
            .collect(),
            description: None,
        };

        FlowRegistry::from_parts(vec![orders, customers], vec![flow])
    }

    pub fn chain_registry() -> FlowRegistry {
        let orders = SemanticTable {
            data_source: "ds1".to_string(),
            name: "orders".to_string(),
            table: "orders".to_string(),
            primary_keys: vec!["id".to_string()],
            time_dimension: None,
            smallest_time_grain: None,
            dimensions: [(
                "amount".to_string(),
                Dimension {
                    expression: Expr::Column {
                        column: "amount".to_string(),
                    },
                    data_type: None,
                    description: None,
                },
            )]
            .into_iter()
            .collect(),
            measures: [(
                "order_total".to_string(),
                Measure {
                    expr: Expr::Column {
                        column: "amount".to_string(),
                    },
                    agg: Aggregation::Sum,
                    filter: None,
                    post_expr: None,
                    data_type: None,
                    description: None,
                },
            )]
            .into_iter()
            .collect(),
            description: None,
        };

        let customers = SemanticTable {
            data_source: "ds1".to_string(),
            name: "customers".to_string(),
            table: "customers".to_string(),
            primary_keys: vec!["id".to_string()],
            time_dimension: None,
            smallest_time_grain: None,
            dimensions: [(
                "country".to_string(),
                Dimension {
                    expression: Expr::Column {
                        column: "country".to_string(),
                    },
                    data_type: None,
                    description: None,
                },
            )]
            .into_iter()
            .collect(),
            measures: Default::default(),
            description: None,
        };

        let regions = SemanticTable {
            data_source: "ds1".to_string(),
            name: "regions".to_string(),
            table: "regions".to_string(),
            primary_keys: vec!["id".to_string()],
            time_dimension: None,
            smallest_time_grain: None,
            dimensions: [(
                "region".to_string(),
                Dimension {
                    expression: Expr::Column {
                        column: "region".to_string(),
                    },
                    data_type: None,
                    description: None,
                },
            )]
            .into_iter()
            .collect(),
            measures: Default::default(),
            description: None,
        };

        let flow = SemanticFlow {
            name: "sales".to_string(),
            base_table: FlowTableRef {
                semantic_table: "orders".to_string(),
                alias: "o".to_string(),
            },
            joins: [
                (
                    "c".to_string(),
                    FlowJoin {
                        semantic_table: "customers".to_string(),
                        alias: "c".to_string(),
                        to_table: "o".to_string(),
                        join_type: JoinType::Left,
                        join_keys: vec![JoinKey {
                            left: "id".to_string(),
                            right: "id".to_string(),
                        }],
                        cardinality: None,
                        description: None,
                    },
                ),
                (
                    "r".to_string(),
                    FlowJoin {
                        semantic_table: "regions".to_string(),
                        alias: "r".to_string(),
                        to_table: "c".to_string(),
                        join_type: JoinType::Left,
                        join_keys: vec![JoinKey {
                            left: "country".to_string(),
                            right: "id".to_string(),
                        }],
                        cardinality: None,
                        description: None,
                    },
                ),
            ]
            .into_iter()
            .collect(),
            description: None,
        };

        FlowRegistry::from_parts(vec![orders, customers, regions], vec![flow])
    }

    pub fn measures_registry() -> FlowRegistry {
        let table = SemanticTable {
            data_source: "ds1".to_string(),
            name: "orders".to_string(),
            table: "orders".to_string(),
            primary_keys: vec!["id".to_string()],
            time_dimension: None,
            smallest_time_grain: None,
            dimensions: [(
                "country".to_string(),
                Dimension {
                    expression: Expr::Column {
                        column: "country".to_string(),
                    },
                    data_type: None,
                    description: None,
                },
            )]
            .into_iter()
            .collect(),
            measures: [
                (
                    "sum_amount".to_string(),
                    Measure {
                        expr: Expr::Column {
                            column: "amount".to_string(),
                        },
                        agg: Aggregation::Sum,
                        filter: None,
                        post_expr: None,
                        data_type: None,
                        description: None,
                    },
                ),
                (
                    "cnt_orders".to_string(),
                    Measure {
                        expr: Expr::Column {
                            column: "id".to_string(),
                        },
                        agg: Aggregation::Count,
                        filter: None,
                        post_expr: None,
                        data_type: None,
                        description: None,
                    },
                ),
                (
                    "us_amount".to_string(),
                    Measure {
                        expr: Expr::Column {
                            column: "amount".to_string(),
                        },
                        agg: Aggregation::Sum,
                        filter: Some(Expr::Binary {
                            op: BinaryOp::Eq,
                            left: Box::new(Expr::Column {
                                column: "country".to_string(),
                            }),
                            right: Box::new(Expr::Literal {
                                value: serde_json::json!("US"),
                            }),
                        }),
                        post_expr: None,
                        data_type: None,
                        description: None,
                    },
                ),
                (
                    "avg_amount".to_string(),
                    Measure {
                        expr: Expr::Column {
                            column: "amount".to_string(),
                        },
                        agg: Aggregation::Sum,
                        filter: None,
                        post_expr: Some(Expr::Func {
                            func: Function::SafeDivide,
                            args: vec![
                                Expr::MeasureRef {
                                    name: "sum_amount".to_string(),
                                },
                                Expr::MeasureRef {
                                    name: "cnt_orders".to_string(),
                                },
                            ],
                        }),
                        data_type: None,
                        description: None,
                    },
                ),
            ]
            .into_iter()
            .collect(),
            description: None,
        };

        let flow = SemanticFlow {
            name: "sales".to_string(),
            base_table: FlowTableRef {
                semantic_table: "orders".to_string(),
                alias: "o".to_string(),
            },
            joins: Default::default(),
            description: None,
        };

        FlowRegistry::from_parts(vec![table], vec![flow])
    }
}

// ============================================================================
// Basic query building tests
// ============================================================================

#[test]
fn build_with_functions_and_distinct() {
    let registry = fixtures::simple_orders_registry();
    let request = QueryRequest {
        flow: "sales".to_string(),
        dimensions: vec!["month".to_string()],
        measures: vec!["distinct_customers".to_string()],
        filters: vec![],
        order: vec![],
        limit: None,
        offset: None,
    };
    let sql = SqlBuilder::default()
        .build_with_dialect(&registry, &request, &DuckDbDialect)
        .unwrap();
    assert!(sql.contains("date_trunc('month'"));
    assert!(sql.contains("COUNT(DISTINCT"));
    assert!(sql.contains("FROM \"orders\" \"o\""));
}

#[test]
fn accepts_alias_qualified_fields() {
    let registry = fixtures::orders_with_customers_registry();
    let request = QueryRequest {
        flow: "sales".to_string(),
        dimensions: vec!["c.country".to_string()],
        measures: vec!["o.order_total".to_string()],
        filters: vec![],
        order: vec![],
        limit: None,
        offset: None,
    };
    let sql = SqlBuilder::default()
        .build_with_dialect(&registry, &request, &DuckDbDialect)
        .unwrap();
    assert!(sql.contains("\"c\".\"country\""));
    assert!(sql.contains("\"o\".\"amount\""));
}

// ============================================================================
// Validation tests
// ============================================================================

#[test]
fn measure_filters_rejected() {
    let registry = fixtures::simple_orders_registry();
    let request = QueryRequest {
        flow: "sales".to_string(),
        dimensions: vec!["country".to_string()],
        measures: vec!["order_total".to_string()],
        filters: vec![semaflow::flows::Filter {
            field: "order_total".to_string(),
            op: semaflow::flows::FilterOp::Eq,
            value: serde_json::json!(1),
        }],
        order: vec![],
        limit: None,
        offset: None,
    };
    let err = SqlBuilder::default()
        .build_with_dialect(&registry, &request, &DuckDbDialect)
        .unwrap_err();
    match err {
        SemaflowError::Validation(msg) => {
            assert!(msg.contains("filters on measures"));
        }
        other => panic!("unexpected error {other:?}"),
    }
}

#[test]
fn unqualified_fields_error_when_ambiguous() {
    let mut registry = fixtures::simple_orders_registry();
    let customers = SemanticTable {
        data_source: "ds1".to_string(),
        name: "customers".to_string(),
        table: "customers".to_string(),
        primary_keys: vec!["id".to_string()],
        time_dimension: None,
        smallest_time_grain: None,
        dimensions: [(
            "country".to_string(),
            semaflow::flows::Dimension {
                expression: Expr::Column {
                    column: "country".to_string(),
                },
                data_type: None,
                description: None,
            },
        )]
        .into_iter()
        .collect(),
        measures: Default::default(),
        description: None,
    };

    let flow = SemanticFlow {
        name: "sales".to_string(),
        base_table: FlowTableRef {
            semantic_table: "orders".to_string(),
            alias: "o".to_string(),
        },
        joins: [(
            "customers".to_string(),
            FlowJoin {
                semantic_table: "customers".to_string(),
                alias: "c".to_string(),
                to_table: "o".to_string(),
                join_type: JoinType::Left,
                join_keys: vec![JoinKey {
                    left: "id".to_string(),
                    right: "id".to_string(),
                }],
                cardinality: None,
                description: None,
            },
        )]
        .into_iter()
        .collect(),
        description: None,
    };

    registry.tables.insert(customers.name.clone(), customers);
    registry.flows.insert(flow.name.clone(), flow);

    let request = QueryRequest {
        flow: "sales".to_string(),
        dimensions: vec!["country".to_string()],
        measures: vec![],
        filters: vec![],
        order: vec![],
        limit: None,
        offset: None,
    };
    let err = SqlBuilder::default()
        .build_with_dialect(&registry, &request, &DuckDbDialect)
        .unwrap_err();
    match err {
        SemaflowError::Validation(msg) => {
            assert!(msg.contains("ambiguous"));
            assert!(msg.contains("o"));
            assert!(msg.contains("c"));
        }
        other => panic!("unexpected error {other:?}"),
    }
}

#[test]
fn supports_measures_from_multiple_tables() {
    let mut registry = fixtures::simple_orders_registry();

    let customers = SemanticTable {
        data_source: "ds1".to_string(),
        name: "customers".to_string(),
        table: "customers".to_string(),
        primary_keys: vec!["id".to_string()],
        time_dimension: None,
        smallest_time_grain: None,
        dimensions: [].into_iter().collect(),
        measures: [(
            "customer_count".to_string(),
            Measure {
                expr: Expr::Column {
                    column: "id".to_string(),
                },
                agg: Aggregation::Count,
                filter: None,
                post_expr: None,
                data_type: None,
                description: None,
            },
        )]
        .into_iter()
        .collect(),
        description: None,
    };

    let flow = SemanticFlow {
        name: "sales".to_string(),
        base_table: FlowTableRef {
            semantic_table: "orders".to_string(),
            alias: "o".to_string(),
        },
        joins: [(
            "customers".to_string(),
            FlowJoin {
                semantic_table: "customers".to_string(),
                alias: "c".to_string(),
                to_table: "o".to_string(),
                join_type: JoinType::Left,
                join_keys: vec![JoinKey {
                    left: "customer_id".to_string(),
                    right: "id".to_string(),
                }],
                cardinality: None,
                description: None,
            },
        )]
        .into_iter()
        .collect(),
        description: None,
    };

    registry.tables.insert(customers.name.clone(), customers);
    registry.flows.insert(flow.name.clone(), flow);

    let request = QueryRequest {
        flow: "sales".to_string(),
        dimensions: vec![],
        measures: vec![
            "o.order_total".to_string(),
            "c.customer_count".to_string(),
        ],
        filters: vec![],
        order: vec![],
        limit: None,
        offset: None,
    };

    // Multi-table measures are now supported via multi-grain pre-aggregation
    let sql = SqlBuilder::default()
        .build_with_dialect(&registry, &request, &DuckDbDialect)
        .unwrap();

    // Should create CTEs for each table with measures
    assert!(
        sql.contains("FROM (SELECT"),
        "should use subquery for pre-aggregation; sql={sql}"
    );
    assert!(
        sql.contains("o_agg") && sql.contains("c_agg"),
        "should have separate CTEs for each table; sql={sql}"
    );
    assert!(
        sql.contains("order_total") && sql.contains("customer_count"),
        "should include both measures; sql={sql}"
    );
    assert!(
        sql.contains("LEFT JOIN"),
        "should join CTEs with flow join type; sql={sql}"
    );
}

// ============================================================================
// Join pruning tests
// ============================================================================

#[test]
fn prunes_all_joins_when_only_base_fields_used() {
    let registry = fixtures::chain_registry();
    let request = QueryRequest {
        flow: "sales".to_string(),
        dimensions: vec!["o.amount".to_string()],
        measures: vec!["o.order_total".to_string()],
        filters: vec![],
        order: vec![],
        limit: None,
        offset: None,
    };
    let sql = SqlBuilder::default()
        .build_with_dialect(&registry, &request, &DuckDbDialect)
        .unwrap();
    assert!(
        !sql.contains("JOIN"),
        "expected no joins when only base fields are requested, got {sql}"
    );
}

#[test]
fn includes_only_needed_join_for_single_hop_dimension() {
    let registry = fixtures::chain_registry();
    let request = QueryRequest {
        flow: "sales".to_string(),
        dimensions: vec!["c.country".to_string()],
        measures: vec!["o.order_total".to_string()],
        filters: vec![],
        order: vec![],
        limit: None,
        offset: None,
    };
    let sql = SqlBuilder::default()
        .build_with_dialect(&registry, &request, &DuckDbDialect)
        .unwrap();
    assert!(
        sql.contains("JOIN \"customers\" \"c\""),
        "expected customers join, got {sql}"
    );
    assert!(
        !sql.contains("JOIN \"regions\" \"r\""),
        "expected regions join to be pruned, got {sql}"
    );
}

#[test]
fn includes_dependency_chain_for_deeper_dimension() {
    let registry = fixtures::chain_registry();
    let request = QueryRequest {
        flow: "sales".to_string(),
        dimensions: vec!["r.region".to_string()],
        measures: vec!["o.order_total".to_string()],
        filters: vec![],
        order: vec![],
        limit: None,
        offset: None,
    };
    let sql = SqlBuilder::default()
        .build_with_dialect(&registry, &request, &DuckDbDialect)
        .unwrap();
    let customers_join_idx = sql
        .find("JOIN \"customers\" \"c\"")
        .expect("customers join missing");
    let regions_join_idx = sql
        .find("JOIN \"regions\" \"r\"")
        .expect("regions join missing");
    assert!(
        customers_join_idx < regions_join_idx,
        "expected parent join before dependent; sql={sql}"
    );
}

#[test]
fn keeps_inner_join_when_unused() {
    let mut registry = fixtures::chain_registry();
    if let Some(flow) = registry.flows.get_mut("sales") {
        if let Some(join) = flow.joins.get_mut("c") {
            join.join_type = JoinType::Inner;
        }
    }
    let request = QueryRequest {
        flow: "sales".to_string(),
        dimensions: vec!["o.amount".to_string()],
        measures: vec!["o.order_total".to_string()],
        filters: vec![],
        order: vec![],
        limit: None,
        offset: None,
    };
    let sql = SqlBuilder::default()
        .build_with_dialect(&registry, &request, &DuckDbDialect)
        .unwrap();
    assert!(
        sql.contains("JOIN \"customers\" \"c\""),
        "inner join should not be pruned; sql={sql}"
    );
}

// ============================================================================
// Cardinality and pre-aggregation tests
// ============================================================================

#[test]
fn uses_flat_query_for_many_to_one_join_filter() {
    let mut registry = fixtures::simple_orders_registry();
    let customers = SemanticTable {
        data_source: "ds1".to_string(),
        name: "customers".to_string(),
        table: "customers".to_string(),
        primary_keys: vec!["id".to_string()],
        time_dimension: None,
        smallest_time_grain: None,
        dimensions: [(
            "customer_country".to_string(),
            semaflow::flows::Dimension {
                expression: Expr::Column {
                    column: "country".to_string(),
                },
                data_type: None,
                description: None,
            },
        )]
        .into_iter()
        .collect(),
        measures: Default::default(),
        description: None,
    };

    let flow = SemanticFlow {
        name: "sales".to_string(),
        base_table: FlowTableRef {
            semantic_table: "orders".to_string(),
            alias: "o".to_string(),
        },
        joins: [(
            "customers".to_string(),
            FlowJoin {
                semantic_table: "customers".to_string(),
                alias: "c".to_string(),
                to_table: "o".to_string(),
                join_type: JoinType::Left,
                join_keys: vec![JoinKey {
                    left: "customer_id".to_string(),
                    right: "id".to_string(), // Joining to customers.id (PK) = ManyToOne
                }],
                cardinality: None,
                description: None,
            },
        )]
        .into_iter()
        .collect(),
        description: None,
    };

    registry.tables.insert(customers.name.clone(), customers);
    registry.flows.insert(flow.name.clone(), flow);

    let request = QueryRequest {
        flow: "sales".to_string(),
        dimensions: vec!["customer_country".to_string()],
        measures: vec!["order_total".to_string()],
        filters: vec![semaflow::flows::Filter {
            field: "customer_country".to_string(),
            op: semaflow::flows::FilterOp::Eq,
            value: serde_json::json!("US"),
        }],
        order: vec![],
        limit: None,
        offset: None,
    };

    let sql = SqlBuilder::default()
        .build_with_dialect(&registry, &request, &DuckDbDialect)
        .unwrap();

    assert!(
        !sql.contains("FROM (SELECT"),
        "ManyToOne join should use flat query, not pre-aggregation; sql={sql}"
    );
    assert!(
        sql.contains("LEFT JOIN \"customers\" \"c\" ON"),
        "should have direct LEFT JOIN; sql={sql}"
    );
    assert!(
        sql.contains("WHERE") && sql.contains("\"c\".\"country\" = 'US'"),
        "filter should be in WHERE clause; sql={sql}"
    );
}

#[test]
fn preaggregates_when_join_cardinality_unknown() {
    let mut registry = fixtures::simple_orders_registry();
    let customers = SemanticTable {
        data_source: "ds1".to_string(),
        name: "customers".to_string(),
        table: "customers".to_string(),
        primary_keys: vec!["id".to_string()],
        time_dimension: None,
        smallest_time_grain: None,
        dimensions: [(
            "customer_country".to_string(),
            semaflow::flows::Dimension {
                expression: Expr::Column {
                    column: "country".to_string(),
                },
                data_type: None,
                description: None,
            },
        )]
        .into_iter()
        .collect(),
        measures: Default::default(),
        description: None,
    };

    let flow = SemanticFlow {
        name: "sales".to_string(),
        base_table: FlowTableRef {
            semantic_table: "orders".to_string(),
            alias: "o".to_string(),
        },
        joins: [(
            "customers".to_string(),
            FlowJoin {
                semantic_table: "customers".to_string(),
                alias: "c".to_string(),
                to_table: "o".to_string(),
                join_type: JoinType::Inner,
                join_keys: vec![JoinKey {
                    left: "customer_id".to_string(),
                    right: "external_id".to_string(), // NOT the PK - unknown cardinality
                }],
                cardinality: None,
                description: None,
            },
        )]
        .into_iter()
        .collect(),
        description: None,
    };

    registry.tables.insert(customers.name.clone(), customers);
    registry.flows.insert(flow.name.clone(), flow);

    let request = QueryRequest {
        flow: "sales".to_string(),
        dimensions: vec!["customer_country".to_string()],
        measures: vec!["order_total".to_string()],
        filters: vec![semaflow::flows::Filter {
            field: "customer_country".to_string(),
            op: semaflow::flows::FilterOp::Eq,
            value: serde_json::json!("US"),
        }],
        order: vec![],
        limit: None,
        offset: None,
    };

    let sql = SqlBuilder::default()
        .build_with_dialect(&registry, &request, &DuckDbDialect)
        .unwrap();

    assert!(
        sql.contains("FROM (SELECT"),
        "unknown cardinality should use pre-aggregation; sql={sql}"
    );
    // Filter on dimension-only table is applied as WHERE clause on final query
    assert!(
        sql.contains("WHERE") && sql.contains("country") && sql.contains("'US'"),
        "dimension filter should be applied as WHERE clause; sql={sql}"
    );
}

// ============================================================================
// Measure expression tests
// ============================================================================

#[test]
fn renders_filtered_measure() {
    let registry = fixtures::measures_registry();
    let request = QueryRequest {
        flow: "sales".to_string(),
        dimensions: vec![],
        measures: vec!["us_amount".to_string()],
        filters: vec![],
        order: vec![],
        limit: None,
        offset: None,
    };
    let sql = SqlBuilder::default()
        .build_with_dialect(&registry, &request, &DuckDbDialect)
        .unwrap();
    assert!(
        sql.contains("SUM(\"o\".\"amount\") FILTER (WHERE (\"o\".\"country\" = 'US'))"),
        "filtered measure not rendered with FILTER; sql={sql}"
    );
}

#[test]
fn renders_composite_measure_with_safe_divide() {
    let registry = fixtures::measures_registry();
    let request = QueryRequest {
        flow: "sales".to_string(),
        dimensions: vec![],
        measures: vec![
            "sum_amount".to_string(),
            "cnt_orders".to_string(),
            "avg_amount".to_string(),
        ],
        filters: vec![],
        order: vec![],
        limit: None,
        offset: None,
    };
    let sql = SqlBuilder::default()
        .build_with_dialect(&registry, &request, &DuckDbDialect)
        .unwrap();
    assert!(sql.contains("SUM(\"o\".\"amount\") AS \"sum_amount\""));
    assert!(sql.contains("COUNT(\"o\".\"id\") AS \"cnt_orders\""));
    assert!(
        sql.contains("/ NULLIF(") || sql.contains("safe_divide"),
        "composite measure should use safe divide; sql={sql}"
    );
}

struct NoFilterDialect;

impl semaflow::dialect::Dialect for NoFilterDialect {
    fn quote_ident(&self, ident: &str) -> String {
        format!("`{}`", ident)
    }

    fn render_function(&self, func: &Function, args: Vec<String>) -> String {
        DuckDbDialect.render_function(func, args)
    }
}

#[test]
fn falls_back_to_case_when_filter_not_supported() {
    let registry = fixtures::measures_registry();
    let request = QueryRequest {
        flow: "sales".to_string(),
        dimensions: vec![],
        measures: vec!["us_amount".to_string()],
        filters: vec![],
        order: vec![],
        limit: None,
        offset: None,
    };
    let sql = SqlBuilder::default()
        .build_with_dialect(&registry, &request, &NoFilterDialect)
        .unwrap();
    assert!(
        sql.contains("SUM(CASE WHEN (`o`.`country` = 'US') THEN `o`.`amount` ELSE NULL END)"),
        "filtered measure should render with CASE when dialect lacks FILTER support; sql={sql}"
    );
}

// ============================================================================
// PostgreSQL Dialect Tests
// ============================================================================

#[cfg(feature = "postgres")]
mod postgres_tests {
    use super::*;

    #[test]
    fn build_basic_query_with_postgres_dialect() {
        let registry = fixtures::simple_orders_registry();
        let request = QueryRequest {
            flow: "sales".to_string(),
            dimensions: vec!["country".to_string()],
            measures: vec!["order_total".to_string()],
            filters: vec![],
            order: vec![],
            limit: Some(10),
            offset: None,
        };
        let sql = SqlBuilder::default()
            .build_with_dialect(&registry, &request, &PostgresDialect)
            .unwrap();

        // PostgreSQL uses same quoting as DuckDB
        assert!(sql.contains("\"o\".\"country\""));
        assert!(sql.contains("SUM(\"o\".\"amount\")"));
        assert!(sql.contains("FROM \"orders\" \"o\""));
        assert!(sql.contains("GROUP BY"));
        assert!(sql.contains("LIMIT 10"));
    }

    #[test]
    fn postgres_renders_filtered_measure_with_filter_syntax() {
        let registry = fixtures::measures_registry();
        let request = QueryRequest {
            flow: "sales".to_string(),
            dimensions: vec![],
            measures: vec!["us_amount".to_string()],
            filters: vec![],
            order: vec![],
            limit: None,
            offset: None,
        };
        let sql = SqlBuilder::default()
            .build_with_dialect(&registry, &request, &PostgresDialect)
            .unwrap();

        // PostgreSQL supports FILTER (WHERE ...) syntax
        assert!(
            sql.contains("SUM(\"o\".\"amount\") FILTER (WHERE (\"o\".\"country\" = 'US'))"),
            "PostgreSQL should use FILTER syntax for filtered measures; sql={sql}"
        );
    }

    #[test]
    fn postgres_handles_join_with_filters() {
        let registry = fixtures::orders_with_customers_registry();
        let request = QueryRequest {
            flow: "sales".to_string(),
            dimensions: vec!["c.country".to_string()],
            measures: vec!["o.order_total".to_string()],
            filters: vec![semaflow::flows::Filter {
                field: "c.country".to_string(),
                op: semaflow::flows::FilterOp::Eq,
                value: serde_json::json!("US"),
            }],
            order: vec![],
            limit: None,
            offset: None,
        };
        let sql = SqlBuilder::default()
            .build_with_dialect(&registry, &request, &PostgresDialect)
            .unwrap();

        // Should include the filter
        assert!(
            sql.contains("'US'"),
            "should include filter value; sql={sql}"
        );
        // Should have correct quoting
        assert!(
            sql.contains("\"c\".\"country\""),
            "should use double-quote identifiers; sql={sql}"
        );
    }

    #[test]
    fn postgres_handles_composite_measure() {
        let registry = fixtures::measures_registry();
        let request = QueryRequest {
            flow: "sales".to_string(),
            dimensions: vec![],
            measures: vec!["avg_amount".to_string()],
            filters: vec![],
            order: vec![],
            limit: None,
            offset: None,
        };
        let sql = SqlBuilder::default()
            .build_with_dialect(&registry, &request, &PostgresDialect)
            .unwrap();

        // Should use safe divide (NULLIF pattern)
        assert!(
            sql.contains("NULLIF") || sql.contains("safe_divide"),
            "composite measure should use safe divide; sql={sql}"
        );
    }
}
