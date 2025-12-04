use semaflow::dialect::DuckDbDialect;
use semaflow::flows::{
    Aggregation, Expr, FlowJoin, FlowTableRef, Function, QueryRequest, SemanticFlow, SemanticTable,
    TimeGrain,
};
use semaflow::query_builder::SqlBuilder;
use semaflow::registry::FlowRegistry;
use semaflow::SemaflowError;

fn inline_registry() -> FlowRegistry {
    let table = SemanticTable {
        data_source: "ds1".to_string(),
        name: "orders".to_string(),
        table: "orders".to_string(),
        primary_key: "id".to_string(),
        time_dimension: Some("created_at".to_string()),
        smallest_time_grain: None,
        dimensions: [
            (
                "country".to_string(),
                semaflow::flows::Dimension {
                    expression: Expr::Column {
                        column: "country".to_string(),
                    },
                    data_type: None,
                    description: None,
                },
            ),
            (
                "month".to_string(),
                semaflow::flows::Dimension {
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
                semaflow::flows::Measure {
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
                semaflow::flows::Measure {
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
        joins: std::collections::BTreeMap::<String, FlowJoin>::new(),
        description: None,
    };

    FlowRegistry::from_parts(vec![table], vec![flow])
}

#[test]
fn build_with_functions_and_distinct() {
    let registry = inline_registry();
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
fn measure_filters_rejected() {
    let registry = inline_registry();
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
    let mut registry = inline_registry();
    // Add a joined table that shares a dimension name to force ambiguity.
    let customers = SemanticTable {
        data_source: "ds1".to_string(),
        name: "customers".to_string(),
        table: "customers".to_string(),
        primary_key: "id".to_string(),
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

    let flow = semaflow::flows::SemanticFlow {
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
                join_type: semaflow::flows::JoinType::Left,
                join_keys: vec![semaflow::flows::JoinKey {
                    left: "id".to_string(),
                    right: "id".to_string(),
                }],
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
fn preaggregates_when_filtering_on_join_dimension() {
    let mut registry = inline_registry();
    let customers = SemanticTable {
        data_source: "ds1".to_string(),
        name: "customers".to_string(),
        table: "customers".to_string(),
        primary_key: "id".to_string(),
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

    let flow = semaflow::flows::SemanticFlow {
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
                join_type: semaflow::flows::JoinType::Left,
                join_keys: vec![semaflow::flows::JoinKey {
                    left: "customer_id".to_string(),
                    right: "id".to_string(),
                }],
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
        "expected derived table for pre-aggregation; sql={sql}"
    );
    assert!(
        sql.contains("EXISTS (SELECT true FROM \"customers\" \"c\""),
        "dimension filter should be applied via EXISTS; sql={sql}"
    );
    assert!(
        sql.contains(
            "LEFT JOIN \"customers\" \"c\" ON (\"fact_preagg\".\"c__customer_id\" = \"c\".\"id\")"
        ),
        "outer join should connect pre-agg keys to dimension; sql={sql}"
    );
    assert!(
        sql.contains("\"c\".\"country\" AS \"customer_country\""),
        "outer select should project dimension from joined table; sql={sql}"
    );
}
