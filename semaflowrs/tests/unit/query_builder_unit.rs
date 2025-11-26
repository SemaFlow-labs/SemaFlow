use semaflow_core::dialect::DuckDbDialect;
use semaflow_core::models::{
    Aggregation, Expr, Function, ModelJoin, ModelTableRef, QueryRequest, SemanticModel,
    SemanticTable, TimeGrain,
};
use semaflow_core::query_builder::SqlBuilder;
use semaflow_core::registry::ModelRegistry;
use semaflow_core::SemaflowError;

fn inline_registry() -> ModelRegistry {
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
                semaflow_core::models::Dimension {
                    expression: Expr::Column {
                        column: "country".to_string(),
                    },
                    data_type: None,
                    description: None,
                },
            ),
            (
                "month".to_string(),
                semaflow_core::models::Dimension {
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
                semaflow_core::models::Measure {
                    expr: Expr::Column {
                        column: "amount".to_string(),
                    },
                    agg: Aggregation::Sum,
                    data_type: None,
                    description: None,
                },
            ),
            (
                "distinct_customers".to_string(),
                semaflow_core::models::Measure {
                    expr: Expr::Column {
                        column: "customer_id".to_string(),
                    },
                    agg: Aggregation::CountDistinct,
                    data_type: None,
                    description: None,
                },
            ),
        ]
        .into_iter()
        .collect(),
        description: None,
    };

    let model = SemanticModel {
        name: "sales".to_string(),
        base_table: ModelTableRef {
            semantic_table: "orders".to_string(),
            alias: "o".to_string(),
        },
        joins: std::collections::BTreeMap::<String, ModelJoin>::new(),
        description: None,
    };

    ModelRegistry::from_parts(vec![table], vec![model])
}

#[test]
fn build_with_functions_and_distinct() {
    let registry = inline_registry();
    let request = QueryRequest {
        model: "sales".to_string(),
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
        model: "sales".to_string(),
        dimensions: vec!["country".to_string()],
        measures: vec!["order_total".to_string()],
        filters: vec![semaflow_core::models::Filter {
            field: "order_total".to_string(),
            op: semaflow_core::models::FilterOp::Eq,
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
