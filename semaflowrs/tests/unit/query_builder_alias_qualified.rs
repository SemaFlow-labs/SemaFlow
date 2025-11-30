use semaflow::dialect::DuckDbDialect;
use semaflow::flows::{
    Aggregation, Expr, FlowJoin, FlowTableRef, QueryRequest, SemanticFlow, SemanticTable,
};
use semaflow::query_builder::SqlBuilder;
use semaflow::registry::FlowRegistry;

fn registry_with_join() -> FlowRegistry {
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

    let orders = SemanticTable {
        data_source: "ds1".to_string(),
        name: "orders".to_string(),
        table: "orders".to_string(),
        primary_key: "id".to_string(),
        time_dimension: None,
        smallest_time_grain: None,
        dimensions: [(
            "amount".to_string(),
            semaflow::flows::Dimension {
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
            semaflow::flows::Measure {
                expr: Expr::Column {
                    column: "amount".to_string(),
                },
                agg: Aggregation::Sum,
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

    FlowRegistry::from_parts(vec![orders, customers], vec![flow])
}

#[test]
fn accepts_alias_qualified_fields() {
    let registry = registry_with_join();
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
