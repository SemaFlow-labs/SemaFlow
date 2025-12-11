//! Integration tests for the FlowRegistry introspection API.

use semaflow::flows::{
    Aggregation, Expr, FlowJoin, FlowTableRef, JoinKey, JoinType, SemanticFlow, SemanticTable,
};
use semaflow::registry::FlowRegistry;

fn introspection_registry() -> FlowRegistry {
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
                data_type: Some("string".to_string()),
                description: Some("customer country".to_string()),
            },
        )]
        .into_iter()
        .collect(),
        measures: Default::default(),
        description: Some("customer table".to_string()),
    };

    let orders = SemanticTable {
        data_source: "ds1".to_string(),
        name: "orders".to_string(),
        table: "orders".to_string(),
        primary_keys: vec!["id".to_string()],
        time_dimension: Some("created_at".to_string()),
        smallest_time_grain: None,
        dimensions: [(
            "id".to_string(),
            semaflow::flows::Dimension {
                expression: Expr::Column {
                    column: "id".to_string(),
                },
                data_type: Some("int".to_string()),
                description: Some("order id".to_string()),
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
                filter: None,
                post_expr: None,
                data_type: Some("double".to_string()),
                description: Some("sum of amounts".to_string()),
            },
        )]
        .into_iter()
        .collect(),
        description: Some("orders table".to_string()),
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
                description: Some("customer join".to_string()),
            },
        )]
        .into_iter()
        .collect(),
        description: Some("sales flow".to_string()),
    };

    FlowRegistry::from_parts(vec![customers, orders], vec![flow])
}

#[test]
fn list_flow_summaries_returns_names_and_descriptions() {
    let registry = introspection_registry();
    let summaries = registry.list_flow_summaries();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].name, "sales");
    assert_eq!(summaries[0].description.as_deref(), Some("sales flow"));
}

#[test]
fn flow_schema_includes_dimensions_measures_and_joins() {
    let registry = introspection_registry();
    let schema = registry.flow_schema("sales").expect("schema");
    assert_eq!(schema.name, "sales");
    assert_eq!(schema.data_source, "ds1");
    assert_eq!(schema.time_dimension.as_deref(), Some("created_at"));
    assert!(schema.smallest_time_grain.is_none());

    let dim_names: Vec<_> = schema.dimensions.iter().map(|d| d.name.as_str()).collect();
    assert!(dim_names.contains(&"country"));
    assert!(dim_names.contains(&"id"));

    let measure_names: Vec<_> = schema.measures.iter().map(|m| m.name.as_str()).collect();
    assert!(measure_names.contains(&"order_total"));
}
