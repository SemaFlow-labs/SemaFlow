use semaflow::dialect::DuckDbDialect;
use semaflow::flows::{
    Aggregation, Expr, FlowJoin, FlowTableRef, QueryRequest, SemanticFlow, SemanticTable,
};
use semaflow::query_builder::SqlBuilder;
use semaflow::registry::FlowRegistry;

fn registry_with_chain() -> FlowRegistry {
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

    let regions = SemanticTable {
        data_source: "ds1".to_string(),
        name: "regions".to_string(),
        table: "regions".to_string(),
        primary_key: "id".to_string(),
        time_dimension: None,
        smallest_time_grain: None,
        dimensions: [(
            "region".to_string(),
            semaflow::flows::Dimension {
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
                    join_type: semaflow::flows::JoinType::Left,
                    join_keys: vec![semaflow::flows::JoinKey {
                        left: "id".to_string(),
                        right: "id".to_string(),
                    }],
                    description: None,
                },
            ),
            (
                "r".to_string(),
                FlowJoin {
                    semantic_table: "regions".to_string(),
                    alias: "r".to_string(),
                    to_table: "c".to_string(),
                    join_type: semaflow::flows::JoinType::Left,
                    join_keys: vec![semaflow::flows::JoinKey {
                        left: "country".to_string(),
                        right: "id".to_string(),
                    }],
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

#[test]
fn prunes_all_joins_when_only_base_fields_used() {
    let registry = registry_with_chain();
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
    let registry = registry_with_chain();
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
    let registry = registry_with_chain();
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
    let mut registry = registry_with_chain();
    if let Some(flow) = registry.flows.get_mut("sales") {
        if let Some(join) = flow.joins.get_mut("c") {
            join.join_type = semaflow::flows::JoinType::Inner;
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
