use semaflow::dialect::DuckDbDialect;
use semaflow::flows::{
    Aggregation, Expr, FlowTableRef, Function, Measure, QueryRequest, SemanticFlow, SemanticTable,
};
use semaflow::query_builder::SqlBuilder;
use semaflow::registry::FlowRegistry;

fn registry_with_measures() -> FlowRegistry {
    let table = SemanticTable {
        data_source: "ds1".to_string(),
        name: "orders".to_string(),
        table: "orders".to_string(),
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
                        op: semaflow::flows::BinaryOp::Eq,
                        left: Box::new(Expr::Column {
                            column: "country".to_string(),
                        }),
                        right: Box::new(Expr::Literal {
                            value: serde_json::Value::String("US".to_string()),
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

#[test]
fn renders_filtered_measure() {
    let registry = registry_with_measures();
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
    let registry = registry_with_measures();
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
        sql.contains("safe_divide(\"sum_amount\", \"cnt_orders\") AS \"avg_amount\"")
            || sql.contains(
                "SUM(\"o\".\"amount\") / NULLIF(COUNT(\"o\".\"id\"), 0) AS \"avg_amount\""
            ),
        "composite measure should divide sum by count; sql={sql}"
    );
}

struct NoFilterDialect;

impl semaflow::dialect::Dialect for NoFilterDialect {
    fn quote_ident(&self, ident: &str) -> String {
        format!("`{}`", ident)
    }

    fn render_function(&self, func: &semaflow::flows::Function, args: Vec<String>) -> String {
        semaflow::dialect::DuckDbDialect.render_function(func, args)
    }
}

#[test]
fn falls_back_to_case_when_filter_not_supported() {
    let registry = registry_with_measures();
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
