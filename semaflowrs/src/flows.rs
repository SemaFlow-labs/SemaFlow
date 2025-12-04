use std::collections::BTreeMap;

use crate::expr_parser::parse_expr;
use serde::{de, Deserializer};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticTable {
    pub data_source: String,
    pub name: String,
    pub table: String,
    pub primary_key: String,
    pub time_dimension: Option<String>,
    pub smallest_time_grain: Option<TimeGrain>,
    #[serde(default)]
    pub dimensions: BTreeMap<String, Dimension>,
    #[serde(default)]
    pub measures: BTreeMap<String, Measure>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Dimension {
    pub expression: Expr,
    pub data_type: Option<String>,
    pub description: Option<String>,
}

impl<'de> Deserialize<'de> for Dimension {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        match value {
            Value::String(s) => Ok(Dimension {
                expression: Expr::Column { column: s },
                data_type: None,
                description: None,
            }),
            other => {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Full {
                    expression: Expr,
                    data_type: Option<String>,
                    description: Option<String>,
                }
                let full = Full::deserialize(other).map_err(de::Error::custom)?;
                Ok(Dimension {
                    expression: full.expression,
                    data_type: full.data_type,
                    description: full.description,
                })
            }
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Measure {
    pub expr: Expr,
    pub agg: Aggregation,
    #[serde(default)]
    pub filter: Option<Expr>,
    #[serde(default)]
    pub post_expr: Option<Expr>,
    pub data_type: Option<String>,
    pub description: Option<String>,
}

impl<'de> Deserialize<'de> for Measure {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            expr: Value,
            agg: Aggregation,
            #[serde(default)]
            filter: Option<Value>,
            #[serde(default)]
            post_expr: Option<Value>,
            data_type: Option<String>,
            description: Option<String>,
        }
        let raw = Raw::deserialize(deserializer)?;
        let expr: Expr = serde_json::from_value(raw.expr).map_err(de::Error::custom)?;

        let filter = match raw.filter {
            Some(Value::String(s)) => parse_expr(&s)
                .ok()
                .or_else(|| Some(Expr::Column { column: s.clone() })),
            Some(other) => Some(serde_json::from_value(other).map_err(de::Error::custom)?),
            None => None,
        };
        let post_expr = match raw.post_expr {
            Some(Value::String(s)) => parse_expr(&s)
                .ok()
                .or_else(|| Some(Expr::Column { column: s.clone() })),
            Some(other) => Some(serde_json::from_value(other).map_err(de::Error::custom)?),
            None => None,
        };

        Ok(Measure {
            expr,
            agg: raw.agg,
            filter,
            post_expr,
            data_type: raw.data_type,
            description: raw.description,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Expr {
    Column {
        column: String,
    },
    Literal {
        value: Value,
    },
    MeasureRef {
        name: String,
    },
    Func {
        func: Function,
        args: Vec<Expr>,
    },
    Case {
        branches: Vec<CaseBranch>,
        else_expr: Box<Expr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
}

impl<'de> Deserialize<'de> for Expr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            Value::String(s) => Ok(Expr::Column { column: s }),
            Value::Object(map) if map.len() == 1 && map.contains_key("measure") => {
                let name = map
                    .get("measure")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| de::Error::custom("measure reference must be a string"))?;
                Ok(Expr::MeasureRef {
                    name: name.to_string(),
                })
            }
            other => {
                #[derive(Deserialize)]
                #[serde(tag = "type", rename_all = "snake_case")]
                enum TaggedExpr {
                    Column {
                        column: String,
                    },
                    Literal {
                        value: Value,
                    },
                    MeasureRef {
                        name: String,
                    },
                    Func {
                        func: Function,
                        args: Vec<Expr>,
                    },
                    Case {
                        branches: Vec<CaseBranch>,
                        else_expr: Box<Expr>,
                    },
                    Binary {
                        op: BinaryOp,
                        left: Box<Expr>,
                        right: Box<Expr>,
                    },
                }
                let tagged: TaggedExpr =
                    TaggedExpr::deserialize(other).map_err(de::Error::custom)?;
                Ok(match tagged {
                    TaggedExpr::Column { column } => Expr::Column { column },
                    TaggedExpr::Literal { value } => Expr::Literal { value },
                    TaggedExpr::MeasureRef { name } => Expr::MeasureRef { name },
                    TaggedExpr::Func { func, args } => Expr::Func { func, args },
                    TaggedExpr::Case {
                        branches,
                        else_expr,
                    } => Expr::Case {
                        branches,
                        else_expr,
                    },
                    TaggedExpr::Binary { op, left, right } => Expr::Binary { op, left, right },
                })
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseBranch {
    pub when: Expr,
    pub then: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Function {
    DateTrunc(TimeGrain),
    DatePart { field: String },
    Lower,
    Upper,
    Coalesce,
    IfNull,
    Now,
    Concat,
    ConcatWs { sep: String },
    Substring,
    Length,
    Greatest,
    Least,
    Cast { data_type: String },
    Trim,
    Ltrim,
    Rtrim,
    SafeDivide,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    And,
    Or,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Aggregation {
    Sum,
    Count,
    CountDistinct,
    Min,
    Max,
    Avg,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeGrain {
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticFlow {
    pub name: String,
    pub base_table: FlowTableRef,
    #[serde(default)]
    pub joins: BTreeMap<String, FlowJoin>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlowTableRef {
    pub semantic_table: String,
    pub alias: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlowJoin {
    pub semantic_table: String,
    pub alias: String,
    pub to_table: String,
    pub join_type: JoinType,
    pub join_keys: Vec<JoinKey>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinKey {
    pub left: String,
    pub right: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct QueryRequest {
    pub flow: String,
    #[serde(default)]
    pub dimensions: Vec<String>,
    #[serde(default)]
    pub measures: Vec<String>,
    #[serde(default)]
    pub filters: Vec<Filter>,
    #[serde(default)]
    pub order: Vec<OrderItem>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Filter {
    pub field: String,
    pub op: FilterOp,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterOp {
    #[serde(rename = "==")]
    Eq,
    #[serde(rename = "!=")]
    Neq,
    #[serde(rename = ">")]
    Gt,
    #[serde(rename = ">=")]
    Gte,
    #[serde(rename = "<")]
    Lt,
    #[serde(rename = "<=")]
    Lte,
    #[serde(rename = "in")]
    In,
    #[serde(rename = "not in")]
    NotIn,
    #[serde(rename = "like")]
    Like,
    #[serde(rename = "ilike")]
    ILike,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderItem {
    pub column: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Asc,
    Desc,
}
