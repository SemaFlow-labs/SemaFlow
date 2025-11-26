use std::collections::BTreeMap;

use serde::{de, Deserializer};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Measure {
    pub expr: Expr,
    pub agg: Aggregation,
    pub data_type: Option<String>,
    pub description: Option<String>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
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
pub struct SemanticModel {
    pub name: String,
    pub base_table: ModelTableRef,
    #[serde(default)]
    pub joins: BTreeMap<String, ModelJoin>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelTableRef {
    pub semantic_table: String,
    pub alias: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelJoin {
    pub semantic_table: String,
    pub alias: String,
    pub to_table: String,
    pub join_type: JoinType,
    pub join_keys: Vec<JoinKey>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinKey {
    pub left: String,
    pub right: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QueryRequest {
    pub model: String,
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
pub struct Filter {
    pub field: String,
    pub op: FilterOp,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    In,
    NotIn,
    Like,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
