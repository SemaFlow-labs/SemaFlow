use std::collections::BTreeMap;

use crate::expr_parser::parse_expr;
use serde::{de, Deserializer};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize)]
pub struct SemanticTable {
    pub data_source: String,
    pub name: String,
    pub table: String,
    /// Primary key columns. Supports composite keys.
    pub primary_keys: Vec<String>,
    pub time_dimension: Option<String>,
    pub smallest_time_grain: Option<TimeGrain>,
    pub dimensions: BTreeMap<String, Dimension>,
    pub measures: BTreeMap<String, Measure>,
    pub description: Option<String>,
}

impl<'de> Deserialize<'de> for SemanticTable {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            data_source: String,
            name: String,
            table: String,
            // Support both single key and composite keys
            #[serde(default)]
            primary_key: Option<String>,
            #[serde(default)]
            primary_keys: Option<Vec<String>>,
            time_dimension: Option<String>,
            smallest_time_grain: Option<TimeGrain>,
            #[serde(default)]
            dimensions: BTreeMap<String, Dimension>,
            #[serde(default)]
            measures: BTreeMap<String, Measure>,
            description: Option<String>,
        }

        let raw = Raw::deserialize(deserializer)?;

        // Resolve primary keys: prefer primary_keys, fall back to primary_key
        let primary_keys = match (raw.primary_keys, raw.primary_key) {
            (Some(keys), _) => keys,
            (None, Some(key)) => vec![key],
            (None, None) => {
                return Err(de::Error::custom(
                    "either primary_key or primary_keys must be specified",
                ))
            }
        };

        Ok(SemanticTable {
            data_source: raw.data_source,
            name: raw.name,
            table: raw.table,
            primary_keys,
            time_dimension: raw.time_dimension,
            smallest_time_grain: raw.smallest_time_grain,
            dimensions: raw.dimensions,
            measures: raw.measures,
            description: raw.description,
        })
    }
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
    // === Date/Time Functions ===
    DateTrunc(TimeGrain),
    DatePart { field: String },
    Now,
    CurrentDate,
    CurrentTimestamp,
    /// Add interval to date: DateAdd(unit, amount, date)
    DateAdd { unit: TimeGrain },
    /// Difference between dates: DateDiff(unit, start, end)
    DateDiff { unit: TimeGrain },
    /// Extract part from date (alternative to DatePart)
    Extract { field: String },

    // === String Functions ===
    Lower,
    Upper,
    Concat,
    ConcatWs { sep: String },
    Substring,
    Length,
    Trim,
    Ltrim,
    Rtrim,
    /// Left N characters
    Left,
    /// Right N characters
    Right,
    /// Replace occurrences: Replace(str, from, to)
    Replace,
    /// Position of substring: Position(needle, haystack)
    Position,
    /// Reverse string
    Reverse,
    /// Repeat string N times
    Repeat,
    /// Check if string starts with prefix
    StartsWith,
    /// Check if string ends with suffix
    EndsWith,
    /// Check if string contains substring
    Contains,

    // === Null Handling ===
    Coalesce,
    IfNull,
    /// Returns NULL if two expressions are equal
    NullIf,

    // === Math Functions ===
    Greatest,
    Least,
    SafeDivide,
    /// Absolute value
    Abs,
    /// Ceiling (round up)
    Ceil,
    /// Floor (round down)
    Floor,
    /// Round to N decimal places
    Round,
    /// Power: base^exponent
    Power,
    /// Square root
    Sqrt,
    /// Natural logarithm
    Ln,
    /// Logarithm base 10
    Log10,
    /// Logarithm with custom base
    Log,
    /// Exponential (e^x)
    Exp,
    /// Sign (-1, 0, 1)
    Sign,

    // === Type Conversion ===
    Cast { data_type: String },
    /// Try cast, returns NULL on failure
    TryCast { data_type: String },
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
    // === Basic Aggregations ===
    Sum,
    Count,
    CountDistinct,
    Min,
    Max,
    Avg,

    // === Statistical Aggregations ===
    /// Median (50th percentile)
    Median,
    /// Standard deviation (population)
    Stddev,
    /// Standard deviation (sample)
    StddevSamp,
    /// Variance (population)
    Variance,
    /// Variance (sample)
    VarianceSamp,

    // === List/String Aggregations ===
    /// Concatenate strings with separator
    StringAgg { separator: String },
    /// Collect values into array
    ArrayAgg,

    // === Approximate Aggregations ===
    /// Approximate count distinct using HyperLogLog
    ApproxCountDistinct,

    // === First/Last (requires ORDER BY in aggregate) ===
    /// First value in group
    First,
    /// Last value in group
    Last,
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
    /// Optional cardinality hint. If not provided, inferred from primary keys.
    /// Use this when the system can't correctly infer the relationship.
    #[serde(default)]
    pub cardinality: Option<JoinCardinality>,
    pub description: Option<String>,
}

/// Cardinality of a join relationship (user-specified hint).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JoinCardinality {
    /// Many rows on the left map to one row on the right (N:1) - safe for aggregation
    ManyToOne,
    /// One row on the left maps to many rows on the right (1:N) - fanout risk
    OneToMany,
    /// Exactly one row on each side (1:1) - safe for aggregation
    OneToOne,
    /// Many rows on both sides (M:N) - fanout risk
    ManyToMany,
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
