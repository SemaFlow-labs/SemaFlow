use crate::models::{Function, TimeGrain};

/// Dialects render identifiers and primitive expression pieces.
/// Expression tree walking lives in the query builder; the dialect
/// only maps logical constructs to SQL fragments.
pub trait Dialect {
    fn quote_ident(&self, ident: &str) -> String;
    fn placeholder(&self, _idx: usize) -> String {
        "?".to_string()
    }
    fn render_function(&self, func: &Function, args: Vec<String>) -> String;
    fn render_literal(&self, value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::Null => "NULL".to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            serde_json::Value::Array(items) => {
                let rendered: Vec<String> = items.iter().map(|v| self.render_literal(v)).collect();
                rendered.join(", ")
            }
            serde_json::Value::Object(_) => format!("'{}'", value.to_string().replace('\'', "''")),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct DuckDbDialect;

impl Dialect for DuckDbDialect {
    fn quote_ident(&self, ident: &str) -> String {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }

    fn render_function(&self, func: &Function, args: Vec<String>) -> String {
        match func {
            Function::DateTrunc(grain) => {
                let unit = match grain {
                    TimeGrain::Day => "day",
                    TimeGrain::Week => "week",
                    TimeGrain::Month => "month",
                    TimeGrain::Quarter => "quarter",
                    TimeGrain::Year => "year",
                };
                format!("date_trunc('{unit}', {})", args.join(", "))
            }
            Function::DatePart { field } => match args.as_slice() {
                [expr] => format!("date_part('{field}', {expr})"),
                _ => "NULL".to_string(),
            },
            Function::Lower => format!("lower({})", args.join(", ")),
            Function::Upper => format!("upper({})", args.join(", ")),
            Function::Coalesce => format!("coalesce({})", args.join(", ")),
            Function::IfNull => format!("ifnull({})", args.join(", ")),
            Function::Now => "now()".to_string(),
            Function::Concat => format!("concat({})", args.join(", ")),
            Function::ConcatWs { sep } => {
                let mut quoted = sep.replace('\'', "''");
                if quoted.is_empty() {
                    quoted = "".to_string();
                }
                format!("concat_ws('{quoted}', {})", args.join(", "))
            }
            Function::Substring => match args.as_slice() {
                [expr, start, len] => format!("substring({expr}, {start}, {len})"),
                [expr, start] => format!("substring({expr}, {start})"),
                _ => "NULL".to_string(),
            },
            Function::Length => format!("length({})", args.join(", ")),
            Function::Greatest => format!("greatest({})", args.join(", ")),
            Function::Least => format!("least({})", args.join(", ")),
            Function::Cast { data_type } => match args.as_slice() {
                [expr] => format!("CAST({expr} AS {data_type})"),
                _ => "NULL".to_string(),
            },
            Function::Trim => format!("trim({})", args.join(", ")),
            Function::Ltrim => format!("ltrim({})", args.join(", ")),
            Function::Rtrim => format!("rtrim({})", args.join(", ")),
        }
    }
}
