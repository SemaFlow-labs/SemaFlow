use crate::flows::{Aggregation, Function, TimeGrain};

/// Dialects render identifiers and primitive expression pieces.
/// Expression tree walking lives in the query builder; the dialect
/// only maps logical constructs to SQL fragments.
pub trait Dialect {
    fn quote_ident(&self, ident: &str) -> String;
    fn placeholder(&self, _idx: usize) -> String {
        "?".to_string()
    }
    fn supports_filtered_aggregates(&self) -> bool {
        false
    }
    fn render_function(&self, func: &Function, args: Vec<String>) -> String;
    fn render_aggregation(&self, agg: &Aggregation, expr: &str) -> String {
        match agg {
            // Basic aggregations
            Aggregation::Sum => format!("SUM({expr})"),
            Aggregation::Count => format!("COUNT({expr})"),
            Aggregation::CountDistinct => format!("COUNT(DISTINCT {expr})"),
            Aggregation::Min => format!("MIN({expr})"),
            Aggregation::Max => format!("MAX({expr})"),
            Aggregation::Avg => format!("AVG({expr})"),
            // Statistical aggregations
            Aggregation::Median => format!("MEDIAN({expr})"),
            Aggregation::Stddev => format!("STDDEV_POP({expr})"),
            Aggregation::StddevSamp => format!("STDDEV_SAMP({expr})"),
            Aggregation::Variance => format!("VAR_POP({expr})"),
            Aggregation::VarianceSamp => format!("VAR_SAMP({expr})"),
            // List/String aggregations
            Aggregation::StringAgg { separator } => {
                let escaped = separator.replace('\'', "''");
                format!("STRING_AGG({expr}, '{escaped}')")
            }
            Aggregation::ArrayAgg => format!("ARRAY_AGG({expr})"),
            // Approximate aggregations
            Aggregation::ApproxCountDistinct => format!("APPROX_COUNT_DISTINCT({expr})"),
            // First/Last
            Aggregation::First => format!("FIRST({expr})"),
            Aggregation::Last => format!("LAST({expr})"),
        }
    }
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

    fn supports_filtered_aggregates(&self) -> bool {
        true
    }

    fn render_function(&self, func: &Function, args: Vec<String>) -> String {
        match func {
            // === Date/Time Functions ===
            Function::DateTrunc(grain) => {
                let unit = grain_to_str(grain);
                format!("date_trunc('{unit}', {})", args.join(", "))
            }
            Function::DatePart { field } => match args.as_slice() {
                [expr] => format!("date_part('{field}', {expr})"),
                _ => "NULL".to_string(),
            },
            Function::Now => "now()".to_string(),
            Function::CurrentDate => "current_date".to_string(),
            Function::CurrentTimestamp => "current_timestamp".to_string(),
            Function::DateAdd { unit } => {
                let unit_str = grain_to_str(unit);
                match args.as_slice() {
                    [amount, date] => format!("{date} + INTERVAL ({amount}) {unit_str}"),
                    _ => "NULL".to_string(),
                }
            }
            Function::DateDiff { unit } => {
                let unit_str = grain_to_str(unit);
                match args.as_slice() {
                    [start, end] => format!("date_diff('{unit_str}', {start}, {end})"),
                    _ => "NULL".to_string(),
                }
            }
            Function::Extract { field } => match args.as_slice() {
                [expr] => format!("extract({field} FROM {expr})"),
                _ => "NULL".to_string(),
            },

            // === String Functions ===
            Function::Lower => format!("lower({})", args.join(", ")),
            Function::Upper => format!("upper({})", args.join(", ")),
            Function::Concat => format!("concat({})", args.join(", ")),
            Function::ConcatWs { sep } => {
                let quoted = sep.replace('\'', "''");
                format!("concat_ws('{quoted}', {})", args.join(", "))
            }
            Function::Substring => match args.as_slice() {
                [expr, start, len] => format!("substring({expr}, {start}, {len})"),
                [expr, start] => format!("substring({expr}, {start})"),
                _ => "NULL".to_string(),
            },
            Function::Length => format!("length({})", args.join(", ")),
            Function::Trim => format!("trim({})", args.join(", ")),
            Function::Ltrim => format!("ltrim({})", args.join(", ")),
            Function::Rtrim => format!("rtrim({})", args.join(", ")),
            Function::Left => match args.as_slice() {
                [expr, n] => format!("left({expr}, {n})"),
                _ => "NULL".to_string(),
            },
            Function::Right => match args.as_slice() {
                [expr, n] => format!("right({expr}, {n})"),
                _ => "NULL".to_string(),
            },
            Function::Replace => match args.as_slice() {
                [expr, from, to] => format!("replace({expr}, {from}, {to})"),
                _ => "NULL".to_string(),
            },
            Function::Position => match args.as_slice() {
                [needle, haystack] => format!("position({needle} IN {haystack})"),
                _ => "NULL".to_string(),
            },
            Function::Reverse => format!("reverse({})", args.join(", ")),
            Function::Repeat => match args.as_slice() {
                [expr, n] => format!("repeat({expr}, {n})"),
                _ => "NULL".to_string(),
            },
            Function::StartsWith => match args.as_slice() {
                [expr, prefix] => format!("starts_with({expr}, {prefix})"),
                _ => "NULL".to_string(),
            },
            Function::EndsWith => match args.as_slice() {
                [expr, suffix] => format!("ends_with({expr}, {suffix})"),
                _ => "NULL".to_string(),
            },
            Function::Contains => match args.as_slice() {
                [expr, substr] => format!("contains({expr}, {substr})"),
                _ => "NULL".to_string(),
            },

            // === Null Handling ===
            Function::Coalesce => format!("coalesce({})", args.join(", ")),
            Function::IfNull => format!("ifnull({})", args.join(", ")),
            Function::NullIf => match args.as_slice() {
                [expr1, expr2] => format!("nullif({expr1}, {expr2})"),
                _ => "NULL".to_string(),
            },

            // === Math Functions ===
            Function::Greatest => format!("greatest({})", args.join(", ")),
            Function::Least => format!("least({})", args.join(", ")),
            Function::SafeDivide => match args.as_slice() {
                [left, right] => format!("{left} / NULLIF({right}, 0)"),
                _ => "NULL".to_string(),
            },
            Function::Abs => format!("abs({})", args.join(", ")),
            Function::Ceil => format!("ceil({})", args.join(", ")),
            Function::Floor => format!("floor({})", args.join(", ")),
            Function::Round => match args.as_slice() {
                [expr, decimals] => format!("round({expr}, {decimals})"),
                [expr] => format!("round({expr})"),
                _ => "NULL".to_string(),
            },
            Function::Power => match args.as_slice() {
                [base, exp] => format!("power({base}, {exp})"),
                _ => "NULL".to_string(),
            },
            Function::Sqrt => format!("sqrt({})", args.join(", ")),
            Function::Ln => format!("ln({})", args.join(", ")),
            Function::Log10 => format!("log10({})", args.join(", ")),
            Function::Log => match args.as_slice() {
                [base, value] => format!("log({base}, {value})"),
                [value] => format!("ln({value})"),
                _ => "NULL".to_string(),
            },
            Function::Exp => format!("exp({})", args.join(", ")),
            Function::Sign => format!("sign({})", args.join(", ")),

            // === Type Conversion ===
            Function::Cast { data_type } => match args.as_slice() {
                [expr] => format!("CAST({expr} AS {data_type})"),
                _ => "NULL".to_string(),
            },
            Function::TryCast { data_type } => match args.as_slice() {
                [expr] => format!("TRY_CAST({expr} AS {data_type})"),
                _ => "NULL".to_string(),
            },
        }
    }
}

/// Convert TimeGrain to SQL interval string.
fn grain_to_str(grain: &TimeGrain) -> &'static str {
    match grain {
        TimeGrain::Day => "day",
        TimeGrain::Week => "week",
        TimeGrain::Month => "month",
        TimeGrain::Quarter => "quarter",
        TimeGrain::Year => "year",
    }
}
