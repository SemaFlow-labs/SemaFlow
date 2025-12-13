//! PostgreSQL dialect implementation.

use crate::flows::{Aggregation, Function, TimeGrain};

use super::{grain_to_str, Dialect};

#[derive(Debug, Default, Clone, Copy)]
pub struct PostgresDialect;

impl Dialect for PostgresDialect {
    fn quote_ident(&self, ident: &str) -> String {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }

    fn placeholder(&self, idx: usize) -> String {
        format!("${}", idx + 1) // PostgreSQL uses $1, $2, ...
    }

    fn supports_filtered_aggregates(&self) -> bool {
        true // PostgreSQL 9.4+ supports FILTER
    }

    fn render_aggregation(&self, agg: &Aggregation, expr: &str) -> String {
        match agg {
            // PostgreSQL uses FIRST_VALUE/LAST_VALUE with window functions,
            // but for simple aggregates we can use (array_agg(x))[1]
            Aggregation::First => format!("(array_agg({expr}))[1]"),
            Aggregation::Last => {
                format!("(array_agg({expr}))[array_length(array_agg({expr}), 1)]")
            }
            // All others are standard SQL
            _ => {
                // Delegate to default implementation for standard aggregations
                match agg {
                    Aggregation::Sum => format!("SUM({expr})"),
                    Aggregation::Count => format!("COUNT({expr})"),
                    Aggregation::CountDistinct => format!("COUNT(DISTINCT {expr})"),
                    Aggregation::Min => format!("MIN({expr})"),
                    Aggregation::Max => format!("MAX({expr})"),
                    Aggregation::Avg => format!("AVG({expr})"),
                    Aggregation::Median => {
                        format!("PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {expr})")
                    }
                    Aggregation::Stddev => format!("STDDEV_POP({expr})"),
                    Aggregation::StddevSamp => format!("STDDEV_SAMP({expr})"),
                    Aggregation::Variance => format!("VAR_POP({expr})"),
                    Aggregation::VarianceSamp => format!("VAR_SAMP({expr})"),
                    Aggregation::StringAgg { separator } => {
                        let escaped = separator.replace('\'', "''");
                        format!("STRING_AGG({expr}, '{escaped}')")
                    }
                    Aggregation::ArrayAgg => format!("ARRAY_AGG({expr})"),
                    Aggregation::ApproxCountDistinct => format!("COUNT(DISTINCT {expr})"), // No native approx in PG
                    Aggregation::First | Aggregation::Last => unreachable!(),
                }
            }
        }
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
                let unit_str = pg_interval_unit(unit);
                match args.as_slice() {
                    // PostgreSQL: date + INTERVAL 'n unit'
                    [amount, date] => format!("{date} + ({amount} * INTERVAL '1 {unit_str}')"),
                    _ => "NULL".to_string(),
                }
            }
            Function::DateDiff { unit } => {
                let unit_str = grain_to_str(unit);
                match args.as_slice() {
                    // PostgreSQL: date_part('unit', end - start)
                    [start, end] => format!("date_part('{unit_str}', {end} - {start})"),
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
                [expr, start, len] => format!("substring({expr} FROM {start} FOR {len})"),
                [expr, start] => format!("substring({expr} FROM {start})"),
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
                // PostgreSQL doesn't have ends_with, use right() comparison
                [expr, suffix] => format!("right({expr}, length({suffix})) = {suffix}"),
                _ => "NULL".to_string(),
            },
            Function::Contains => match args.as_slice() {
                // PostgreSQL: use LIKE or position
                [expr, substr] => format!("position({substr} IN {expr}) > 0"),
                _ => "NULL".to_string(),
            },

            // === Null Handling ===
            Function::Coalesce => format!("coalesce({})", args.join(", ")),
            Function::IfNull => match args.as_slice() {
                [expr, default] => format!("coalesce({expr}, {default})"),
                _ => "NULL".to_string(),
            },
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
            Function::Log10 => format!("log({})", args.join(", ")), // PostgreSQL log() is base 10
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
                // PostgreSQL doesn't have TRY_CAST, use CASE with exception handling
                // For simplicity, just do a regular CAST (will error on invalid input)
                [expr] => format!("CAST({expr} AS {data_type})"),
                _ => "NULL".to_string(),
            },
        }
    }
}

/// Convert TimeGrain to PostgreSQL interval unit string.
fn pg_interval_unit(grain: &TimeGrain) -> &'static str {
    match grain {
        TimeGrain::Day => "day",
        TimeGrain::Week => "week",
        TimeGrain::Month => "month",
        TimeGrain::Quarter => "month", // Will multiply by 3
        TimeGrain::Year => "year",
    }
}
