//! BigQuery dialect implementation.

use crate::flows::{Aggregation, Function, TimeGrain};

use super::Dialect;

#[derive(Debug, Default, Clone, Copy)]
pub struct BigQueryDialect;

impl Dialect for BigQueryDialect {
    fn quote_ident(&self, ident: &str) -> String {
        // BigQuery uses backticks for identifiers
        format!("`{}`", ident.replace('`', "\\`"))
    }

    fn placeholder(&self, idx: usize) -> String {
        // BigQuery uses @param0, @param1, etc. for named parameters
        format!("@p{}", idx)
    }

    fn supports_filtered_aggregates(&self) -> bool {
        false // BigQuery doesn't support FILTER (WHERE) syntax
    }

    fn render_aggregation(&self, agg: &Aggregation, expr: &str) -> String {
        match agg {
            // BigQuery has native APPROX_COUNT_DISTINCT
            Aggregation::ApproxCountDistinct => format!("APPROX_COUNT_DISTINCT({expr})"),
            // BigQuery MEDIAN requires PERCENTILE_CONT with OVER()
            Aggregation::Median => {
                format!("PERCENTILE_CONT({expr}, 0.5) OVER()")
            }
            // BigQuery STRING_AGG syntax
            Aggregation::StringAgg { separator } => {
                let escaped = separator.replace('\'', "\\'");
                format!("STRING_AGG({expr}, '{escaped}')")
            }
            // BigQuery uses ARRAY_AGG
            Aggregation::ArrayAgg => format!("ARRAY_AGG({expr})"),
            // BigQuery doesn't have FIRST/LAST natively - use ARRAY_AGG with OFFSET
            Aggregation::First => format!("ARRAY_AGG({expr} IGNORE NULLS)[OFFSET(0)]"),
            Aggregation::Last => format!(
                "ARRAY_AGG({expr} IGNORE NULLS)[ORDINAL(ARRAY_LENGTH(ARRAY_AGG({expr} IGNORE NULLS)))]"
            ),
            // Standard aggregations
            Aggregation::Sum => format!("SUM({expr})"),
            Aggregation::Count => format!("COUNT({expr})"),
            Aggregation::CountDistinct => format!("COUNT(DISTINCT {expr})"),
            Aggregation::Min => format!("MIN({expr})"),
            Aggregation::Max => format!("MAX({expr})"),
            Aggregation::Avg => format!("AVG({expr})"),
            Aggregation::Stddev => format!("STDDEV_POP({expr})"),
            Aggregation::StddevSamp => format!("STDDEV_SAMP({expr})"),
            Aggregation::Variance => format!("VAR_POP({expr})"),
            Aggregation::VarianceSamp => format!("VAR_SAMP({expr})"),
        }
    }

    fn render_function(&self, func: &Function, args: Vec<String>) -> String {
        match func {
            // === Date/Time Functions (BigQuery-specific) ===
            Function::DateTrunc(grain) => {
                let unit = bq_grain_to_str(grain);
                // BigQuery: TIMESTAMP_TRUNC(timestamp, MONTH)
                format!("TIMESTAMP_TRUNC({}, {})", args.join(", "), unit)
            }
            Function::DatePart { field } => match args.as_slice() {
                // BigQuery: EXTRACT(field FROM expr)
                [expr] => format!("EXTRACT({field} FROM {expr})"),
                _ => "NULL".to_string(),
            },
            Function::Now => "CURRENT_TIMESTAMP()".to_string(),
            Function::CurrentDate => "CURRENT_DATE()".to_string(),
            Function::CurrentTimestamp => "CURRENT_TIMESTAMP()".to_string(),
            Function::DateAdd { unit } => {
                let unit_str = bq_grain_to_str(unit);
                match args.as_slice() {
                    // BigQuery: DATE_ADD(date, INTERVAL n DAY)
                    [amount, date] => format!("DATE_ADD({date}, INTERVAL {amount} {unit_str})"),
                    _ => "NULL".to_string(),
                }
            }
            Function::DateDiff { unit } => {
                let unit_str = bq_grain_to_str(unit);
                match args.as_slice() {
                    // BigQuery: DATE_DIFF(end, start, DAY)
                    [start, end] => format!("DATE_DIFF({end}, {start}, {unit_str})"),
                    _ => "NULL".to_string(),
                }
            }
            Function::Extract { field } => match args.as_slice() {
                [expr] => format!("EXTRACT({field} FROM {expr})"),
                _ => "NULL".to_string(),
            },

            // === String Functions ===
            Function::Lower => format!("LOWER({})", args.join(", ")),
            Function::Upper => format!("UPPER({})", args.join(", ")),
            Function::Concat => format!("CONCAT({})", args.join(", ")),
            Function::ConcatWs { sep } => {
                // BigQuery doesn't have CONCAT_WS, use ARRAY_TO_STRING
                let quoted = sep.replace('\'', "\\'");
                format!("ARRAY_TO_STRING([{}], '{quoted}')", args.join(", "))
            }
            Function::Substring => match args.as_slice() {
                [expr, start, len] => format!("SUBSTR({expr}, {start}, {len})"),
                [expr, start] => format!("SUBSTR({expr}, {start})"),
                _ => "NULL".to_string(),
            },
            Function::Length => format!("LENGTH({})", args.join(", ")),
            Function::Trim => format!("TRIM({})", args.join(", ")),
            Function::Ltrim => format!("LTRIM({})", args.join(", ")),
            Function::Rtrim => format!("RTRIM({})", args.join(", ")),
            Function::Left => match args.as_slice() {
                [expr, n] => format!("LEFT({expr}, {n})"),
                _ => "NULL".to_string(),
            },
            Function::Right => match args.as_slice() {
                [expr, n] => format!("RIGHT({expr}, {n})"),
                _ => "NULL".to_string(),
            },
            Function::Replace => match args.as_slice() {
                [expr, from, to] => format!("REPLACE({expr}, {from}, {to})"),
                _ => "NULL".to_string(),
            },
            Function::Position => match args.as_slice() {
                // BigQuery: STRPOS(haystack, needle)
                [needle, haystack] => format!("STRPOS({haystack}, {needle})"),
                _ => "NULL".to_string(),
            },
            Function::Reverse => format!("REVERSE({})", args.join(", ")),
            Function::Repeat => match args.as_slice() {
                [expr, n] => format!("REPEAT({expr}, {n})"),
                _ => "NULL".to_string(),
            },
            Function::StartsWith => match args.as_slice() {
                [expr, prefix] => format!("STARTS_WITH({expr}, {prefix})"),
                _ => "NULL".to_string(),
            },
            Function::EndsWith => match args.as_slice() {
                [expr, suffix] => format!("ENDS_WITH({expr}, {suffix})"),
                _ => "NULL".to_string(),
            },
            Function::Contains => match args.as_slice() {
                // BigQuery: CONTAINS_SUBSTR or STRPOS > 0
                [expr, substr] => format!("STRPOS({expr}, {substr}) > 0"),
                _ => "NULL".to_string(),
            },

            // === Null Handling ===
            Function::Coalesce => format!("COALESCE({})", args.join(", ")),
            Function::IfNull => format!("IFNULL({})", args.join(", ")),
            Function::NullIf => match args.as_slice() {
                [expr1, expr2] => format!("NULLIF({expr1}, {expr2})"),
                _ => "NULL".to_string(),
            },

            // === Math Functions ===
            Function::Greatest => format!("GREATEST({})", args.join(", ")),
            Function::Least => format!("LEAST({})", args.join(", ")),
            Function::SafeDivide => match args.as_slice() {
                // BigQuery has native SAFE_DIVIDE
                [left, right] => format!("SAFE_DIVIDE({left}, {right})"),
                _ => "NULL".to_string(),
            },
            Function::Abs => format!("ABS({})", args.join(", ")),
            Function::Ceil => format!("CEIL({})", args.join(", ")),
            Function::Floor => format!("FLOOR({})", args.join(", ")),
            Function::Round => match args.as_slice() {
                [expr, decimals] => format!("ROUND({expr}, {decimals})"),
                [expr] => format!("ROUND({expr})"),
                _ => "NULL".to_string(),
            },
            Function::Power => match args.as_slice() {
                [base, exp] => format!("POWER({base}, {exp})"),
                _ => "NULL".to_string(),
            },
            Function::Sqrt => format!("SQRT({})", args.join(", ")),
            Function::Ln => format!("LN({})", args.join(", ")),
            Function::Log10 => format!("LOG10({})", args.join(", ")),
            Function::Log => match args.as_slice() {
                [base, value] => format!("LOG({value}, {base})"), // BigQuery: LOG(x, base)
                [value] => format!("LN({value})"),
                _ => "NULL".to_string(),
            },
            Function::Exp => format!("EXP({})", args.join(", ")),
            Function::Sign => format!("SIGN({})", args.join(", ")),

            // === Type Conversion ===
            Function::Cast { data_type } => match args.as_slice() {
                [expr] => format!("CAST({expr} AS {data_type})"),
                _ => "NULL".to_string(),
            },
            Function::TryCast { data_type } => match args.as_slice() {
                // BigQuery: SAFE_CAST
                [expr] => format!("SAFE_CAST({expr} AS {data_type})"),
                _ => "NULL".to_string(),
            },
        }
    }
}

/// Convert TimeGrain to BigQuery date part string.
fn bq_grain_to_str(grain: &TimeGrain) -> &'static str {
    match grain {
        TimeGrain::Day => "DAY",
        TimeGrain::Week => "WEEK",
        TimeGrain::Month => "MONTH",
        TimeGrain::Quarter => "QUARTER",
        TimeGrain::Year => "YEAR",
    }
}
