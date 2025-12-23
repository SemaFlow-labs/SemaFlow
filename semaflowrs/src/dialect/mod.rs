//! SQL dialect abstractions for different database backends.
//!
//! Each dialect is implemented in its own file and gated behind a feature flag.

#[cfg(any(feature = "duckdb", feature = "postgres"))]
use crate::flows::TimeGrain;
use crate::flows::{Aggregation, Function};

/// Dialects render identifiers and primitive expression pieces.
/// Expression tree walking lives in the query builder; the dialect
/// only maps logical constructs to SQL fragments.
pub trait Dialect {
    fn quote_ident(&self, ident: &str) -> String;
    fn qualify_table(&self, table: &str) -> String {
        self.quote_ident(table).to_string()
    }
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
            serde_json::Value::Object(_) => {
                format!("'{}'", value.to_string().replace('\'', "''"))
            }
        }
    }
}

/// Convert TimeGrain to SQL interval string (shared by DuckDB and PostgreSQL).
#[cfg(any(feature = "duckdb", feature = "postgres"))]
pub(crate) fn grain_to_str(grain: &TimeGrain) -> &'static str {
    match grain {
        TimeGrain::Day => "day",
        TimeGrain::Week => "week",
        TimeGrain::Month => "month",
        TimeGrain::Quarter => "quarter",
        TimeGrain::Year => "year",
    }
}

// Feature-gated dialect implementations
#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "duckdb")]
pub use duckdb::DuckDbDialect;

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "postgres")]
pub use postgres::PostgresDialect;

#[cfg(feature = "bigquery")]
mod bigquery;
#[cfg(feature = "bigquery")]
pub use bigquery::BigQueryDialect;
