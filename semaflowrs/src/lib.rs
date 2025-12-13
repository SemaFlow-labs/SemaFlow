pub mod backends;
pub mod dialect;
pub mod error;
pub mod executor;
pub mod expr_parser;
pub mod expr_utils;
pub mod flows;
#[cfg(feature = "python")]
pub mod python;
pub mod query_builder;
pub mod registry;
pub mod runtime;
pub mod schema_cache;
pub mod sql_ast;
pub mod validation;

use std::path::Path;

use crate::error::Result;
use crate::registry::FlowRegistry;

/// Load semantic definitions from disk and validate them with the provided validator.
pub async fn load_and_validate<P: AsRef<Path>>(
    flow_dir: P,
    validator: &crate::validation::Validator,
) -> Result<FlowRegistry> {
    let mut registry = FlowRegistry::load_from_dir(flow_dir)?;
    validator.validate_registry(&mut registry).await?;
    Ok(registry)
}

pub use crate::validation::Validator;
pub use backends::{BackendConnection, ConnectionManager};
#[cfg(feature = "duckdb")]
pub use backends::DuckDbConnection;
#[cfg(feature = "postgres")]
pub use backends::PostgresConnection;
#[cfg(feature = "bigquery")]
pub use backends::BigQueryConnection;
pub use error::SemaflowError;
pub use executor::QueryResult;
pub use flows::{QueryRequest, SemanticFlow, SemanticTable};
pub use query_builder::SqlBuilder;
pub use registry::{DimensionInfo, FlowSchema, FlowSummary, MeasureInfo};
pub use schema_cache::TableSchema;

// Dialect re-exports
pub use dialect::Dialect;
#[cfg(feature = "duckdb")]
pub use dialect::DuckDbDialect;
#[cfg(feature = "postgres")]
pub use dialect::PostgresDialect;
#[cfg(feature = "bigquery")]
pub use dialect::BigQueryDialect;
