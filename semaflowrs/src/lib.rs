pub mod data_sources;
pub mod dialect;
pub mod error;
pub mod executor;
pub mod models;
#[cfg(feature = "python")]
pub mod python;
pub mod query_builder;
pub mod registry;
pub mod runtime;
pub mod schema_cache;
pub mod validation;

use std::path::Path;

use crate::error::Result;
use crate::registry::ModelRegistry;

/// Load semantic definitions from disk and validate them with the provided validator.
pub async fn load_and_validate<P: AsRef<Path>>(
    model_dir: P,
    validator: &crate::validation::Validator,
) -> Result<ModelRegistry> {
    let mut registry = ModelRegistry::load_from_dir(model_dir)?;
    validator.validate_registry(&mut registry).await?;
    Ok(registry)
}

pub use crate::validation::Validator;
pub use data_sources::{DataSource, DataSourceRegistry};
pub use error::SemaflowError;
pub use executor::{DuckDbExecutor, QueryExecutor, QueryResult, SchemaProvider};
pub use models::{QueryRequest, SemanticModel, SemanticTable};
pub use query_builder::SqlBuilder;
pub use schema_cache::TableSchema;
