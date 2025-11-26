use thiserror::Error;

pub type Result<T> = std::result::Result<T, SemaflowError>;

#[derive(Debug, Error)]
pub enum SemaflowError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("yaml parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("validation error: {0}")]
    Validation(String),
    #[error("schema error: {0}")]
    Schema(String),
    #[error("sql generation error: {0}")]
    Sql(String),
    #[error("execution error: {0}")]
    Execution(String),
    #[error("duckdb error: {0}")]
    DuckDb(#[from] duckdb::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
