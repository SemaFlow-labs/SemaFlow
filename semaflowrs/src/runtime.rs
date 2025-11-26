use crate::data_sources::DataSourceRegistry;
use crate::error::Result;
use crate::query_builder::SqlBuilder;
use crate::registry::ModelRegistry;

pub async fn run_query(
    registry: &ModelRegistry,
    data_sources: &DataSourceRegistry,
    request: &crate::models::QueryRequest,
) -> Result<crate::executor::QueryResult> {
    let builder = SqlBuilder::default();
    let sql = builder.build_for_request(registry, data_sources, request)?;

    let model = registry.get_model(&request.model).ok_or_else(|| {
        crate::SemaflowError::Validation(format!("unknown model {}", request.model))
    })?;
    let base_table = registry
        .get_table(&model.base_table.semantic_table)
        .ok_or_else(|| {
            crate::SemaflowError::Validation(format!(
                "model base table {} not found",
                model.base_table.semantic_table
            ))
        })?;
    let ds = data_sources.get(&base_table.data_source).ok_or_else(|| {
        crate::SemaflowError::Validation(format!(
            "data source {} not registered",
            base_table.data_source
        ))
    })?;
    ds.executor.query(&sql).await
}
