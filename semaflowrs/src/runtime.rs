use crate::data_sources::ConnectionManager;
use crate::error::Result;
use crate::query_builder::SqlBuilder;
use crate::registry::FlowRegistry;

pub async fn run_query(
    registry: &FlowRegistry,
    connections: &ConnectionManager,
    request: &crate::flows::QueryRequest,
) -> Result<crate::executor::QueryResult> {
    let builder = SqlBuilder::default();
    let sql = builder.build_for_request(registry, connections, request)?;

    let flow = registry.get_flow(&request.flow).ok_or_else(|| {
        crate::SemaflowError::Validation(format!("unknown flow {}", request.flow))
    })?;
    let base_table = registry
        .get_table(&flow.base_table.semantic_table)
        .ok_or_else(|| {
            crate::SemaflowError::Validation(format!(
                "flow base table {} not found",
                flow.base_table.semantic_table
            ))
        })?;
    let ds = connections.get(&base_table.data_source).ok_or_else(|| {
        crate::SemaflowError::Validation(format!(
            "data source {} not registered",
            base_table.data_source
        ))
    })?;
    ds.execute_sql(&sql).await
}
