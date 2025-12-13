use crate::backends::ConnectionManager;
use crate::error::Result;
use crate::query_builder::SqlBuilder;
use crate::registry::FlowRegistry;

#[tracing::instrument(
    skip(registry, connections),
    fields(
        flow = %request.flow,
        dimensions = ?request.dimensions,
        measures = ?request.measures,
    )
)]
pub async fn run_query(
    registry: &FlowRegistry,
    connections: &ConnectionManager,
    request: &crate::flows::QueryRequest,
) -> Result<crate::executor::QueryResult> {
    let start = std::time::Instant::now();
    tracing::debug!("starting query execution");

    let builder = SqlBuilder::default();
    let sql = builder.build_for_request(registry, connections, request)?;
    tracing::debug!(sql_len = sql.len(), "SQL generated");
    tracing::trace!(sql = %sql, "generated SQL");

    let flow = registry.get_flow(&request.flow).ok_or_else(|| {
        tracing::warn!(flow = %request.flow, "unknown flow requested");
        crate::SemaflowError::Validation(format!("unknown flow {}", request.flow))
    })?;
    let base_table = registry
        .get_table(&flow.base_table.semantic_table)
        .ok_or_else(|| {
            tracing::warn!(table = %flow.base_table.semantic_table, "base table not found");
            crate::SemaflowError::Validation(format!(
                "flow base table {} not found",
                flow.base_table.semantic_table
            ))
        })?;
    let ds = connections.get(&base_table.data_source).ok_or_else(|| {
        tracing::warn!(data_source = %base_table.data_source, "data source not registered");
        crate::SemaflowError::Validation(format!(
            "data source {} not registered",
            base_table.data_source
        ))
    })?;

    tracing::debug!(data_source = %base_table.data_source, "executing SQL");
    let result = ds.execute_sql(&sql).await;

    let elapsed = start.elapsed();
    match &result {
        Ok(r) => tracing::info!(
            flow = %request.flow,
            rows = r.rows.len(),
            ms = elapsed.as_millis(),
            "query completed successfully"
        ),
        Err(e) => tracing::error!(
            flow = %request.flow,
            error = %e,
            ms = elapsed.as_millis(),
            "query failed"
        ),
    }

    result
}
