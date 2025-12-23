use crate::backends::ConnectionManager;
use crate::error::Result;
use crate::executor::PaginatedResult;
use crate::pagination::{compute_query_hash, Cursor};
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

    let builder = SqlBuilder;
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

/// Execute a paginated query against a semantic flow.
///
/// This function handles cursor-based pagination by:
/// 1. Building SQL without LIMIT/OFFSET (the backend adds those or uses native pagination)
/// 2. Computing a query hash for cursor validation
/// 3. Decoding the cursor if present
/// 4. Calling the backend's paginated execution method
///
/// Returns a `PaginatedResult` with the current page and cursor for the next page.
#[tracing::instrument(
    skip(registry, connections),
    fields(
        flow = %request.flow,
        page_size = ?request.page_size,
        has_cursor = request.cursor.is_some(),
    )
)]
pub async fn run_query_paginated(
    registry: &FlowRegistry,
    connections: &ConnectionManager,
    request: &crate::flows::QueryRequest,
) -> Result<PaginatedResult> {
    let start = std::time::Instant::now();
    tracing::debug!("starting paginated query execution");

    // Require page_size for pagination
    let page_size = request.page_size.ok_or_else(|| {
        crate::SemaflowError::Validation("page_size is required for paginated queries".to_string())
    })?;

    // Build SQL without limit/offset - the backend handles pagination via LIMIT/OFFSET
    // The request.limit is a total cap that should be enforced separately (future enhancement)
    let sql_request = crate::flows::QueryRequest {
        flow: request.flow.clone(),
        dimensions: request.dimensions.clone(),
        measures: request.measures.clone(),
        filters: request.filters.clone(),
        order: request.order.clone(),
        limit: None,     // Don't include limit - backend adds LIMIT/OFFSET for pagination
        offset: None,    // Don't pass offset - cursor handles this
        page_size: None, // Don't include pagination in SQL
        cursor: None,
    };

    let builder = SqlBuilder;
    let sql = builder.build_for_request(registry, connections, &sql_request)?;
    tracing::debug!(sql_len = sql.len(), "SQL generated for pagination");
    tracing::trace!(sql = %sql, "generated SQL");

    // Compute query hash for cursor validation
    let query_hash = compute_query_hash(request);
    tracing::trace!(query_hash = query_hash, "computed query hash");

    // Decode cursor if present
    let cursor = match &request.cursor {
        Some(encoded) => {
            let decoded = Cursor::decode(encoded)?;
            decoded.validate_query_hash(query_hash)?;
            Some(decoded)
        }
        None => None,
    };

    // Get the backend connection
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

    tracing::debug!(
        data_source = %base_table.data_source,
        page_size = page_size,
        "executing paginated SQL"
    );

    // Execute paginated query
    let result = ds
        .execute_sql_paginated(&sql, page_size, cursor.as_ref(), query_hash)
        .await;

    let elapsed = start.elapsed();
    match &result {
        Ok(r) => tracing::info!(
            flow = %request.flow,
            rows = r.rows.len(),
            has_more = r.has_more,
            total_rows = ?r.total_rows,
            ms = elapsed.as_millis(),
            "paginated query completed successfully"
        ),
        Err(e) => tracing::error!(
            flow = %request.flow,
            error = %e,
            ms = elapsed.as_millis(),
            "paginated query failed"
        ),
    }

    result
}
