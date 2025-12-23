//! BigQuery backend implementation using gcp-bigquery-client.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use gcp_bigquery_client::model::get_query_results_parameters::GetQueryResultsParameters;
use gcp_bigquery_client::model::get_query_results_response::GetQueryResultsResponse;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::query_response::ResultSet;
use gcp_bigquery_client::Client;
use tokio::sync::Semaphore;

use crate::config::BigQueryConfig;
use crate::dialect::BigQueryDialect;
use crate::error::{Result, SemaflowError};
use crate::executor::{ColumnMeta, PaginatedResult, QueryResult};
use crate::pagination::Cursor;
use crate::schema_cache::TableSchema;

use super::BackendConnection;

pub struct BigQueryConnection {
    client: Client,
    project_id: String,
    dataset: String,
    dialect: BigQueryDialect,
    config: BigQueryConfig,
    /// Semaphore to limit concurrent BigQuery queries for backpressure.
    limiter: Arc<Semaphore>,
}

impl BigQueryConnection {
    /// Create a new BigQuery connection from a service account key file.
    ///
    /// # Arguments
    /// * `service_account_path` - Path to the GCP service account JSON key file
    /// * `project_id` - GCP project ID
    /// * `dataset` - BigQuery dataset name
    pub async fn from_service_account_key_file(
        service_account_path: &str,
        project_id: &str,
        dataset: &str,
    ) -> Result<Self> {
        Self::from_service_account_key_file_with_config(
            service_account_path,
            project_id,
            dataset,
            BigQueryConfig::default(),
        )
        .await
    }

    /// Create a new BigQuery connection from a service account key file with config.
    pub async fn from_service_account_key_file_with_config(
        service_account_path: &str,
        project_id: &str,
        dataset: &str,
        config: BigQueryConfig,
    ) -> Result<Self> {
        tracing::info!(
            project_id = %project_id,
            dataset = %dataset,
            use_query_cache = config.use_query_cache,
            maximum_bytes_billed = config.maximum_bytes_billed,
            "creating BigQuery connection from service account"
        );

        let client = Client::from_service_account_key_file(service_account_path)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to create BigQuery client");
                SemaflowError::Execution(format!("create bigquery client: {e}"))
            })?;

        tracing::info!(
            project_id = %project_id,
            dataset = %dataset,
            max_concurrent = config.max_concurrent_queries,
            "BigQuery connection established"
        );

        Ok(Self {
            client,
            project_id: project_id.to_string(),
            dataset: dataset.to_string(),
            dialect: BigQueryDialect::new(project_id, dataset),
            limiter: Arc::new(Semaphore::new(config.max_concurrent_queries)),
            config,
        })
    }

    /// Create a new BigQuery connection from application default credentials.
    ///
    /// This uses GOOGLE_APPLICATION_CREDENTIALS environment variable or
    /// the default credentials from gcloud CLI.
    pub async fn from_application_default_credentials(
        project_id: &str,
        dataset: &str,
    ) -> Result<Self> {
        Self::from_application_default_credentials_with_config(
            project_id,
            dataset,
            BigQueryConfig::default(),
        )
        .await
    }

    /// Create a new BigQuery connection from ADC with config.
    pub async fn from_application_default_credentials_with_config(
        project_id: &str,
        dataset: &str,
        config: BigQueryConfig,
    ) -> Result<Self> {
        tracing::info!(
            project_id = %project_id,
            dataset = %dataset,
            use_query_cache = config.use_query_cache,
            maximum_bytes_billed = config.maximum_bytes_billed,
            "creating BigQuery connection from application default credentials"
        );

        let client = Client::from_application_default_credentials()
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to create BigQuery client from ADC");
                SemaflowError::Execution(format!("create bigquery client: {e}"))
            })?;

        tracing::info!(
            project_id = %project_id,
            dataset = %dataset,
            max_concurrent = config.max_concurrent_queries,
            "BigQuery connection established via ADC"
        );

        Ok(Self {
            client,
            project_id: project_id.to_string(),
            dataset: dataset.to_string(),
            dialect: BigQueryDialect::new(project_id, dataset),
            limiter: Arc::new(Semaphore::new(config.max_concurrent_queries)),
            config,
        })
    }

    /// Get the current BigQuery configuration.
    pub fn config(&self) -> &BigQueryConfig {
        &self.config
    }

    /// Get the project ID.
    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    /// Get the dataset name.
    pub fn dataset(&self) -> &str {
        &self.dataset
    }

    /// Acquire a slot for query execution with backpressure.
    ///
    /// If all slots are in use, waits up to `queue_timeout_ms` before rejecting.
    /// This prevents unbounded request queuing under load.
    async fn acquire_slot(&self) -> Result<tokio::sync::OwnedSemaphorePermit> {
        let available = self.limiter.available_permits();
        if available == 0 {
            tracing::debug!(
                max_concurrent = self.config.max_concurrent_queries,
                queue_timeout_ms = self.config.queue_timeout_ms,
                "BigQuery slots exhausted, waiting for permit"
            );
        }

        let timeout_ms = self.config.queue_timeout_ms;
        if timeout_ms == 0 {
            // No timeout - wait indefinitely (not recommended for production)
            self.limiter
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| SemaflowError::Execution(format!("limiter closed: {e}")))
        } else {
            // Wait with timeout for backpressure
            let timeout = Duration::from_millis(timeout_ms);
            match tokio::time::timeout(timeout, self.limiter.clone().acquire_owned()).await {
                Ok(Ok(permit)) => Ok(permit),
                Ok(Err(e)) => Err(SemaflowError::Execution(format!("limiter closed: {e}"))),
                Err(_) => {
                    tracing::warn!(
                        max_concurrent = self.config.max_concurrent_queries,
                        timeout_ms = timeout_ms,
                        "BigQuery request rejected: queue timeout exceeded"
                    );
                    Err(SemaflowError::Execution(format!(
                        "BigQuery overloaded: request queued for {}ms, max concurrent queries ({}) reached",
                        timeout_ms, self.config.max_concurrent_queries
                    )))
                }
            }
        }
    }

    /// Execute SQL query against BigQuery.
    ///
    /// Uses query() instead of query_all() to get schema and data from the same response,
    /// avoiding column ordering mismatches between separate API calls.
    async fn execute_query(&self, sql: &str) -> Result<QueryResult> {
        // Acquire slot with backpressure - rejects if queue timeout exceeded
        let _permit = self.acquire_slot().await?;

        let start = Instant::now();
        tracing::debug!(
            project = %self.project_id,
            sql_len = sql.len(),
            use_query_cache = self.config.use_query_cache,
            "executing BigQuery query"
        );
        tracing::trace!(sql = %sql, "BigQuery SQL");

        // Build query request with config options
        let mut query_request = QueryRequest::new(sql);
        query_request.use_query_cache = Some(self.config.use_query_cache);
        if self.config.maximum_bytes_billed > 0 {
            query_request.maximum_bytes_billed = Some(self.config.maximum_bytes_billed.to_string());
        }

        // Execute query - returns schema and data together, ensuring column order matches
        let response = self
            .client
            .job()
            .query(&self.project_id, query_request)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "BigQuery query execution failed");
                SemaflowError::Execution(format!("bigquery query: {e}"))
            })?;

        // Use ResultSet to handle schema and data together
        let mut rs = ResultSet::new_from_query_response(response);

        // IMPORTANT: column_names() returns HashMap keys in arbitrary order (Rust HashMap)
        // but get_json_value(idx) uses the original schema position.
        // We MUST use get_json_value_by_name() to ensure correct column-value mapping.
        let col_names: Vec<String> = rs.column_names().iter().map(|s| s.to_string()).collect();

        // Build column metadata
        let columns: Vec<ColumnMeta> = col_names
            .iter()
            .map(|name| ColumnMeta { name: name.clone() })
            .collect();

        // Convert rows to JSON maps - use get_json_value_by_name for correct mapping
        let mut result_rows = Vec::new();
        while rs.next_row() {
            let mut map = serde_json::Map::new();
            for col_name in &col_names {
                // Get value by name to avoid HashMap ordering issues
                let value = rs
                    .get_json_value_by_name(col_name)
                    .ok()
                    .flatten()
                    .unwrap_or(serde_json::Value::Null);
                map.insert(col_name.to_string(), value);
            }
            result_rows.push(map);
        }

        let elapsed = start.elapsed();
        tracing::debug!(
            rows = result_rows.len(),
            columns = columns.len(),
            ms = elapsed.as_millis(),
            "bigquery execute_sql"
        );

        Ok(QueryResult {
            columns,
            rows: result_rows,
        })
    }
}

#[async_trait]
impl BackendConnection for BigQueryConnection {
    fn dialect(&self) -> &(dyn crate::dialect::Dialect + Send + Sync) {
        &self.dialect
    }

    async fn fetch_schema(&self, table: &str) -> Result<TableSchema> {
        let start = Instant::now();
        tracing::debug!(
            project = %self.project_id,
            dataset = %self.dataset,
            table = %table,
            "fetching BigQuery table schema"
        );

        let table_info = self
            .client
            .table()
            .get(&self.project_id, &self.dataset, table, None)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, table = %table, "failed to get BigQuery table info");
                SemaflowError::Execution(format!("fetch bigquery table: {e}"))
            })?;

        let mut columns = Vec::new();
        if let Some(fields) = &table_info.schema.fields {
            for field in fields {
                columns.push(crate::schema_cache::ColumnSchema {
                    name: field.name.clone(),
                    data_type: format!("{:?}", field.r#type),
                    nullable: field.mode.as_ref().is_none_or(|m| m != "REQUIRED"),
                });
            }
        }

        // BigQuery doesn't expose PK/FK through this API - semantic layer defines these
        let primary_keys = Vec::new();
        let foreign_keys = Vec::new();

        let elapsed = start.elapsed();
        tracing::debug!(
            table = table,
            columns = columns.len(),
            ms = elapsed.as_millis(),
            "bigquery fetch_schema"
        );

        Ok(TableSchema {
            columns,
            primary_keys,
            foreign_keys,
        })
    }

    async fn execute_sql(&self, sql: &str) -> Result<QueryResult> {
        self.execute_query(sql).await
    }

    async fn execute_sql_paginated(
        &self,
        sql: &str,
        page_size: u32,
        cursor: Option<&Cursor>,
        query_hash: u64,
    ) -> Result<PaginatedResult> {
        // Acquire slot with backpressure - rejects if queue timeout exceeded
        let _permit = self.acquire_slot().await?;

        let start = Instant::now();

        // Handle subsequent pages (from cursor) vs first page differently
        // because they return different response types
        match cursor {
            Some(Cursor::BigQuery {
                job_id,
                page_token,
                query_hash: cursor_hash,
                offset,
            }) => {
                // Validate cursor matches this query
                if *cursor_hash != query_hash {
                    return Err(SemaflowError::Validation(
                        "cursor does not match current query".to_string(),
                    ));
                }

                tracing::debug!(
                    job_id = %job_id,
                    offset = offset,
                    "fetching subsequent BigQuery page using cached job"
                );

                // Fetch subsequent page from cached job
                let params = GetQueryResultsParameters {
                    page_token: Some(page_token.clone()),
                    max_results: Some(page_size as i32),
                    ..Default::default()
                };

                let response = self.client
                    .job()
                    .get_query_results(&self.project_id, job_id, params)
                    .await
                    .map_err(|e| {
                        // TODO: Detect expired job and re-run query with start_index
                        tracing::error!(error = %e, job_id = %job_id, "failed to fetch BigQuery results page");
                        SemaflowError::Execution(format!("bigquery get_query_results: {e}"))
                    })?;

                self.process_get_query_results_response(response, *offset, query_hash, start)
            }
            Some(Cursor::Sql { .. }) => Err(SemaflowError::Validation(
                "SQL cursor provided to BigQuery backend".to_string(),
            )),
            None => {
                // First page - execute query with max_results
                tracing::debug!(
                    page_size = page_size,
                    "executing initial BigQuery paginated query"
                );

                let mut query_request = QueryRequest::new(sql);
                query_request.max_results = Some(page_size as i32);
                query_request.use_query_cache = Some(self.config.use_query_cache);
                if self.config.maximum_bytes_billed > 0 {
                    query_request.maximum_bytes_billed =
                        Some(self.config.maximum_bytes_billed.to_string());
                }

                let response = self
                    .client
                    .job()
                    .query(&self.project_id, query_request)
                    .await
                    .map_err(|e| {
                        tracing::error!(error = %e, "BigQuery paginated query failed");
                        SemaflowError::Execution(format!("bigquery query: {e}"))
                    })?;

                self.process_query_response_paginated(response, query_hash, start)
            }
        }
    }
}

impl BigQueryConnection {
    /// Process QueryResponse (first page) for paginated results.
    fn process_query_response_paginated(
        &self,
        response: gcp_bigquery_client::model::query_response::QueryResponse,
        query_hash: u64,
        start: Instant,
    ) -> Result<PaginatedResult> {
        // Extract job_id for cursor
        let job_id = response
            .job_reference
            .as_ref()
            .and_then(|jr| jr.job_id.clone())
            .ok_or_else(|| {
                SemaflowError::Execution("BigQuery response missing job_id".to_string())
            })?;

        // Extract total_rows and page_token
        let total_rows = response
            .total_rows
            .as_ref()
            .and_then(|s| s.parse::<u64>().ok());
        let page_token = response.page_token.clone();

        // Convert response to rows using ResultSet
        // IMPORTANT: Use get_json_value_by_name to avoid HashMap ordering issues
        let mut rs = ResultSet::new_from_query_response(response);
        let col_names: Vec<String> = rs.column_names().iter().map(|s| s.to_string()).collect();

        let columns: Vec<ColumnMeta> = col_names
            .iter()
            .map(|name| ColumnMeta { name: name.clone() })
            .collect();

        let mut rows = Vec::new();
        while rs.next_row() {
            let mut map = serde_json::Map::new();
            for col_name in &col_names {
                let value = rs
                    .get_json_value_by_name(col_name)
                    .ok()
                    .flatten()
                    .unwrap_or(serde_json::Value::Null);
                map.insert(col_name.to_string(), value);
            }
            rows.push(map);
        }

        // Build next cursor if page_token exists (more pages available)
        let has_more = page_token.is_some();
        let next_cursor = if let Some(token) = page_token {
            let next_offset = rows.len() as u64;
            let cursor = Cursor::bigquery(job_id, token, query_hash, next_offset);
            Some(cursor.encode()?)
        } else {
            None
        };

        let elapsed = start.elapsed();
        tracing::debug!(
            rows = rows.len(),
            has_more = has_more,
            total_rows = ?total_rows,
            ms = elapsed.as_millis(),
            "bigquery execute_sql_paginated (first page)"
        );

        Ok(PaginatedResult {
            columns,
            rows,
            cursor: next_cursor,
            has_more,
            total_rows,
        })
    }

    /// Process GetQueryResultsResponse (subsequent pages) for paginated results.
    fn process_get_query_results_response(
        &self,
        response: GetQueryResultsResponse,
        offset: u64,
        query_hash: u64,
        start: Instant,
    ) -> Result<PaginatedResult> {
        // Extract job_id for cursor
        let job_id = response
            .job_reference
            .as_ref()
            .and_then(|jr| jr.job_id.clone())
            .ok_or_else(|| {
                SemaflowError::Execution("BigQuery response missing job_id".to_string())
            })?;

        // Extract total_rows and page_token
        let total_rows = response
            .total_rows
            .as_ref()
            .and_then(|s| s.parse::<u64>().ok());
        let page_token = response.page_token.clone();

        // Get schema columns
        let col_names: Vec<String> = response
            .schema
            .as_ref()
            .and_then(|s| s.fields.as_ref())
            .map(|fields| fields.iter().map(|f| f.name.clone()).collect())
            .unwrap_or_default();

        let columns: Vec<ColumnMeta> = col_names
            .iter()
            .map(|name| ColumnMeta { name: name.clone() })
            .collect();

        // Convert rows
        let mut rows = Vec::new();
        if let Some(table_rows) = &response.rows {
            for table_row in table_rows {
                let mut map = serde_json::Map::new();
                if let Some(cells) = &table_row.columns {
                    for (col_idx, cell) in cells.iter().enumerate() {
                        let col_name = col_names
                            .get(col_idx)
                            .cloned()
                            .unwrap_or_else(|| format!("col_{col_idx}"));
                        let value = cell.value.clone().unwrap_or(serde_json::Value::Null);
                        map.insert(col_name, value);
                    }
                }
                rows.push(map);
            }
        }

        // Build next cursor if page_token exists (more pages available)
        let has_more = page_token.is_some();
        let next_cursor = if let Some(token) = page_token {
            let next_offset = offset + rows.len() as u64;
            let cursor = Cursor::bigquery(job_id, token, query_hash, next_offset);
            Some(cursor.encode()?)
        } else {
            None
        };

        let elapsed = start.elapsed();
        tracing::debug!(
            rows = rows.len(),
            has_more = has_more,
            total_rows = ?total_rows,
            ms = elapsed.as_millis(),
            "bigquery execute_sql_paginated (subsequent page)"
        );

        Ok(PaginatedResult {
            columns,
            rows,
            cursor: next_cursor,
            has_more,
            total_rows,
        })
    }
}
