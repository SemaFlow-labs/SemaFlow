//! BigQuery backend implementation.

use std::time::Instant;

use async_trait::async_trait;

use crate::dialect::BigQueryDialect;
use crate::error::{Result, SemaflowError};
use crate::executor::{ColumnMeta, QueryResult};
use crate::schema_cache::TableSchema;

use super::BackendConnection;

pub struct BigQueryConnection {
    client: gcp_bigquery_client::Client,
    project_id: String,
    dataset: String,
    dialect: BigQueryDialect,
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
        tracing::info!(
            project_id = %project_id,
            dataset = %dataset,
            "creating BigQuery connection from service account"
        );

        let client =
            gcp_bigquery_client::Client::from_service_account_key_file(service_account_path)
                .await
                .map_err(|e| {
                    tracing::error!(error = %e, "failed to create BigQuery client");
                    SemaflowError::Execution(format!("create bigquery client: {e}"))
                })?;

        tracing::info!(
            project_id = %project_id,
            dataset = %dataset,
            "BigQuery connection established"
        );

        Ok(Self {
            client,
            project_id: project_id.to_string(),
            dataset: dataset.to_string(),
            dialect: BigQueryDialect,
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
        tracing::info!(
            project_id = %project_id,
            dataset = %dataset,
            "creating BigQuery connection from application default credentials"
        );

        let client = gcp_bigquery_client::Client::from_application_default_credentials()
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to create BigQuery client from ADC");
                SemaflowError::Execution(format!("create bigquery client: {e}"))
            })?;

        tracing::info!(
            project_id = %project_id,
            dataset = %dataset,
            "BigQuery connection established via ADC"
        );

        Ok(Self {
            client,
            project_id: project_id.to_string(),
            dataset: dataset.to_string(),
            dialect: BigQueryDialect,
        })
    }

    /// Get the project ID.
    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    /// Get the dataset name.
    pub fn dataset(&self) -> &str {
        &self.dataset
    }
}

#[async_trait]
impl BackendConnection for BigQueryConnection {
    fn dialect(&self) -> &(dyn crate::dialect::Dialect + Send + Sync) {
        &self.dialect
    }

    async fn fetch_schema(&self, table: &str) -> Result<TableSchema> {
        use gcp_bigquery_client::model::table::Table;

        let start = Instant::now();
        tracing::debug!(
            project = %self.project_id,
            dataset = %self.dataset,
            table = %table,
            "fetching BigQuery table schema"
        );

        let table_info: Table = self
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
                    data_type: format!("{:?}", field.r#type), // Convert FieldType enum to string
                    nullable: field.mode.as_ref().map_or(true, |m| m != "REQUIRED"),
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
        use gcp_bigquery_client::model::query_request::QueryRequest;

        let start = Instant::now();
        tracing::debug!(
            project = %self.project_id,
            sql_len = sql.len(),
            "executing BigQuery query"
        );
        tracing::trace!(sql = %sql, "BigQuery SQL");

        let query_request = QueryRequest::new(sql);
        let response = self
            .client
            .job()
            .query(&self.project_id, query_request)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "BigQuery query execution failed");
                SemaflowError::Execution(format!("bigquery query: {e}"))
            })?;

        // Use ResultSet to iterate over rows
        let mut rs =
            gcp_bigquery_client::model::query_response::ResultSet::new_from_query_response(
                response,
            );

        // Build column metadata from schema - collect names as owned Strings
        let col_names: Vec<String> = rs.column_names().iter().map(|s| s.to_string()).collect();
        let columns: Vec<ColumnMeta> = col_names
            .iter()
            .map(|name| ColumnMeta { name: name.clone() })
            .collect();

        // Convert rows to JSON
        let mut result_rows = Vec::new();
        while rs.next_row() {
            let mut map = serde_json::Map::new();
            for col_name in &col_names {
                let value = bq_value_to_json(&rs, col_name);
                map.insert(col_name.clone(), value);
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

/// Convert a BigQuery ResultSet value to JSON.
fn bq_value_to_json(
    rs: &gcp_bigquery_client::model::query_response::ResultSet,
    col_name: &str,
) -> serde_json::Value {
    use serde_json::Value;

    // Try different types in order of likelihood for analytics data
    if let Ok(Some(v)) = rs.get_f64_by_name(col_name) {
        return serde_json::Number::from_f64(v)
            .map(Value::Number)
            .unwrap_or(Value::Null);
    }
    if let Ok(Some(v)) = rs.get_i64_by_name(col_name) {
        return Value::Number(v.into());
    }
    if let Ok(Some(v)) = rs.get_bool_by_name(col_name) {
        return Value::Bool(v);
    }
    if let Ok(Some(v)) = rs.get_string_by_name(col_name) {
        return Value::String(v);
    }

    Value::Null
}
