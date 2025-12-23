//! Cursor-based pagination utilities.
//!
//! Provides stateless cursor encoding/decoding for paginating query results.
//! Supports two cursor types:
//! - BigQuery: Uses native job_id + page_token for zero re-computation
//! - SQL (Postgres/DuckDB): Uses LIMIT/OFFSET pagination

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::error::{Result, SemaflowError};
use crate::flows::QueryRequest;

/// Cursor for paginating through query results.
///
/// Two variants support backend-specific pagination strategies:
/// - BigQuery uses native job pagination (no re-execution)
/// - SQL backends use LIMIT/OFFSET
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "backend", rename_all = "snake_case")]
pub enum Cursor {
    /// BigQuery cursor using native job-based pagination.
    /// Subsequent pages fetch from the SAME cached job result.
    BigQuery {
        /// BigQuery job ID for result retrieval
        job_id: String,
        /// Native page token from BigQuery
        page_token: String,
        /// Query hash to validate cursor matches current query
        query_hash: u64,
        /// Row offset for graceful fallback if job expires
        offset: u64,
    },
    /// SQL cursor using LIMIT/OFFSET pagination.
    /// Used for Postgres and DuckDB backends.
    Sql {
        /// Row offset for next page
        offset: u64,
        /// Query hash to validate cursor matches current query
        query_hash: u64,
    },
}

impl Cursor {
    /// Create a new BigQuery cursor.
    pub fn bigquery(job_id: String, page_token: String, query_hash: u64, offset: u64) -> Self {
        Cursor::BigQuery {
            job_id,
            page_token,
            query_hash,
            offset,
        }
    }

    /// Create a new SQL cursor for LIMIT/OFFSET pagination.
    pub fn sql(offset: u64, query_hash: u64) -> Self {
        Cursor::Sql { offset, query_hash }
    }

    /// Get the query hash from this cursor.
    pub fn query_hash(&self) -> u64 {
        match self {
            Cursor::BigQuery { query_hash, .. } => *query_hash,
            Cursor::Sql { query_hash, .. } => *query_hash,
        }
    }

    /// Get the row offset from this cursor.
    pub fn offset(&self) -> u64 {
        match self {
            Cursor::BigQuery { offset, .. } => *offset,
            Cursor::Sql { offset, .. } => *offset,
        }
    }

    /// Encode cursor to a URL-safe base64 string.
    pub fn encode(&self) -> Result<String> {
        let json = serde_json::to_string(self)
            .map_err(|e| SemaflowError::Execution(format!("failed to serialize cursor: {e}")))?;
        Ok(URL_SAFE_NO_PAD.encode(json.as_bytes()))
    }

    /// Decode cursor from a base64 string.
    pub fn decode(encoded: &str) -> Result<Self> {
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|e| SemaflowError::Validation(format!("invalid cursor encoding: {e}")))?;
        let json = String::from_utf8(bytes)
            .map_err(|e| SemaflowError::Validation(format!("invalid cursor UTF-8: {e}")))?;
        serde_json::from_str(&json)
            .map_err(|e| SemaflowError::Validation(format!("invalid cursor format: {e}")))
    }

    /// Validate that this cursor matches the given query hash.
    pub fn validate_query_hash(&self, expected_hash: u64) -> Result<()> {
        if self.query_hash() != expected_hash {
            return Err(SemaflowError::Validation(
                "cursor does not match current query - the query parameters may have changed"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

/// Compute a hash of the query request for cursor validation.
///
/// This ensures cursors can only be used with the same query they were created for.
/// The hash includes all query parameters except pagination-specific fields.
pub fn compute_query_hash(request: &QueryRequest) -> u64 {
    let mut hasher = DefaultHasher::new();

    // Hash all non-pagination fields
    request.flow.hash(&mut hasher);
    request.dimensions.hash(&mut hasher);
    request.measures.hash(&mut hasher);

    // Hash filters by serializing them
    if let Ok(filters_json) = serde_json::to_string(&request.filters) {
        filters_json.hash(&mut hasher);
    }

    // Hash order
    if let Ok(order_json) = serde_json::to_string(&request.order) {
        order_json.hash(&mut hasher);
    }

    // Include limit in hash since it affects total result cap
    request.limit.hash(&mut hasher);

    // Note: page_size, cursor, and offset are NOT included in hash
    // since they're pagination controls, not query definition

    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_cursor_roundtrip() {
        let cursor = Cursor::sql(100, 12345678);
        let encoded = cursor.encode().unwrap();
        let decoded = Cursor::decode(&encoded).unwrap();

        match decoded {
            Cursor::Sql { offset, query_hash } => {
                assert_eq!(offset, 100);
                assert_eq!(query_hash, 12345678);
            }
            _ => panic!("expected SQL cursor"),
        }
    }

    #[test]
    fn test_bigquery_cursor_roundtrip() {
        let cursor = Cursor::bigquery(
            "job_abc123".to_string(),
            "token_xyz".to_string(),
            98765432,
            50,
        );
        let encoded = cursor.encode().unwrap();
        let decoded = Cursor::decode(&encoded).unwrap();

        match decoded {
            Cursor::BigQuery {
                job_id,
                page_token,
                query_hash,
                offset,
            } => {
                assert_eq!(job_id, "job_abc123");
                assert_eq!(page_token, "token_xyz");
                assert_eq!(query_hash, 98765432);
                assert_eq!(offset, 50);
            }
            _ => panic!("expected BigQuery cursor"),
        }
    }

    #[test]
    fn test_invalid_cursor_rejected() {
        let result = Cursor::decode("not-valid-base64!!!");
        assert!(result.is_err());

        let result = Cursor::decode(&URL_SAFE_NO_PAD.encode(b"not json"));
        assert!(result.is_err());
    }

    #[test]
    fn test_query_hash_validation() {
        let cursor = Cursor::sql(100, 12345);

        // Same hash should pass
        assert!(cursor.validate_query_hash(12345).is_ok());

        // Different hash should fail
        assert!(cursor.validate_query_hash(99999).is_err());
    }

    #[test]
    fn test_query_hash_consistency() {
        let request = QueryRequest {
            flow: "sales".to_string(),
            dimensions: vec!["country".to_string()],
            measures: vec!["revenue".to_string()],
            ..Default::default()
        };

        // Same request should produce same hash
        let hash1 = compute_query_hash(&request);
        let hash2 = compute_query_hash(&request);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_query_hash_different_queries() {
        let request1 = QueryRequest {
            flow: "sales".to_string(),
            dimensions: vec!["country".to_string()],
            measures: vec!["revenue".to_string()],
            ..Default::default()
        };

        let request2 = QueryRequest {
            flow: "sales".to_string(),
            dimensions: vec!["region".to_string()], // Different dimension
            measures: vec!["revenue".to_string()],
            ..Default::default()
        };

        // Different requests should produce different hashes
        let hash1 = compute_query_hash(&request1);
        let hash2 = compute_query_hash(&request2);
        assert_ne!(hash1, hash2);
    }
}
