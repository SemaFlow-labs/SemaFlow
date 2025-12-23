use std::fs;
use std::path::Path;

use semaflow::{
    backends::{BackendConnection, ConnectionManager, DuckDbConnection},
    query_builder::SqlBuilder,
    registry::FlowRegistry,
    runtime::{run_query, run_query_paginated},
    validation::Validator,
    QueryRequest, QueryResult, TableSchema,
};
use tokio;

#[derive(Clone)]
struct FakeConnection;

#[async_trait::async_trait]
impl BackendConnection for FakeConnection {
    fn dialect(&self) -> &(dyn semaflow::dialect::Dialect + Send + Sync) {
        &semaflow::dialect::DuckDbDialect
    }

    async fn fetch_schema(&self, _table: &str) -> semaflow::error::Result<TableSchema> {
        Ok(TableSchema {
            columns: vec![semaflow::schema_cache::ColumnSchema {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
            }],
            primary_keys: vec!["id".to_string()],
            foreign_keys: vec![],
        })
    }
    async fn execute_sql(&self, _sql: &str) -> semaflow::error::Result<QueryResult> {
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        })
    }

    async fn execute_sql_paginated(
        &self,
        _sql: &str,
        _page_size: u32,
        _cursor: Option<&semaflow::pagination::Cursor>,
        _query_hash: u64,
    ) -> semaflow::error::Result<semaflow::executor::PaginatedResult> {
        Ok(semaflow::executor::PaginatedResult {
            columns: vec![],
            rows: vec![],
            cursor: None,
            has_more: false,
            total_rows: None,
        })
    }
}

fn bootstrap_duckdb(db_path: &Path) -> anyhow::Result<()> {
    let conn = duckdb::Connection::open(db_path)?;
    conn.execute_batch(
        "
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            country VARCHAR
        );
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            amount DOUBLE,
            created_at TIMESTAMP
        );
        INSERT INTO customers VALUES
            (1, 'Alice', 'US'),
            (2, 'Bob', 'UK'),
            (3, 'Carla', 'US');
        INSERT INTO orders VALUES
            (1, 1, 100.0, '2023-01-01'),
            (2, 1, 50.0, '2023-01-02'),
            (3, 2, 25.0, '2023-01-03');
        ",
    )?;
    Ok(())
}

fn write_flows(root: &Path) -> anyhow::Result<()> {
    let tables_dir = root.join("tables");
    let flows_dir = root.join("flows");
    fs::create_dir_all(&tables_dir)?;
    fs::create_dir_all(&flows_dir)?;

    let customers = r#"
name: customers
data_source: duckdb_local
table: customers
primary_key: id
dimensions:
  id:
    expr:
      type: column
      column: id
  country:
    expr:
      type: column
      column: country
measures:
  customer_count:
    expr:
      type: column
      column: id
    agg: count
"#;
    fs::write(tables_dir.join("customers.yaml"), customers)?;

    let orders = r#"
name: orders
data_source: duckdb_local
table: orders
primary_key: id
time_dimension: created_at
dimensions:
  id:
    expr:
      type: column
      column: id
  customer_id:
    expr:
      type: column
      column: customer_id
measures:
  order_total:
    expr:
      type: column
      column: amount
    agg: sum
  distinct_customers:
    expr:
      type: column
      column: customer_id
    agg: count_distinct
"#;
    fs::write(tables_dir.join("orders.yaml"), orders)?;

    let sales_flow = r#"
name: sales
base_table:
  semantic_table: orders
  alias: o
joins:
  customers:
    semantic_table: customers
    alias: c
    to_table: o
    join_type: left
    join_keys:
      - left: customer_id
        right: id
"#;
    fs::write(flows_dir.join("sales.yaml"), sales_flow)?;
    Ok(())
}

#[tokio::test]
async fn duckdb_query_round_trip() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("demo.duckdb");
    bootstrap_duckdb(&db_path)?;
    write_flows(dir.path())?;

    let mut connections = ConnectionManager::new();
    connections.insert(
        "duckdb_local",
        std::sync::Arc::new(DuckDbConnection::new(&db_path).with_max_concurrency(8)),
    );
    let validator = Validator::new(connections.clone(), false);

    let mut registry = FlowRegistry::load_from_dir(dir.path())?;
    validator.validate_registry(&mut registry).await?;

    let builder = SqlBuilder::default();
    let request = QueryRequest {
        flow: "sales".to_string(),
        dimensions: vec!["country".to_string()],
        measures: vec!["order_total".to_string(), "distinct_customers".to_string()],
        filters: vec![],
        order: vec![],
        limit: Some(10),
        offset: None,
        page_size: None,
        cursor: None,
    };
    let sql = builder.build_for_request(&registry, &connections, &request)?;
    let result = connections
        .get("duckdb_local")
        .unwrap()
        .execute_sql(&sql)
        .await?;
    assert_eq!(result.rows.len(), 2);
    let mut by_country = std::collections::HashMap::new();
    for row in result.rows {
        let country = row.get("country").and_then(|v| v.as_str()).unwrap();
        by_country.insert(country.to_string(), row);
    }
    let us = by_country.get("US").unwrap();
    assert_eq!(us.get("order_total").unwrap().as_f64().unwrap(), 150.0);
    assert_eq!(us.get("distinct_customers").unwrap().as_u64().unwrap(), 1);
    Ok(())
}

#[tokio::test]
async fn duckdb_runtime_run_query() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("demo.duckdb");
    bootstrap_duckdb(&db_path)?;
    write_flows(dir.path())?;

    let mut connections = ConnectionManager::new();
    connections.insert(
        "duckdb_local",
        std::sync::Arc::new(DuckDbConnection::new(&db_path).with_max_concurrency(8)),
    );
    let validator = Validator::new(connections.clone(), false);

    let mut registry = FlowRegistry::load_from_dir(dir.path())?;
    validator.validate_registry(&mut registry).await?;

    let request = QueryRequest {
        flow: "sales".to_string(),
        dimensions: vec!["country".to_string()],
        measures: vec!["order_total".to_string()],
        filters: vec![],
        order: vec![],
        limit: Some(10),
        offset: None,
        page_size: None,
        cursor: None,
    };

    let result = run_query(&registry, &connections, &request).await?;
    assert_eq!(result.rows.len(), 2);
    Ok(())
}

#[tokio::test]
async fn duckdb_paginated_query() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("demo.duckdb");
    bootstrap_duckdb(&db_path)?;
    write_flows(dir.path())?;

    let mut connections = ConnectionManager::new();
    connections.insert(
        "duckdb_local",
        std::sync::Arc::new(DuckDbConnection::new(&db_path).with_max_concurrency(8)),
    );
    let validator = Validator::new(connections.clone(), false);

    let mut registry = FlowRegistry::load_from_dir(dir.path())?;
    validator.validate_registry(&mut registry).await?;

    // First page - page_size=1 to ensure multiple pages
    let request = QueryRequest {
        flow: "sales".to_string(),
        dimensions: vec!["country".to_string()],
        measures: vec!["order_total".to_string()],
        filters: vec![],
        order: vec![],
        limit: None,
        offset: None,
        page_size: Some(1),
        cursor: None,
    };

    let result = run_query_paginated(&registry, &connections, &request).await?;
    assert_eq!(result.rows.len(), 1, "First page should have 1 row");
    assert!(result.has_more, "Should have more pages");
    assert!(result.cursor.is_some(), "Should have cursor for next page");

    // Second page using cursor
    let request2 = QueryRequest {
        flow: "sales".to_string(),
        dimensions: vec!["country".to_string()],
        measures: vec!["order_total".to_string()],
        filters: vec![],
        order: vec![],
        limit: None,
        offset: None,
        page_size: Some(1),
        cursor: result.cursor,
    };

    let result2 = run_query_paginated(&registry, &connections, &request2).await?;
    assert_eq!(result2.rows.len(), 1, "Second page should have 1 row");
    // With 2 countries total, second page is the last
    assert!(!result2.has_more, "Should be last page");
    assert!(result2.cursor.is_none(), "Last page should have no cursor");

    Ok(())
}

#[tokio::test]
async fn duckdb_paginated_invalid_cursor_rejected() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("demo.duckdb");
    bootstrap_duckdb(&db_path)?;
    write_flows(dir.path())?;

    let mut connections = ConnectionManager::new();
    connections.insert(
        "duckdb_local",
        std::sync::Arc::new(DuckDbConnection::new(&db_path).with_max_concurrency(8)),
    );
    let validator = Validator::new(connections.clone(), false);

    let mut registry = FlowRegistry::load_from_dir(dir.path())?;
    validator.validate_registry(&mut registry).await?;

    // Invalid cursor should be rejected
    let request = QueryRequest {
        flow: "sales".to_string(),
        dimensions: vec!["country".to_string()],
        measures: vec!["order_total".to_string()],
        filters: vec![],
        order: vec![],
        limit: None,
        offset: None,
        page_size: Some(10),
        cursor: Some("invalid_cursor".to_string()),
    };

    let result = run_query_paginated(&registry, &connections, &request).await;
    assert!(result.is_err(), "Invalid cursor should error");

    Ok(())
}

#[tokio::test]
async fn mixed_data_sources_fail_validation() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let tables_dir = dir.path().join("tables");
    let flows_dir = dir.path().join("flows");
    fs::create_dir_all(&tables_dir)?;
    fs::create_dir_all(&flows_dir)?;

    let t1 = r#"
name: t1
data_source: ds1
table: t1
primary_key: id
dimensions:
  id: id
"#;
    fs::write(tables_dir.join("t1.yaml"), t1)?;

    let t2 = r#"
name: t2
data_source: ds2
table: t2
primary_key: id
dimensions:
  id: id
"#;
    fs::write(tables_dir.join("t2.yaml"), t2)?;

    let flow_yaml = r#"
name: cross
base_table:
  semantic_table: t1
  alias: a
joins:
  b:
    semantic_table: t2
    alias: b
    to_table: a
    join_type: inner
    join_keys:
      - left: id
        right: id
"#;
    fs::write(flows_dir.join("cross.yaml"), flow_yaml)?;

    let mut registry = FlowRegistry::load_from_dir(dir.path())?;
    let mut connections = ConnectionManager::new();
    let dummy = FakeConnection;
    connections.insert("ds1", std::sync::Arc::new(dummy.clone()));
    connections.insert("ds2", std::sync::Arc::new(dummy));

    let validator = Validator::new(connections, false);
    let err = validator
        .validate_registry(&mut registry)
        .await
        .unwrap_err();
    match err {
        semaflow::SemaflowError::Validation(msg) => {
            eprintln!("validation message: {msg}");
            assert!(msg.contains("mixes data sources"));
        }
        other => panic!("unexpected error {other:?}"),
    }
    Ok(())
}
