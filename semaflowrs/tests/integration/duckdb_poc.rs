use std::fs;
use std::path::Path;

use semaflow_core::{
    data_sources::{DataSource, DataSourceRegistry},
    query_builder::SqlBuilder,
    registry::ModelRegistry,
    runtime::run_query,
    validation::Validator,
    DuckDbExecutor, QueryExecutor, QueryRequest, QueryResult, SchemaProvider, TableSchema,
};
use tokio;

#[derive(Clone)]
struct FakeExecutor;

#[async_trait::async_trait]
impl SchemaProvider for FakeExecutor {
    async fn fetch_schema(&self, _table: &str) -> semaflow_core::error::Result<TableSchema> {
        Ok(TableSchema {
            columns: vec![semaflow_core::schema_cache::ColumnSchema {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
            }],
            primary_keys: vec!["id".to_string()],
            foreign_keys: vec![],
        })
    }
}

#[async_trait::async_trait]
impl QueryExecutor for FakeExecutor {
    async fn query(&self, _sql: &str) -> semaflow_core::error::Result<QueryResult> {
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
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

fn write_models(root: &Path) -> anyhow::Result<()> {
    let tables_dir = root.join("tables");
    let models_dir = root.join("models");
    fs::create_dir_all(&tables_dir)?;
    fs::create_dir_all(&models_dir)?;

    let customers = r#"
name: customers
data_source: duckdb_local
table: customers
primary_key: id
dimensions:
  id:
    expression:
      type: column
      column: id
  country:
    expression:
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
    expression:
      type: column
      column: id
  customer_id:
    expression:
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

    let sales_model = r#"
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
    fs::write(models_dir.join("sales.yaml"), sales_model)?;
    Ok(())
}

#[tokio::test]
async fn duckdb_query_round_trip() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("demo.duckdb");
    bootstrap_duckdb(&db_path)?;
    write_models(dir.path())?;

    let executor = DuckDbExecutor::new(&db_path);
    let mut ds_registry = DataSourceRegistry::new();
    ds_registry.insert("duckdb_local", DataSource::duckdb(executor.clone()));
    let validator = Validator::new(ds_registry.clone(), false);

    let mut registry = ModelRegistry::load_from_dir(dir.path())?;
    validator.validate_registry(&mut registry).await?;

    let builder = SqlBuilder::default();
    let request = QueryRequest {
        model: "sales".to_string(),
        dimensions: vec!["country".to_string()],
        measures: vec!["order_total".to_string(), "distinct_customers".to_string()],
        filters: vec![],
        order: vec![],
        limit: Some(10),
        offset: None,
    };
    let sql = builder.build_for_request(&registry, &ds_registry, &request)?;
    let result = executor.query(&sql).await?;
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
    write_models(dir.path())?;

    let executor = DuckDbExecutor::new(&db_path);
    let mut ds_registry = DataSourceRegistry::new();
    ds_registry.insert("duckdb_local", DataSource::duckdb(executor.clone()));
    let validator = Validator::new(ds_registry.clone(), false);

    let mut registry = ModelRegistry::load_from_dir(dir.path())?;
    validator.validate_registry(&mut registry).await?;

    let request = QueryRequest {
        model: "sales".to_string(),
        dimensions: vec!["country".to_string()],
        measures: vec!["order_total".to_string()],
        filters: vec![],
        order: vec![],
        limit: Some(10),
        offset: None,
    };

    let result = run_query(&registry, &ds_registry, &request).await?;
    assert_eq!(result.rows.len(), 2);
    Ok(())
}

#[tokio::test]
async fn mixed_data_sources_fail_validation() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let tables_dir = dir.path().join("tables");
    let models_dir = dir.path().join("models");
    fs::create_dir_all(&tables_dir)?;
    fs::create_dir_all(&models_dir)?;

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

    let model = r#"
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
    fs::write(models_dir.join("cross.yaml"), model)?;

    let mut registry = ModelRegistry::load_from_dir(dir.path())?;
    let mut ds_registry = DataSourceRegistry::new();
    let dummy = FakeExecutor;
    ds_registry.insert("ds1", DataSource::duckdb(dummy.clone()));
    ds_registry.insert("ds2", DataSource::duckdb(dummy));

    let validator = Validator::new(ds_registry, false);
    let err = validator
        .validate_registry(&mut registry)
        .await
        .unwrap_err();
    match err {
        semaflow_core::SemaflowError::Validation(msg) => {
            eprintln!("validation message: {msg}");
            assert!(msg.contains("mixes data sources"));
        }
        other => panic!("unexpected error {other:?}"),
    }
    Ok(())
}
