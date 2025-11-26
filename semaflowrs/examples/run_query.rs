use std::{fs, path::Path};

use semaflow_core::{
    data_sources::{DataSource, DataSourceRegistry},
    registry::ModelRegistry,
    runtime::run_query,
    validation::Validator,
    DuckDbExecutor, QueryRequest,
};

fn bootstrap_duckdb(path: &Path) -> anyhow::Result<()> {
    let conn = duckdb::Connection::open(path)?;
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_path = Path::new("examples/demo.duckdb");
    if db_path.exists() {
        fs::remove_file(db_path)?;
    }
    bootstrap_duckdb(db_path)?;

    let executor = DuckDbExecutor::new(db_path);
    let mut data_sources = DataSourceRegistry::new();
    data_sources.insert("duckdb_local", DataSource::duckdb(executor.clone()));

    let mut registry = ModelRegistry::load_from_dir("examples/models")?;
    let validator = Validator::new(data_sources.clone(), false);
    validator.validate_registry(&mut registry).await?;

    let request: QueryRequest =
        serde_json::from_str(&fs::read_to_string("examples/requests/sales_country.json")?)?;

    let result = run_query(&registry, &data_sources, &request).await?;
    println!("SQL rows: {}", result.rows.len());
    for row in result.rows {
        println!("{row:?}");
    }
    Ok(())
}
