use std::{env, fs, path::PathBuf};

use semaflow_core::{
    data_sources::{DataSource, DataSourceRegistry},
    query_builder::SqlBuilder,
    registry::ModelRegistry,
    DuckDbExecutor, QueryRequest,
};

fn usage() {
    eprintln!("Usage: print_sql <models_dir> <request_json>");
    eprintln!("Example: cargo run --example print_sql -- examples/models examples/requests/sales_country.json");
}

fn main() -> anyhow::Result<()> {
    let mut args = env::args().skip(1).collect::<Vec<_>>();
    if args.len() < 2 {
        usage();
        std::process::exit(1);
    }

    let models_dir = PathBuf::from(args.remove(0));
    let request_path = PathBuf::from(args.remove(0));

    let mut data_sources = DataSourceRegistry::new();
    data_sources.insert(
        "duckdb_local",
        DataSource::duckdb(DuckDbExecutor::new(":memory:")),
    );

    let registry = ModelRegistry::load_from_dir(models_dir)?;
    let request_str = fs::read_to_string(request_path)?;
    let request: QueryRequest = serde_json::from_str(&request_str)?;

    let builder = SqlBuilder::default();
    let sql = builder.build_for_request(&registry, &data_sources, &request)?;
    println!("{sql}");
    Ok(())
}
