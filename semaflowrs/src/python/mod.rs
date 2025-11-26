//! Python bindings (PyO3) for SemaFlow core. DuckDB-only for now.

use crate::{
    data_sources::{DataSource, DataSourceRegistry},
    query_builder::SqlBuilder,
    registry::ModelRegistry,
    runtime::run_query,
    validation::Validator,
    DuckDbExecutor, QueryRequest, SemanticModel, SemanticTable,
};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyAny;

fn py_err<E: std::fmt::Display>(msg: E) -> PyErr {
    PyRuntimeError::new_err(msg.to_string())
}

fn to_validation_err<E: std::fmt::Display>(msg: E) -> PyErr {
    PyValueError::new_err(msg.to_string())
}

fn dumps(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<String> {
    let json = py.import_bound("json")?;
    json.call_method1("dumps", (obj,))?.extract()
}

fn parse_tables(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Vec<SemanticTable>> {
    let s = dumps(py, obj)?;
    serde_json::from_str(&s).map_err(py_err)
}

fn parse_models(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Vec<SemanticModel>> {
    let s = dumps(py, obj)?;
    serde_json::from_str(&s).map_err(py_err)
}

fn parse_request(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<QueryRequest> {
    let s = dumps(py, obj)?;
    serde_json::from_str(&s).map_err(py_err)
}

fn build_registry(tables: Vec<SemanticTable>, models: Vec<SemanticModel>) -> ModelRegistry {
    ModelRegistry::from_parts(tables, models)
}

fn build_data_sources(mapping: &Bound<'_, PyAny>) -> PyResult<DataSourceRegistry> {
    let dict: std::collections::HashMap<String, String> = mapping.extract()?;
    let mut ds = DataSourceRegistry::new();
    for (name, path) in dict {
        ds.insert(name, DataSource::duckdb(DuckDbExecutor::new(path)));
    }
    Ok(ds)
}

#[pyfunction]
#[pyo3(text_signature = "(tables, models, data_sources, request)")]
/// Build SQL for a request.
/// - `tables`: list/dict of semantic tables (shape matches Rust schema; strings allowed for column exprs)
/// - `models`: list/dict of semantic models
/// - `data_sources`: dict of name -> DuckDB database path
/// - `request`: query request dict
fn build_sql(
    py: Python<'_>,
    tables: &Bound<'_, PyAny>,
    models: &Bound<'_, PyAny>,
    data_sources: &Bound<'_, PyAny>,
    request: &Bound<'_, PyAny>,
) -> PyResult<String> {
    let tables = parse_tables(py, tables)?;
    let models = parse_models(py, models)?;
    let request = parse_request(py, request)?;
    let registry = build_registry(tables, models);
    let ds = build_data_sources(data_sources)?;
    let builder = SqlBuilder::default();
    builder
        .build_for_request(&registry, &ds, &request)
        .map_err(to_validation_err)
}

#[pyfunction]
#[pyo3(text_signature = "(tables, models, data_sources, request)")]
/// Validate, build SQL, execute against DuckDB, and return rows (list[dict]).
fn run(
    py: Python<'_>,
    tables: &Bound<'_, PyAny>,
    models: &Bound<'_, PyAny>,
    data_sources: &Bound<'_, PyAny>,
    request: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let tables = parse_tables(py, tables)?;
    let models = parse_models(py, models)?;
    let request = parse_request(py, request)?;
    let mut registry = build_registry(tables, models);
    let ds = build_data_sources(data_sources)?;
    let validator = Validator::new(ds.clone(), false);
    let rt = tokio::runtime::Runtime::new().map_err(py_err)?;
    let rows_json: String = rt.block_on(async {
        validator
            .validate_registry(&mut registry)
            .await
            .map_err(to_validation_err)?;
        let result = run_query(&registry, &ds, &request)
            .await
            .map_err(to_validation_err)?;
        serde_json::to_string(&result.rows).map_err(py_err)
    })?;

    let json = py.import_bound("json")?;
    let py_obj = json.call_method1("loads", (rows_json,))?;
    Ok(py_obj.into_py(py))
}

/// PyO3 module entrypoint
#[pymodule]
fn semaflow_core(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(build_sql, m)?)?;
    m.add_function(wrap_pyfunction!(run, m)?)?;
    Ok(())
}
