//! Python bindings (PyO3) for SemaFlow core. DuckDB-only for now.

use crate::{
    data_sources::{ConnectionManager, DuckDbConnection},
    flows::{
        Aggregation, Dimension, Expr, FlowJoin, FlowTableRef, SemanticFlow as CoreSemanticFlow,
        SemanticTable,
    },
    query_builder::SqlBuilder,
    registry::FlowRegistry,
    runtime::run_query,
    validation::Validator,
    QueryRequest, SemaflowError,
};
use once_cell::sync::OnceCell;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use tracing_subscriber::{fmt, EnvFilter};

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceCell<tokio::runtime::Runtime> = OnceCell::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("create tokio runtime"))
}

fn init_tracing() {
    static TRACING: OnceCell<()> = OnceCell::new();
    TRACING.get_or_init(|| {
        // Safe to ignore error if a subscriber is already set elsewhere.
        let _ = fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .with_target(false)
            .try_init();
    });
}

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

fn expr_from_py(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Expr> {
    if let Ok(s) = obj.extract::<String>() {
        return Ok(Expr::Column { column: s });
    }
    let s = dumps(py, obj)?;
    serde_json::from_str(&s).map_err(py_err)
}

fn dimensions_from_py(
    py: Python<'_>,
    obj: Option<&Bound<'_, PyAny>>,
) -> PyResult<BTreeMap<String, Dimension>> {
    let mut map = BTreeMap::new();
    let Some(value) = obj else {
        return Ok(map);
    };
    if let Ok(dict) = value.downcast::<PyDict>() {
        for (key, val) in dict.iter() {
            let name: String = key.extract()?;
            if let Ok(dim) = val.extract::<PyDimension>() {
                map.insert(name, dim.inner);
            } else {
                let s = dumps(py, &val)?;
                let dim: Dimension = serde_json::from_str(&s).map_err(py_err)?;
                map.insert(name, dim);
            }
        }
        return Ok(map);
    }
    let s = dumps(py, value)?;
    serde_json::from_str(&s).map_err(py_err)
}

fn measures_from_py(
    py: Python<'_>,
    obj: Option<&Bound<'_, PyAny>>,
) -> PyResult<BTreeMap<String, crate::flows::Measure>> {
    let mut map = BTreeMap::new();
    let Some(value) = obj else {
        return Ok(map);
    };
    if let Ok(dict) = value.downcast::<PyDict>() {
        for (key, val) in dict.iter() {
            let name: String = key.extract()?;
            if let Ok(measure) = val.extract::<PyMeasure>() {
                map.insert(name, measure.inner);
            } else {
                let s = dumps(py, &val)?;
                let measure: crate::flows::Measure = serde_json::from_str(&s).map_err(py_err)?;
                map.insert(name, measure);
            }
        }
        return Ok(map);
    }
    let s = dumps(py, value)?;
    serde_json::from_str(&s).map_err(py_err)
}

#[pyclass(name = "DataSource", module = "semaflow.semaflow")]
#[derive(Clone)]
pub struct PyDataSource {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub uri: String,
    #[pyo3(get)]
    pub max_concurrency: Option<usize>,
}

#[pymethods]
impl PyDataSource {
    #[new]
    #[pyo3(signature = (name, uri, max_concurrency=None))]
    fn new(name: String, uri: String, max_concurrency: Option<usize>) -> Self {
        Self {
            name,
            uri,
            max_concurrency,
        }
    }

    #[staticmethod]
    #[pyo3(signature = (path, name=None, max_concurrency=None))]
    fn duckdb(path: String, name: Option<String>, max_concurrency: Option<usize>) -> Self {
        Self {
            name: name.unwrap_or_else(|| "duckdb".to_string()),
            uri: path,
            max_concurrency,
        }
    }

    fn table(&self, name: String) -> PyTableHandle {
        PyTableHandle {
            data_source: self.name.clone(),
            table: name,
        }
    }
}

#[pyclass(name = "TableHandle", module = "semaflow.semaflow")]
#[derive(Clone)]
pub struct PyTableHandle {
    #[pyo3(get)]
    pub data_source: String,
    #[pyo3(get)]
    pub table: String,
}

#[pyclass(name = "JoinKey", module = "semaflow.semaflow")]
#[derive(Clone)]
pub struct PyJoinKey {
    #[pyo3(get)]
    pub left: String,
    #[pyo3(get)]
    pub right: String,
}

#[pymethods]
impl PyJoinKey {
    #[new]
    fn new(left: String, right: String) -> Self {
        Self { left, right }
    }
}

#[pyclass(name = "Dimension", module = "semaflow.semaflow")]
#[derive(Clone)]
pub struct PyDimension {
    pub inner: Dimension,
}

#[pymethods]
impl PyDimension {
    #[new]
    #[pyo3(signature = (expression, data_type=None, description=None))]
    fn new(
        py: Python<'_>,
        expression: &Bound<'_, PyAny>,
        data_type: Option<String>,
        description: Option<String>,
    ) -> PyResult<Self> {
        let expr = expr_from_py(py, expression)?;
        Ok(Self {
            inner: Dimension {
                expression: expr,
                data_type,
                description,
            },
        })
    }
}

#[pyclass(name = "Measure", module = "semaflow.semaflow")]
#[derive(Clone)]
pub struct PyMeasure {
    pub inner: crate::flows::Measure,
}

#[pymethods]
impl PyMeasure {
    #[new]
    #[pyo3(signature = (expression, agg, data_type=None, description=None, filter=None, post_expr=None))]
    fn new(
        py: Python<'_>,
        expression: &Bound<'_, PyAny>,
        agg: &str,
        data_type: Option<String>,
        description: Option<String>,
        filter: Option<&Bound<'_, PyAny>>,
        post_expr: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let expr = expr_from_py(py, expression)?;
        let agg_enum = match agg {
            "sum" => Aggregation::Sum,
            "count" => Aggregation::Count,
            "count_distinct" => Aggregation::CountDistinct,
            "min" => Aggregation::Min,
            "max" => Aggregation::Max,
            "avg" => Aggregation::Avg,
            other => {
                return Err(PyValueError::new_err(format!(
                    "unknown aggregation {other}"
                )))
            }
        };
        let filter_expr = if let Some(f) = filter {
            Some(expr_from_py(py, f)?)
        } else {
            None
        };
        let post_expr = if let Some(p) = post_expr {
            Some(expr_from_py(py, p)?)
        } else {
            None
        };
        Ok(Self {
            inner: crate::flows::Measure {
                expr,
                agg: agg_enum,
                filter: filter_expr,
                post_expr,
                data_type,
                description,
            },
        })
    }
}

#[pyclass(name = "FlowJoin", module = "semaflow.semaflow")]
#[derive(Clone)]
pub struct PyFlowJoin {
    pub inner: FlowJoin,
    pub table: PySemanticTable,
}

#[pymethods]
impl PyFlowJoin {
    #[new]
    #[pyo3(signature = (semantic_table, alias, to_table, join_keys, join_type="left", description=None))]
    fn new(
        semantic_table: PySemanticTable,
        alias: String,
        to_table: String,
        join_keys: Vec<PyJoinKey>,
        join_type: &str,
        description: Option<String>,
    ) -> PyResult<Self> {
        let jt = match join_type {
            "inner" => crate::flows::JoinType::Inner,
            "left" => crate::flows::JoinType::Left,
            "right" => crate::flows::JoinType::Right,
            "full" => crate::flows::JoinType::Full,
            _ => {
                return Err(PyValueError::new_err(
                    "join_type must be one of: inner, left, right, full",
                ))
            }
        };
        let keys = join_keys
            .into_iter()
            .map(|k| crate::flows::JoinKey {
                left: k.left,
                right: k.right,
            })
            .collect();
        Ok(Self {
            inner: FlowJoin {
                semantic_table: semantic_table.inner.name.clone(),
                alias,
                to_table,
                join_type: jt,
                join_keys: keys,
                description,
            },
            table: semantic_table,
        })
    }
}

#[pyclass(name = "SemanticTable", module = "semaflow.semaflow")]
#[derive(Clone)]
pub struct PySemanticTable {
    pub inner: SemanticTable,
    pub data_source_obj: Option<PyDataSource>,
}

#[pymethods]
impl PySemanticTable {
    #[new]
    #[pyo3(signature = (name, data_source, table, primary_key, time_dimension=None, dimensions=None, measures=None, description=None))]
    fn new(
        py: Python<'_>,
        name: String,
        data_source: &Bound<'_, PyAny>,
        table: String,
        primary_key: String,
        time_dimension: Option<String>,
        dimensions: Option<&Bound<'_, PyAny>>,
        measures: Option<&Bound<'_, PyAny>>,
        description: Option<String>,
    ) -> PyResult<Self> {
        let (ds_name, ds_obj) = if let Ok(ds) = data_source.extract::<PyDataSource>() {
            (ds.name.clone(), Some(ds))
        } else {
            (data_source.extract::<String>()?, None)
        };
        let dims = dimensions_from_py(py, dimensions)?;
        let measures = measures_from_py(py, measures)?;
        Ok(Self {
            inner: SemanticTable {
                name,
                data_source: ds_name,
                table,
                primary_key,
                time_dimension,
                smallest_time_grain: None,
                dimensions: dims,
                measures,
                description,
            },
            data_source_obj: ds_obj,
        })
    }

    #[staticmethod]
    #[pyo3(signature = (name, table_handle, primary_key, time_dimension=None, dimensions=None, measures=None, description=None))]
    fn from_table(
        py: Python<'_>,
        name: String,
        table_handle: PyTableHandle,
        primary_key: String,
        time_dimension: Option<String>,
        dimensions: Option<&Bound<'_, PyAny>>,
        measures: Option<&Bound<'_, PyAny>>,
        description: Option<String>,
    ) -> PyResult<Self> {
        let data_source_obj = pyo3::types::PyString::new_bound(py, &table_handle.data_source);
        Self::new(
            py,
            name,
            &data_source_obj,
            table_handle.table,
            primary_key,
            time_dimension,
            dimensions,
            measures,
            description,
        )
    }

    #[getter]
    fn data_source(&self) -> Option<PyDataSource> {
        self.data_source_obj.clone()
    }

    #[getter]
    fn name(&self) -> &str {
        &self.inner.name
    }
}

#[pyclass(name = "SemanticFlow", module = "semaflow.semaflow")]
#[derive(Clone)]
pub struct PySemanticFlow {
    pub inner: CoreSemanticFlow,
    pub tables: Vec<PySemanticTable>,
}

#[pymethods]
impl PySemanticFlow {
    #[new]
    #[pyo3(signature = (name, base_table, base_table_alias, joins=None, description=None))]
    fn new(
        name: String,
        base_table: PySemanticTable,
        base_table_alias: String,
        joins: Option<Vec<PyFlowJoin>>,
        description: Option<String>,
    ) -> Self {
        let mut table_refs = vec![base_table.clone()];
        let mut join_map: BTreeMap<String, FlowJoin> = BTreeMap::new();
        if let Some(items) = joins {
            for join in items {
                table_refs.push(join.table.clone());
                join_map.insert(join.inner.alias.clone(), join.inner.clone());
            }
        }
        Self {
            inner: CoreSemanticFlow {
                name,
                base_table: FlowTableRef {
                    semantic_table: base_table.inner.name.clone(),
                    alias: base_table_alias,
                },
                joins: join_map,
                description,
            },
            tables: table_refs,
        }
    }

    fn referenced_tables(&self) -> Vec<PySemanticTable> {
        self.tables.clone()
    }

    #[getter]
    fn description(&self) -> Option<String> {
        self.inner.description.clone()
    }
}

fn parse_tables(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Vec<SemanticTable>> {
    if let Ok(v) = obj.extract::<Vec<PySemanticTable>>() {
        return Ok(v.into_iter().map(|t| t.inner).collect());
    }
    let s = dumps(py, obj)?;
    serde_json::from_str(&s).map_err(py_err)
}

fn parse_flows(obj: &Bound<'_, PyAny>) -> PyResult<Vec<CoreSemanticFlow>> {
    if let Ok(v) = obj.extract::<Vec<PySemanticFlow>>() {
        return Ok(v.into_iter().map(|m| m.inner).collect());
    }
    Err(PyValueError::new_err(
        "flows must be a list of SemanticFlow objects",
    ))
}

fn parse_request(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<QueryRequest> {
    let s = dumps(py, obj)?;
    serde_json::from_str(&s).map_err(py_err)
}

fn build_registry(tables: Vec<SemanticTable>, flows: Vec<CoreSemanticFlow>) -> FlowRegistry {
    FlowRegistry::from_parts(tables, flows)
}

fn build_data_sources(mapping: &Bound<'_, PyAny>) -> PyResult<ConnectionManager> {
    // Accept either: list[DataSource] or dict[name -> duckdb_path]
    if let Ok(list) = mapping.extract::<Vec<PyDataSource>>() {
        let mut ds = ConnectionManager::new();
        for item in list {
            let mut conn = DuckDbConnection::new(item.uri.clone());
            if let Some(max) = item.max_concurrency {
                conn = conn.with_max_concurrency(max);
            }
            ds.insert(item.name.clone(), Arc::new(conn));
        }
        return Ok(ds);
    }

    if let Ok(dict) = mapping.extract::<std::collections::HashMap<String, String>>() {
        let mut ds = ConnectionManager::new();
        for (name, path) in dict {
            ds.insert(name, Arc::new(DuckDbConnection::new(path)));
        }
        return Ok(ds);
    }

    Err(PyValueError::new_err(
        "data_sources must be dict[name -> path] or list[DataSource]",
    ))
}

fn serde_json_to_py(py: Python<'_>, value: &serde_json::Value) -> PyResult<PyObject> {
    let json = py.import_bound("json")?;
    let dumps = json.getattr("dumps")?;
    let loads = json.getattr("loads")?;
    let s: String = dumps
        .call1((serde_json::to_string(value).map_err(py_err)?,))?
        .extract()?;
    let obj = loads.call1((s,))?;
    Ok(obj.into_py(py))
}

#[pyfunction]
#[pyo3(text_signature = "(tables, flows, data_sources, request)")]
/// Build SQL for a request.
/// - `tables`: list/dict of semantic tables (shape matches Rust schema; strings allowed for column exprs)
/// - `flows`: list of semantic flows (flow definitions)
/// - `data_sources`: dict of name -> DuckDB database path
/// - `request`: query request dict
fn build_sql(
    py: Python<'_>,
    tables: &Bound<'_, PyAny>,
    flows: &Bound<'_, PyAny>,
    data_sources: &Bound<'_, PyAny>,
    request: &Bound<'_, PyAny>,
) -> PyResult<String> {
    let start = Instant::now();
    let tables = parse_tables(py, tables)?;
    let flows = parse_flows(flows)?;
    let request = parse_request(py, request)?;
    let registry = build_registry(tables, flows);
    let ds = build_data_sources(data_sources)?;
    let builder = SqlBuilder::default();
    let sql = py
        .allow_threads(|| builder.build_for_request(&registry, &ds, &request))
        .map_err(to_validation_err)?;
    tracing::debug!(
        ms = start.elapsed().as_millis(),
        "build_sql (pyfunction) complete"
    );
    Ok(sql)
}

#[pyfunction]
#[pyo3(text_signature = "(tables, flows, data_sources, request)")]
/// Validate, build SQL, execute against DuckDB, and return rows (list[dict]).
fn run(
    py: Python<'_>,
    tables: &Bound<'_, PyAny>,
    flows: &Bound<'_, PyAny>,
    data_sources: &Bound<'_, PyAny>,
    request: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let start = Instant::now();
    let tables = parse_tables(py, tables)?;
    let flows = parse_flows(flows)?;
    let request = parse_request(py, request)?;
    let mut registry = build_registry(tables, flows);
    let ds = build_data_sources(data_sources)?;
    let validator = Validator::new(ds.clone(), false);
    let rows_json: String = py
        .allow_threads(|| {
            runtime().block_on(async {
                validator
                    .validate_registry(&mut registry)
                    .await
                    .map_err(SemaflowError::from)?;
                let result = run_query(&registry, &ds, &request).await?;
                serde_json::to_string(&result.rows).map_err(SemaflowError::from)
            })
        })
        .map_err(to_validation_err)?;

    let json = py.import_bound("json")?;
    let py_obj = json.call_method1("loads", (rows_json,))?;
    tracing::debug!(
        ms = start.elapsed().as_millis(),
        "run (pyfunction) complete"
    );
    Ok(py_obj.into_py(py))
}

/// PyO3 module entrypoint
#[pymodule]
fn semaflow(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    init_tracing();
    m.add_class::<PyDataSource>()?;
    m.add_class::<PyTableHandle>()?;
    m.add_class::<PyJoinKey>()?;
    m.add_class::<PyFlowJoin>()?;
    m.add_class::<PyDimension>()?;
    m.add_class::<PyMeasure>()?;
    m.add_class::<PySemanticTable>()?;
    m.add_class::<PySemanticFlow>()?;
    m.add_function(wrap_pyfunction!(build_sql, m)?)?;
    m.add_function(wrap_pyfunction!(run, m)?)?;

    m.add_class::<SemanticFlowHandle>()?;
    Ok(())
}

#[pyclass(name = "SemanticFlowHandle", module = "semaflow.semaflow")]
#[derive(Clone)]
pub struct SemanticFlowHandle {
    registry: Arc<FlowRegistry>,
    connections: ConnectionManager,
}

#[pymethods]
impl SemanticFlowHandle {
    #[staticmethod]
    #[pyo3(text_signature = "(tables, flows, data_sources)")]
    fn from_parts(
        py: Python<'_>,
        tables: &Bound<'_, PyAny>,
        flows: &Bound<'_, PyAny>,
        data_sources: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let tables = parse_tables(py, tables)?;
        let flows_vec = parse_flows(flows)?;
        let mut registry = FlowRegistry::from_parts(tables, flows_vec);
        let connections = build_data_sources(data_sources)?;
        let validator = Validator::new(connections.clone(), false);
        py.allow_threads(|| {
            runtime().block_on(async { validator.validate_registry(&mut registry).await })
        })
        .map_err(to_validation_err)?;
        Ok(Self {
            registry: Arc::new(registry),
            connections,
        })
    }

    #[staticmethod]
    #[pyo3(text_signature = "(flow_dir, data_sources)")]
    fn from_dir(py: Python<'_>, flow_dir: &str, data_sources: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut registry = FlowRegistry::load_from_dir(flow_dir).map_err(to_validation_err)?;
        let connections = build_data_sources(data_sources)?;
        let validator = Validator::new(connections.clone(), false);
        py.allow_threads(|| {
            runtime().block_on(async { validator.validate_registry(&mut registry).await })
        })
        .map_err(to_validation_err)?;
        Ok(Self {
            registry: Arc::new(registry),
            connections,
        })
    }

    #[new]
    fn new(
        py: Python<'_>,
        tables: &Bound<'_, PyAny>,
        flows: &Bound<'_, PyAny>,
        data_sources: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let tables = parse_tables(py, tables)?;
        let flows_vec = parse_flows(flows)?;
        let mut registry = build_registry(tables, flows_vec);
        let connections = build_data_sources(data_sources)?;
        let validator = Validator::new(connections.clone(), false);
        py.allow_threads(|| {
            runtime().block_on(async { validator.validate_registry(&mut registry).await })
        })
        .map_err(to_validation_err)?;
        Ok(Self {
            registry: Arc::new(registry),
            connections,
        })
    }

    /// Build SQL for a request dict.
    #[pyo3(text_signature = "(self, request)")]
    fn build_sql(&self, py: Python<'_>, request: &Bound<'_, PyAny>) -> PyResult<String> {
        let start = Instant::now();
        let request = parse_request(py, request)?;
        let builder = SqlBuilder::default();
        let registry = self.registry.clone();
        let sql = py
            .allow_threads(|| builder.build_for_request(&registry, &self.connections, &request))
            .map_err(to_validation_err)?;
        tracing::debug!(ms = start.elapsed().as_millis(), "build_sql complete");
        Ok(sql)
    }

    /// Execute a request dict and return list[dict] rows.
    #[pyo3(text_signature = "(self, request)")]
    fn execute(&self, py: Python<'_>, request: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        let start = Instant::now();
        let request = parse_request(py, request)?;
        let registry = self.registry.clone();
        let connections = self.connections.clone();
        let rows_json: String = py
            .allow_threads(|| {
                runtime().block_on(async {
                    let result = run_query(&registry, &connections, &request)
                        .await
                        .map_err(SemaflowError::from)?;
                    serde_json::to_string(&result.rows).map_err(SemaflowError::from)
                })
            })
            .map_err(to_validation_err)?;
        let json = py.import_bound("json")?;
        let py_obj = json.call_method1("loads", (rows_json,))?;
        tracing::debug!(ms = start.elapsed().as_millis(), "execute complete");
        Ok(py_obj.into_py(py))
    }

    /// List flows with names/descriptions.
    #[pyo3(text_signature = "(self)")]
    fn list_flows(&self, py: Python<'_>) -> PyResult<PyObject> {
        let summaries = self.registry.list_flow_summaries();
        let py_list = PyList::empty_bound(py);
        for s in summaries {
            let dict = PyDict::new_bound(py);
            dict.set_item("name", s.name)?;
            if let Some(desc) = s.description {
                dict.set_item("description", desc)?;
            }
            py_list.append(dict)?;
        }
        Ok(py_list.into_py(py))
    }

    /// Get flow schema (dimensions, measures, joins) by name.
    #[pyo3(text_signature = "(self, name)")]
    fn get_flow(&self, py: Python<'_>, name: &str) -> PyResult<PyObject> {
        let schema = self.registry.flow_schema(name).map_err(to_validation_err)?;
        let dict = PyDict::new_bound(py);
        dict.set_item("name", schema.name)?;
        if let Some(desc) = schema.description {
            dict.set_item("description", desc)?;
        }
        dict.set_item("data_source", schema.data_source)?;
        if let Some(td) = schema.time_dimension {
            dict.set_item("time_dimension", td)?;
        }
        if let Some(grain) = schema.smallest_time_grain {
            dict.set_item("smallest_time_grain", grain)?;
        }
        let dims = PyList::empty_bound(py);
        for d in schema.dimensions {
            let dct = PyDict::new_bound(py);
            dct.set_item("name", d.name)?;
            dct.set_item("qualified_name", d.qualified_name)?;
            if let Some(desc) = d.description {
                dct.set_item("description", desc)?;
            }
            if let Some(dt) = d.data_type {
                dct.set_item("data_type", dt)?;
            }
            dct.set_item("semantic_table", d.semantic_table)?;
            dct.set_item("table_alias", d.table_alias)?;
            let expr_json = serde_json::to_value(&d.expr).map_err(py_err)?;
            let expr_py = serde_json_to_py(py, &expr_json)?;
            dct.set_item("expr", expr_py)?;
            dims.append(dct)?;
        }
        dict.set_item("dimensions", dims)?;

        let measures = PyList::empty_bound(py);
        for m in schema.measures {
            let dct = PyDict::new_bound(py);
            dct.set_item("name", m.name)?;
            dct.set_item("qualified_name", m.qualified_name)?;
            if let Some(desc) = m.description {
                dct.set_item("description", desc)?;
            }
            if let Some(dt) = m.data_type {
                dct.set_item("data_type", dt)?;
            }
            dct.set_item("semantic_table", m.semantic_table)?;
            dct.set_item("table_alias", m.table_alias)?;
            let expr_json = serde_json::to_value(&m.expr).map_err(py_err)?;
            let expr_py = serde_json_to_py(py, &expr_json)?;
            dct.set_item("expr", expr_py)?;
            dct.set_item("agg", format!("{:?}", m.agg))?;
            measures.append(dct)?;
        }
        dict.set_item("measures", measures)?;

        Ok(dict.into_py(py))
    }
}
