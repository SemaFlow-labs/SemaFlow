//! Python bindings (PyO3) for SemaFlow core.

#[cfg(feature = "duckdb")]
use crate::backends::DuckDbConnection;
use crate::{
    backends::ConnectionManager,
    config::{BigQueryConfig, DatasourceConfig, DuckDbConfig, PostgresConfig, SemaflowConfig},
    flows::{
        Aggregation, Dimension, Expr, FlowJoin, FlowTableRef, SemanticFlow as CoreSemanticFlow,
        SemanticTable,
    },
    query_builder::SqlBuilder,
    registry::FlowRegistry,
    runtime::{run_query, run_query_paginated},
    validation::Validator,
    QueryRequest, SemaflowError,
};
#[cfg(feature = "duckdb")]
use arrow::array::RecordBatchReader;
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
    let json = py.import("json")?;
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
    #[pyo3(get)]
    pub backend_type: String,
    #[pyo3(get)]
    pub schema: Option<String>,

    /// Pre-created DuckDB connection (used when register_dataframe is called).
    /// This allows the connection to be reused in build_data_sources().
    #[cfg(feature = "duckdb")]
    pub(crate) duckdb_conn: Option<Arc<DuckDbConnection>>,
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
            backend_type: "duckdb".to_string(),
            schema: None,
            #[cfg(feature = "duckdb")]
            duckdb_conn: None,
        }
    }

    /// Create a DuckDB data source.
    ///
    /// Args:
    ///     path: Path to DuckDB database file, or `:memory:` for an in-memory database.
    ///           Note: In-memory databases cannot be shared with Python's `duckdb` package
    ///           since they use separate library instances. Use a file path if you need
    ///           to pre-populate data with Python duckdb before querying via SemaFlow.
    ///     name: Optional data source name (defaults to "duckdb")
    ///     max_concurrency: Optional max concurrent queries
    #[staticmethod]
    #[pyo3(signature = (path, name=None, max_concurrency=None))]
    fn duckdb(path: String, name: Option<String>, max_concurrency: Option<usize>) -> Self {
        Self {
            name: name.unwrap_or_else(|| "duckdb".to_string()),
            uri: path,
            max_concurrency,
            backend_type: "duckdb".to_string(),
            schema: None,
            #[cfg(feature = "duckdb")]
            duckdb_conn: None,
        }
    }

    /// Create a PostgreSQL data source.
    ///
    /// Args:
    ///     connection_string: PostgreSQL connection string (URL or key-value format)
    ///     schema: PostgreSQL schema name (e.g., "public")
    ///     name: Optional data source name (defaults to "postgres")
    ///     max_concurrency: Optional max pool size
    #[staticmethod]
    #[pyo3(signature = (connection_string, schema, name=None, max_concurrency=None))]
    fn postgres(
        connection_string: String,
        schema: String,
        name: Option<String>,
        max_concurrency: Option<usize>,
    ) -> Self {
        Self {
            name: name.unwrap_or_else(|| "postgres".to_string()),
            uri: connection_string,
            max_concurrency,
            backend_type: "postgres".to_string(),
            schema: Some(schema),
            #[cfg(feature = "duckdb")]
            duckdb_conn: None,
        }
    }

    /// Create a BigQuery data source.
    ///
    /// Args:
    ///     project_id: GCP project ID
    ///     dataset: BigQuery dataset name
    ///     service_account_path: Optional path to service account JSON key file.
    ///                           If not provided, uses application default credentials.
    ///     name: Optional data source name (defaults to "bigquery")
    #[staticmethod]
    #[pyo3(signature = (project_id, dataset, service_account_path=None, name=None))]
    fn bigquery(
        project_id: String,
        dataset: String,
        service_account_path: Option<String>,
        name: Option<String>,
    ) -> Self {
        // Store project_id|dataset|service_account_path in URI for later parsing
        let uri = match service_account_path {
            Some(path) => format!("{}|{}|{}", project_id, dataset, path),
            None => format!("{}|{}", project_id, dataset),
        };
        Self {
            name: name.unwrap_or_else(|| "bigquery".to_string()),
            uri,
            max_concurrency: None,
            backend_type: "bigquery".to_string(),
            schema: Some(dataset),
            #[cfg(feature = "duckdb")]
            duckdb_conn: None,
        }
    }

    fn table(&self, name: String) -> PyTableHandle {
        PyTableHandle {
            data_source: self.name.clone(),
            table: name,
        }
    }

    /// Register a DataFrame (passed as Arrow) as a table in this data source.
    ///
    /// This method enables in-memory DuckDB databases to be populated with data
    /// from pandas, polars, or any Arrow-compatible library via zero-copy.
    ///
    /// Args:
    ///     table_name: Name for the table in the database
    ///     data: Arrow RecordBatchReader (e.g., `pa.Table.from_pandas(df).to_reader()`)
    ///
    /// Example:
    ///     ```python
    ///     import pyarrow as pa
    ///     ds = DataSource.duckdb(":memory:", name="test")
    ///     df = pd.DataFrame({"id": [1, 2], "amount": [100.0, 200.0]})
    ///     ds.register_dataframe("orders", pa.Table.from_pandas(df).to_reader())
    ///     ```
    #[cfg(feature = "duckdb")]
    #[pyo3(signature = (table_name, data))]
    fn register_dataframe(
        &mut self,
        table_name: String,
        data: arrow::pyarrow::PyArrowType<arrow::ffi_stream::ArrowArrayStreamReader>,
    ) -> PyResult<()> {
        use crate::config::DuckDbConfig;

        // Ensure this is a DuckDB data source
        if self.backend_type != "duckdb" {
            return Err(PyValueError::new_err(
                "register_dataframe is only supported for DuckDB data sources",
            ));
        }

        // Get or create connection eagerly
        let conn = match &self.duckdb_conn {
            Some(c) => c.clone(),
            None => {
                let config = DuckDbConfig {
                    max_concurrency: self.max_concurrency.unwrap_or(4),
                };
                let new_conn = Arc::new(DuckDbConnection::with_config(&self.uri, config));
                self.duckdb_conn = Some(new_conn.clone());
                new_conn
            }
        };

        // Extract Arrow reader and collect batches
        let reader = data.0;
        let schema = reader.schema();
        let batches: Vec<arrow::array::RecordBatch> = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| PyRuntimeError::new_err(format!("failed to read Arrow batches: {e}")))?;

        // Register the table
        runtime()
            .block_on(conn.register_arrow_table(&table_name, &schema, batches))
            .map_err(|e| PyRuntimeError::new_err(format!("failed to register table: {e}")))?;

        Ok(())
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
    #[pyo3(signature = (expr, data_type=None, description=None))]
    fn new(
        py: Python<'_>,
        expr: &Bound<'_, PyAny>,
        data_type: Option<String>,
        description: Option<String>,
    ) -> PyResult<Self> {
        let expr = expr_from_py(py, expr)?;
        Ok(Self {
            inner: Dimension {
                expr,
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
    #[pyo3(signature = (expr, agg, data_type=None, description=None, filter=None, post_expr=None))]
    fn new(
        py: Python<'_>,
        expr: &Bound<'_, PyAny>,
        agg: &str,
        data_type: Option<String>,
        description: Option<String>,
        filter: Option<&Bound<'_, PyAny>>,
        post_expr: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let expr = expr_from_py(py, expr)?;
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
                expr: Some(expr),
                agg: Some(agg_enum),
                formula: None,
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
                cardinality: None,
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
    #[pyo3(signature = (name, data_source, table, primary_key=None, primary_keys=None, time_dimension=None, dimensions=None, measures=None, description=None))]
    fn new(
        py: Python<'_>,
        name: String,
        data_source: &Bound<'_, PyAny>,
        table: String,
        primary_key: Option<String>,
        primary_keys: Option<Vec<String>>,
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

        // Support both primary_key (single) and primary_keys (composite)
        let pks = match (primary_keys, primary_key) {
            (Some(keys), _) => keys,
            (None, Some(key)) => vec![key],
            (None, None) => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "either primary_key or primary_keys must be specified",
                ))
            }
        };

        Ok(Self {
            inner: SemanticTable {
                name,
                data_source: ds_name,
                table,
                primary_keys: pks,
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
    #[pyo3(signature = (name, table_handle, primary_key=None, primary_keys=None, time_dimension=None, dimensions=None, measures=None, description=None))]
    fn from_table(
        py: Python<'_>,
        name: String,
        table_handle: PyTableHandle,
        primary_key: Option<String>,
        primary_keys: Option<Vec<String>>,
        time_dimension: Option<String>,
        dimensions: Option<&Bound<'_, PyAny>>,
        measures: Option<&Bound<'_, PyAny>>,
        description: Option<String>,
    ) -> PyResult<Self> {
        let data_source_obj = pyo3::types::PyString::new(py, &table_handle.data_source);
        Self::new(
            py,
            name,
            &data_source_obj,
            table_handle.table,
            primary_key,
            primary_keys,
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

fn build_data_sources(
    mapping: &Bound<'_, PyAny>,
    config: Option<&SemaflowConfig>,
) -> PyResult<ConnectionManager> {
    // Auto-load config from default locations if not provided
    let auto_config = config.cloned().unwrap_or_else(SemaflowConfig::load_default);

    // Accept either: list[DataSource] or dict[name -> duckdb_path]
    if let Ok(list) = mapping.extract::<Vec<PyDataSource>>() {
        let mut ds = ConnectionManager::with_config(auto_config.clone());
        for item in list {
            // Get resolved config for this datasource
            let resolved = ds.config_for(&item.name);

            match item.backend_type.as_str() {
                #[cfg(feature = "duckdb")]
                "duckdb" => {
                    // Check if connection was pre-created (via register_dataframe)
                    if let Some(existing_conn) = item.duckdb_conn.clone() {
                        tracing::debug!(
                            name = item.name.as_str(),
                            "reusing pre-created DuckDB connection"
                        );
                        ds.insert(item.name.clone(), existing_conn);
                    } else {
                        // Create new connection
                        let duck_config = crate::config::DuckDbConfig {
                            max_concurrency: item
                                .max_concurrency
                                .unwrap_or(resolved.duckdb.max_concurrency),
                        };
                        let conn = DuckDbConnection::with_config(item.uri.clone(), duck_config);
                        // Initialize pool so checkout_connection works
                        // (especially important for :memory: databases)
                        runtime().block_on(conn.initialize_pool()).map_err(py_err)?;
                        ds.insert(item.name.clone(), Arc::new(conn));
                    }
                }
                #[cfg(not(feature = "duckdb"))]
                "duckdb" => {
                    return Err(PyValueError::new_err(
                        "DuckDB support not enabled. Rebuild with --features duckdb",
                    ));
                }
                #[cfg(feature = "postgres")]
                "postgres" => {
                    use crate::backends::PostgresConnection;
                    let schema = item.schema.as_deref().ok_or_else(|| {
                        PyValueError::new_err("postgres data source requires schema parameter")
                    })?;
                    // Use pool_size from: PyDataSource param > config > default
                    let pg_config = crate::config::PostgresConfig {
                        pool_size: item.max_concurrency.unwrap_or(resolved.postgres.pool_size),
                        statement_timeout_ms: resolved.postgres.statement_timeout_ms,
                    };
                    let conn = PostgresConnection::with_config(&item.uri, schema, pg_config)
                        .map_err(py_err)?;
                    ds.insert(item.name.clone(), Arc::new(conn));
                }
                #[cfg(not(feature = "postgres"))]
                "postgres" => {
                    return Err(PyValueError::new_err(
                        "PostgreSQL support not enabled. Rebuild with --features postgres",
                    ));
                }
                #[cfg(feature = "bigquery")]
                "bigquery" => {
                    use crate::backends::BigQueryConnection;
                    // Parse URI format: project_id|dataset|optional_service_account_path
                    let parts: Vec<&str> = item.uri.split('|').collect();
                    if parts.len() < 2 {
                        return Err(PyValueError::new_err(
                            "BigQuery URI must contain project_id|dataset",
                        ));
                    }
                    let project_id = parts[0];
                    let dataset = parts[1];

                    // Use config from resolved datasource config
                    let bq_config = resolved.bigquery.clone();

                    let conn = if parts.len() >= 3 && !parts[2].is_empty() {
                        // Service account key file provided
                        runtime()
                            .block_on(
                                BigQueryConnection::from_service_account_key_file_with_config(
                                    parts[2], project_id, dataset, bq_config,
                                ),
                            )
                            .map_err(py_err)?
                    } else {
                        // Use application default credentials
                        runtime()
                            .block_on(BigQueryConnection::from_application_default_credentials_with_config(
                                project_id, dataset, bq_config,
                            ))
                            .map_err(py_err)?
                    };
                    ds.insert(item.name.clone(), Arc::new(conn));
                }
                #[cfg(not(feature = "bigquery"))]
                "bigquery" => {
                    return Err(PyValueError::new_err(
                        "BigQuery support not enabled. Rebuild with --features bigquery",
                    ));
                }
                other => {
                    return Err(PyValueError::new_err(format!(
                        "unknown backend_type: {other}. Supported: duckdb, postgres, bigquery"
                    )));
                }
            }
        }
        return Ok(ds);
    }

    // Legacy dict format: name -> duckdb_path (only when duckdb feature enabled)
    #[cfg(feature = "duckdb")]
    if let Ok(dict) = mapping.extract::<std::collections::HashMap<String, String>>() {
        let mut ds = ConnectionManager::with_config(auto_config);
        for (name, path) in dict {
            let resolved = ds.config_for(&name);
            let duck_config = crate::config::DuckDbConfig {
                max_concurrency: resolved.duckdb.max_concurrency,
            };
            ds.insert(
                name,
                Arc::new(DuckDbConnection::with_config(path, duck_config)),
            );
        }
        return Ok(ds);
    }

    #[cfg(not(feature = "duckdb"))]
    if mapping
        .extract::<std::collections::HashMap<String, String>>()
        .is_ok()
    {
        return Err(PyValueError::new_err(
            "dict[name -> path] format requires DuckDB. Rebuild with --features duckdb or use list[DataSource]",
        ));
    }

    Err(PyValueError::new_err(
        "data_sources must be list[DataSource] or dict[name -> duckdb_path]",
    ))
}

fn serde_json_to_py(py: Python<'_>, value: &serde_json::Value) -> PyResult<PyObject> {
    let json = py.import("json")?;
    let dumps = json.getattr("dumps")?;
    let loads = json.getattr("loads")?;
    let s: String = dumps
        .call1((serde_json::to_string(value).map_err(py_err)?,))?
        .extract()?;
    let obj = loads.call1((s,))?;
    Ok(obj.unbind())
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
    let ds = build_data_sources(data_sources, None)?;
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
    let ds = build_data_sources(data_sources, None)?;
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

    let json = py.import("json")?;
    let py_obj = json.call_method1("loads", (rows_json,))?;
    tracing::debug!(
        ms = start.elapsed().as_millis(),
        "run (pyfunction) complete"
    );
    Ok(py_obj.unbind())
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
    m.add_class::<PyConfig>()?;
    Ok(())
}

/// Python wrapper for SemaflowConfig.
///
/// Allows programmatic configuration of query timeouts, pool sizes,
/// schema cache settings, and per-datasource options.
#[pyclass(name = "Config", module = "semaflow.semaflow")]
#[derive(Clone)]
pub struct PyConfig {
    inner: SemaflowConfig,
}

#[pymethods]
impl PyConfig {
    /// Create a new empty config with defaults.
    #[new]
    fn new() -> Self {
        Self {
            inner: SemaflowConfig::default(),
        }
    }

    /// Load config from default locations.
    ///
    /// Searches in order:
    /// 1. SEMAFLOW_CONFIG environment variable
    /// 2. ./semaflow.toml (current directory)
    /// 3. ~/.config/semaflow/config.toml (user config)
    ///
    /// Returns default config if no file found.
    #[staticmethod]
    fn load() -> Self {
        Self {
            inner: SemaflowConfig::load_default(),
        }
    }

    /// Load config from a specific file path.
    #[staticmethod]
    fn from_file(path: &str) -> PyResult<Self> {
        let config = SemaflowConfig::from_file(path).map_err(py_err)?;
        Ok(Self { inner: config })
    }

    /// Parse config from a TOML string.
    #[staticmethod]
    fn from_toml(toml_str: &str) -> PyResult<Self> {
        let config = SemaflowConfig::from_toml(toml_str).map_err(py_err)?;
        Ok(Self { inner: config })
    }

    /// Set the default query timeout in milliseconds.
    fn set_query_timeout_ms(&mut self, timeout_ms: u64) {
        self.inner.defaults.query.timeout_ms = timeout_ms;
    }

    /// Set the maximum row limit for queries (0 = unlimited).
    fn set_max_row_limit(&mut self, limit: u64) {
        self.inner.defaults.query.max_row_limit = limit;
    }

    /// Set the default row limit for queries.
    fn set_default_row_limit(&mut self, limit: u64) {
        self.inner.defaults.query.default_row_limit = limit;
    }

    /// Set the default connection pool size.
    fn set_pool_size(&mut self, size: usize) {
        self.inner.defaults.pool.size = size;
    }

    /// Set the pool idle timeout in seconds.
    fn set_pool_idle_timeout_secs(&mut self, secs: u64) {
        self.inner.defaults.pool.idle_timeout_secs = secs;
    }

    /// Set the schema cache TTL in seconds.
    fn set_schema_cache_ttl_secs(&mut self, secs: u64) {
        self.inner.defaults.schema_cache.ttl_secs = secs;
    }

    /// Set the maximum schema cache size.
    fn set_schema_cache_max_size(&mut self, size: usize) {
        self.inner.defaults.schema_cache.max_size = size;
    }

    /// Set validation to warn-only mode.
    fn set_validation_warn_only(&mut self, warn_only: bool) {
        self.inner.defaults.validation.warn_only = warn_only;
    }

    /// Configure BigQuery settings for a specific datasource.
    ///
    /// Args:
    ///     datasource_name: Name of the datasource
    ///     use_query_cache: Whether to use BigQuery's query cache (default: true)
    ///     maximum_bytes_billed: Maximum bytes billed per query (0 = unlimited)
    ///     query_timeout_ms: Query timeout in milliseconds (0 = use default)
    #[pyo3(signature = (datasource_name, use_query_cache=None, maximum_bytes_billed=None, query_timeout_ms=None))]
    fn set_bigquery_config(
        &mut self,
        datasource_name: &str,
        use_query_cache: Option<bool>,
        maximum_bytes_billed: Option<i64>,
        query_timeout_ms: Option<u64>,
    ) {
        let ds_config = self
            .inner
            .datasources
            .entry(datasource_name.to_string())
            .or_insert_with(DatasourceConfig::default);

        let bq = ds_config
            .bigquery
            .get_or_insert_with(BigQueryConfig::default);
        if let Some(cache) = use_query_cache {
            bq.use_query_cache = cache;
        }
        if let Some(bytes) = maximum_bytes_billed {
            bq.maximum_bytes_billed = bytes;
        }
        if let Some(timeout) = query_timeout_ms {
            bq.query_timeout_ms = timeout;
        }
    }

    /// Configure DuckDB settings for a specific datasource.
    ///
    /// Args:
    ///     datasource_name: Name of the datasource
    ///     max_concurrency: Maximum concurrent queries
    #[pyo3(signature = (datasource_name, max_concurrency=None))]
    fn set_duckdb_config(&mut self, datasource_name: &str, max_concurrency: Option<usize>) {
        let ds_config = self
            .inner
            .datasources
            .entry(datasource_name.to_string())
            .or_insert_with(DatasourceConfig::default);

        let duck = ds_config.duckdb.get_or_insert_with(DuckDbConfig::default);
        if let Some(max) = max_concurrency {
            duck.max_concurrency = max;
        }
    }

    /// Configure PostgreSQL settings for a specific datasource.
    ///
    /// Args:
    ///     datasource_name: Name of the datasource
    ///     pool_size: Connection pool size
    ///     statement_timeout_ms: Statement timeout in milliseconds
    #[pyo3(signature = (datasource_name, pool_size=None, statement_timeout_ms=None))]
    fn set_postgres_config(
        &mut self,
        datasource_name: &str,
        pool_size: Option<usize>,
        statement_timeout_ms: Option<u64>,
    ) {
        let ds_config = self
            .inner
            .datasources
            .entry(datasource_name.to_string())
            .or_insert_with(DatasourceConfig::default);

        let pg = ds_config
            .postgres
            .get_or_insert_with(PostgresConfig::default);
        if let Some(size) = pool_size {
            pg.pool_size = size;
        }
        if let Some(timeout) = statement_timeout_ms {
            pg.statement_timeout_ms = timeout;
        }
    }
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
    #[pyo3(signature = (tables, flows, data_sources, config=None))]
    fn from_parts(
        py: Python<'_>,
        tables: &Bound<'_, PyAny>,
        flows: &Bound<'_, PyAny>,
        data_sources: &Bound<'_, PyAny>,
        config: Option<PyConfig>,
    ) -> PyResult<Self> {
        let tables = parse_tables(py, tables)?;
        let flows_vec = parse_flows(flows)?;
        let mut registry = FlowRegistry::from_parts(tables, flows_vec);
        let cfg = config.as_ref().map(|c| &c.inner);
        let connections = build_data_sources(data_sources, cfg)?;
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
    #[pyo3(signature = (flow_dir, data_sources, config=None))]
    fn from_dir(
        py: Python<'_>,
        flow_dir: &str,
        data_sources: &Bound<'_, PyAny>,
        config: Option<PyConfig>,
    ) -> PyResult<Self> {
        let mut registry = FlowRegistry::load_from_dir(flow_dir).map_err(to_validation_err)?;
        let cfg = config.as_ref().map(|c| &c.inner);
        let connections = build_data_sources(data_sources, cfg)?;
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
    #[pyo3(signature = (tables, flows, data_sources, config=None))]
    fn new(
        py: Python<'_>,
        tables: &Bound<'_, PyAny>,
        flows: &Bound<'_, PyAny>,
        data_sources: &Bound<'_, PyAny>,
        config: Option<PyConfig>,
    ) -> PyResult<Self> {
        let tables = parse_tables(py, tables)?;
        let flows_vec = parse_flows(flows)?;
        let mut registry = build_registry(tables, flows_vec);
        let cfg = config.as_ref().map(|c| &c.inner);
        let connections = build_data_sources(data_sources, cfg)?;
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

    /// Execute a request dict and return results.
    ///
    /// If `page_size` is set in the request, returns a dict with pagination metadata:
    /// - `rows`: list of row dicts for this page
    /// - `cursor`: opaque cursor string for next page (None if last page)
    /// - `has_more`: whether more rows exist after this page
    /// - `total_rows`: total result count (BigQuery only, None for other backends)
    ///
    /// If `page_size` is not set, returns list[dict] rows directly (backwards compatible).
    #[pyo3(text_signature = "(self, request)")]
    fn execute(&self, py: Python<'_>, request: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        let start = Instant::now();
        let request = parse_request(py, request)?;
        let registry = self.registry.clone();
        let connections = self.connections.clone();

        // Check if pagination is enabled
        if request.page_size.is_some() {
            // Paginated execution - return dict with metadata
            let result_json: String = py
                .allow_threads(|| {
                    runtime().block_on(async {
                        let result = run_query_paginated(&registry, &connections, &request)
                            .await
                            .map_err(SemaflowError::from)?;
                        // Serialize the full paginated result
                        let response = serde_json::json!({
                            "rows": result.rows,
                            "cursor": result.cursor,
                            "has_more": result.has_more,
                            "total_rows": result.total_rows,
                        });
                        serde_json::to_string(&response).map_err(SemaflowError::from)
                    })
                })
                .map_err(to_validation_err)?;
            let json = py.import("json")?;
            let py_obj = json.call_method1("loads", (result_json,))?;
            tracing::debug!(
                ms = start.elapsed().as_millis(),
                paginated = true,
                "execute complete"
            );
            Ok(py_obj.unbind())
        } else {
            // Non-paginated execution - return just rows (backwards compatible)
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
            let json = py.import("json")?;
            let py_obj = json.call_method1("loads", (rows_json,))?;
            tracing::debug!(
                ms = start.elapsed().as_millis(),
                paginated = false,
                "execute complete"
            );
            Ok(py_obj.unbind())
        }
    }

    /// List flows with names/descriptions.
    #[pyo3(text_signature = "(self)")]
    fn list_flows(&self, py: Python<'_>) -> PyResult<PyObject> {
        let summaries = self.registry.list_flow_summaries();
        let py_list = PyList::empty(py);
        for s in summaries {
            let dict = PyDict::new(py);
            dict.set_item("name", s.name)?;
            if let Some(desc) = s.description {
                dict.set_item("description", desc)?;
            }
            py_list.append(dict)?;
        }
        Ok(py_list.unbind().into())
    }

    /// Get flow schema (dimensions, measures, joins) by name.
    #[pyo3(text_signature = "(self, name)")]
    fn get_flow(&self, py: Python<'_>, name: &str) -> PyResult<PyObject> {
        let schema = self.registry.flow_schema(name).map_err(to_validation_err)?;
        let dict = PyDict::new(py);
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
        let dims = PyList::empty(py);
        for d in schema.dimensions {
            let dct = PyDict::new(py);
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

        let measures = PyList::empty(py);
        for m in schema.measures {
            let dct = PyDict::new(py);
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

        Ok(dict.unbind().into())
    }
}
