use std::path::{Path, PathBuf};

use async_trait::async_trait;
use duckdb::types::Value as DuckValue;
use duckdb::Connection;
use serde_json::{Map, Value};

use crate::error::{Result, SemaflowError};
use crate::schema_cache::{ForeignKey, TableSchema};

#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<ColumnMeta>,
    pub rows: Vec<Map<String, Value>>,
}

#[async_trait]
pub trait SchemaProvider: Send + Sync {
    async fn fetch_schema(&self, table: &str) -> Result<TableSchema>;
}

#[async_trait]
pub trait QueryExecutor: SchemaProvider + Send + Sync {
    async fn query(&self, sql: &str) -> Result<QueryResult>;
}

/// DuckDB adapter used for POC and tests.
#[derive(Debug, Clone)]
pub struct DuckDbExecutor {
    database_path: PathBuf,
}

impl DuckDbExecutor {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            database_path: path.as_ref().to_path_buf(),
        }
    }
}

#[async_trait]
impl SchemaProvider for DuckDbExecutor {
    async fn fetch_schema(&self, table: &str) -> Result<TableSchema> {
        let path = self.database_path.clone();
        let table = table.to_string();
        tokio::task::spawn_blocking(move || -> Result<TableSchema> {
            let conn = Connection::open(path)?;

            let pragma_sql = format!("PRAGMA table_info('{table}')");
            let mut stmt = conn.prepare(&pragma_sql)?;
            let mut rows = stmt.query([])?;
            let mut columns = Vec::new();
            let mut primary_keys = Vec::new();
            while let Some(row) = rows.next()? {
                let name: String = row.get("name")?;
                let data_type: String = row.get("type")?;
                let not_null: bool = row.get("notnull")?;
                let pk_flag: bool = row.get("pk")?;
                if pk_flag {
                    primary_keys.push(name.clone());
                }
                columns.push(crate::schema_cache::ColumnSchema {
                    name,
                    data_type,
                    nullable: !not_null,
                });
            }

            let mut foreign_keys = Vec::new();
            let fk_sql = format!("PRAGMA foreign_key_list('{table}')");
            if let Ok(mut fk_stmt) = conn.prepare(&fk_sql) {
                let mut fk_rows = fk_stmt.query([])?;
                while let Some(row) = fk_rows.next()? {
                    let from_column: String = row.get("from")?;
                    let to_table: String = row.get("table")?;
                    let to_column: String = row.get("to")?;
                    foreign_keys.push(ForeignKey {
                        from_column,
                        to_table,
                        to_column,
                    });
                }
            }

            Ok(TableSchema {
                columns,
                primary_keys,
                foreign_keys,
            })
        })
        .await
        .map_err(|e| SemaflowError::Execution(format!("task join error: {e}")))?
    }
}

#[async_trait]
impl QueryExecutor for DuckDbExecutor {
    async fn query(&self, sql: &str) -> Result<QueryResult> {
        let path = self.database_path.clone();
        let sql = sql.to_string();
        tokio::task::spawn_blocking(move || -> Result<QueryResult> {
            let conn = Connection::open(path)?;
            let mut stmt = conn.prepare(&sql)?;
            let mut rows_iter = stmt.query([])?;
            let stmt_ref = rows_iter
                .as_ref()
                .ok_or_else(|| SemaflowError::Execution("statement missing".to_string()))?;
            let mut column_names = Vec::new();
            for idx in 0..stmt_ref.column_count() {
                let name = stmt_ref
                    .column_name(idx)
                    .map_err(|e| SemaflowError::Execution(e.to_string()))?;
                column_names.push(name.to_string());
            }
            let mut rows = Vec::new();
            while let Some(row) = rows_iter.next()? {
                let mut map = Map::new();
                for (idx, name) in column_names.iter().enumerate() {
                    let value = duck_value_to_json(row.get_ref(idx)?.to_owned());
                    map.insert(name.clone(), value);
                }
                rows.push(map);
            }

            let columns = column_names
                .into_iter()
                .map(|name| ColumnMeta { name })
                .collect();
            Ok(QueryResult { columns, rows })
        })
        .await
        .map_err(|e| SemaflowError::Execution(format!("task join error: {e}")))?
    }
}

fn duck_value_to_json(value: DuckValue) -> Value {
    match value {
        DuckValue::Null => Value::Null,
        DuckValue::Boolean(b) => Value::Bool(b),
        DuckValue::TinyInt(i) => Value::from(i),
        DuckValue::SmallInt(i) => Value::from(i),
        DuckValue::Int(i) => Value::from(i),
        DuckValue::BigInt(i) => Value::from(i),
        DuckValue::HugeInt(i) => Value::String(i.to_string()),
        DuckValue::UTinyInt(i) => Value::from(i),
        DuckValue::USmallInt(i) => Value::from(i),
        DuckValue::UInt(i) => Value::from(i),
        DuckValue::UBigInt(i) => Value::from(i),
        DuckValue::Float(f) => Value::from(f),
        DuckValue::Double(f) => Value::from(f),
        DuckValue::Decimal(d) => Value::String(d.to_string()),
        DuckValue::Timestamp(unit, t) => Value::String(format!("{t} ({unit:?})")),
        DuckValue::Text(s) => Value::String(s),
        DuckValue::Blob(bytes) => Value::String(hex::encode(bytes)),
        DuckValue::Date32(d) => Value::from(d),
        DuckValue::Time64(unit, t) => Value::String(format!("{t} ({unit:?})")),
        DuckValue::Interval {
            months,
            days,
            nanos,
        } => Value::String(format!("{months} months {days} days {nanos} nanos")),
        DuckValue::List(items) => {
            let values = items.into_iter().map(duck_value_to_json).collect();
            Value::Array(values)
        }
        DuckValue::Enum(s) => Value::String(s),
        DuckValue::Struct(fields) => {
            let mut map = Map::new();
            for (key, val) in fields.iter() {
                map.insert(key.clone(), duck_value_to_json(val.clone()));
            }
            Value::Object(map)
        }
        DuckValue::Array(items) => {
            let values = items.into_iter().map(duck_value_to_json).collect();
            Value::Array(values)
        }
        DuckValue::Map(entries) => {
            let pairs: Vec<Value> = entries
                .iter()
                .map(|(k, v)| {
                    Value::Array(vec![
                        duck_value_to_json(k.clone()),
                        duck_value_to_json(v.clone()),
                    ])
                })
                .collect();
            Value::Array(pairs)
        }
        DuckValue::Union(inner) => duck_value_to_json(*inner),
    }
}
