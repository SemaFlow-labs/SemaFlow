#[cfg(feature = "duckdb")]
use duckdb::types::Value as DuckValue;
use serde_json::{Map, Value};

#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<ColumnMeta>,
    pub rows: Vec<Map<String, Value>>,
}

#[cfg(feature = "duckdb")]
pub(crate) fn duck_value_to_json(value: DuckValue) -> Value {
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
