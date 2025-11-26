use std::collections::HashSet;
use std::sync::Mutex;

use crate::data_sources::DataSourceRegistry;
use crate::error::{Result, SemaflowError};
use crate::models::{Expr, SemanticModel, SemanticTable};
use crate::registry::ModelRegistry;
use crate::schema_cache::{SchemaCache, TableSchema};

pub struct Validator {
    data_sources: DataSourceRegistry,
    cache: Mutex<SchemaCache>,
    warn_only: bool,
}

impl Validator {
    pub fn new(data_sources: DataSourceRegistry, warn_only: bool) -> Self {
        Self {
            data_sources,
            cache: Mutex::new(SchemaCache::new()),
            warn_only,
        }
    }

    pub async fn validate_registry(&self, registry: &mut ModelRegistry) -> Result<()> {
        for table in registry.tables.values() {
            let schema = self.ensure_schema(&table.data_source, &table.table).await?;
            self.validate_table(table, schema)?;
        }

        for model in registry.models.values() {
            self.validate_model(model, registry)?;
        }

        Ok(())
    }

    async fn ensure_schema(&self, data_source: &str, table: &str) -> Result<TableSchema> {
        if let Some(schema) = self.cache.lock().unwrap().get(data_source, table).cloned() {
            return Ok(schema);
        }
        let provider = self
            .data_sources
            .get(data_source)
            .ok_or_else(|| SemaflowError::Validation(format!("unknown data source {data_source}")))?
            .executor
            .clone();
        let schema = provider.fetch_schema(table).await?;
        self.cache.lock().unwrap().insert(
            data_source.to_string(),
            table.to_string(),
            schema.clone(),
        );
        Ok(schema)
    }

    fn validate_table(&self, table: &SemanticTable, schema: TableSchema) -> Result<()> {
        let column_names: HashSet<_> = schema.columns.iter().map(|c| c.name.clone()).collect();

        self.check(
            column_names.contains(&table.primary_key),
            format!(
                "primary key {} missing on table {}",
                table.primary_key, table.name
            ),
        )?;

        for (name, dim) in &table.dimensions {
            if let Some(col) = simple_column_name(&dim.expression) {
                self.check(
                    column_names.contains(col),
                    format!("dimension {name} references missing column {col}"),
                )?;
            }
        }

        for (name, measure) in &table.measures {
            if let Some(col) = simple_column_name(&measure.expr) {
                self.check(
                    column_names.contains(col),
                    format!("measure {name} references missing column {col}"),
                )?;
            }
        }

        if let Some(time_dim) = &table.time_dimension {
            self.check(
                column_names.contains(time_dim),
                format!("time_dimension {time_dim} missing in table {}", table.name),
            )?;
        }

        Ok(())
    }

    fn validate_model(&self, model: &SemanticModel, registry: &ModelRegistry) -> Result<()> {
        let base_table = registry
            .get_table(&model.base_table.semantic_table)
            .ok_or_else(|| {
                SemaflowError::Validation(format!(
                    "model {} base table {} not found",
                    model.name, model.base_table.semantic_table
                ))
            })?;

        let base_ds = &base_table.data_source;

        let mut aliases = HashSet::new();
        aliases.insert(model.base_table.alias.clone());
        let mut alias_to_table = std::collections::HashMap::new();
        alias_to_table.insert(model.base_table.alias.clone(), base_table);

        for (join_name, join) in &model.joins {
            self.check(
                aliases.insert(join.alias.clone()),
                format!("duplicate alias {} in join {join_name}", join.alias),
            )?;
            let join_table = registry.get_table(&join.semantic_table).ok_or_else(|| {
                SemaflowError::Validation(format!(
                    "join {} references missing table {}",
                    join_name, join.semantic_table
                ))
            })?;

            self.check(
                aliases.contains(&join.to_table),
                format!("join {join_name} targets unknown alias {}", join.to_table),
            )?;

            self.check(
                join_table.data_source == *base_ds,
                format!(
                    "join {join_name} mixes data sources ({}) with base ({})",
                    join_table.data_source, base_ds
                ),
            )?;

            alias_to_table.insert(join.alias.clone(), join_table);
        }

        for (join_name, join) in &model.joins {
            self.check(
                !join.join_keys.is_empty(),
                format!("join {join_name} must include at least one join key"),
            )?;

            let right_table = alias_to_table.get(&join.alias).ok_or_else(|| {
                SemaflowError::Validation(format!("join {join_name} missing alias {}", join.alias))
            })?;
            let left_table = alias_to_table.get(&join.to_table).ok_or_else(|| {
                SemaflowError::Validation(format!(
                    "join {join_name} references unknown to_table {}",
                    join.to_table
                ))
            })?;

            for key in &join.join_keys {
                self.check(
                    table_has_column(left_table, &key.left),
                    format!(
                        "join {join_name} left key {} not found on table {}",
                        key.left, left_table.name
                    ),
                )?;
                self.check(
                    table_has_column(right_table, &key.right),
                    format!(
                        "join {join_name} right key {} not found on table {}",
                        key.right, right_table.name
                    ),
                )?;
            }
        }
        Ok(())
    }

    fn check(&self, condition: bool, message: String) -> Result<()> {
        if condition {
            return Ok(());
        }
        if self.warn_only {
            eprintln!("[warn] {}", message);
            Ok(())
        } else {
            Err(SemaflowError::Validation(message))
        }
    }
}

fn simple_column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column { column } => Some(column.as_str()),
        _ => None,
    }
}

fn table_has_column(table: &SemanticTable, col: &str) -> bool {
    if table.primary_key == col {
        return true;
    }
    if let Some(td) = &table.time_dimension {
        if td == col {
            return true;
        }
    }
    table
        .dimensions
        .values()
        .any(|d| simple_column_name(&d.expression) == Some(col))
}
