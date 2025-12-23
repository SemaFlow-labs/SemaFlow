use std::collections::HashSet;
use std::sync::Mutex;

use anyhow::anyhow;

use crate::backends::ConnectionManager;
use crate::error::{Result, SemaflowError};
use crate::expr_parser::parse_formula;
use crate::expr_utils::{collect_column_refs, collect_measure_refs, simple_column_name};
use crate::flows::{FormulaAst, SemanticFlow, SemanticTable};
use crate::registry::FlowRegistry;
use crate::schema_cache::{SchemaCache, TableSchema};

pub struct Validator {
    connections: ConnectionManager,
    cache: Mutex<SchemaCache>,
    warn_only: bool,
}

impl Validator {
    pub fn new(connections: ConnectionManager, warn_only: bool) -> Self {
        Self {
            connections,
            cache: Mutex::new(SchemaCache::new()),
            warn_only,
        }
    }

    #[tracing::instrument(skip(self, registry), fields(tables = registry.tables.len(), flows = registry.flows.len()))]
    pub async fn validate_registry(&self, registry: &mut FlowRegistry) -> Result<()> {
        let start = std::time::Instant::now();
        tracing::info!("starting registry validation");

        for table in registry.tables.values() {
            tracing::debug!(table = %table.name, "validating table");
            let schema = self.ensure_schema(&table.data_source, &table.table).await?;
            self.validate_table(table, schema)?;
        }

        for flow in registry.flows.values() {
            tracing::debug!(flow = %flow.name, "validating flow");
            self.validate_flow(flow, registry)?;
        }

        tracing::info!(
            tables = registry.tables.len(),
            flows = registry.flows.len(),
            ms = start.elapsed().as_millis(),
            "registry validation complete"
        );
        Ok(())
    }

    async fn ensure_schema(&self, data_source: &str, table: &str) -> Result<TableSchema> {
        if let Some(schema) = self
            .cache
            .lock()
            .map_err(|e| SemaflowError::Other(anyhow!("schema cache lock: {e}")))?
            .get(data_source, table)
            .cloned()
        {
            tracing::debug!(data_source = %data_source, table = %table, "schema cache hit");
            return Ok(schema);
        }

        tracing::debug!(data_source = %data_source, table = %table, "schema cache miss, fetching from backend");
        let provider = self.connections.get(data_source).ok_or_else(|| {
            tracing::warn!(data_source = %data_source, "unknown data source");
            SemaflowError::Validation(format!("unknown data source {data_source}"))
        })?;

        let start = std::time::Instant::now();
        let schema = provider.fetch_schema(table).await?;
        tracing::debug!(
            data_source = %data_source,
            table = %table,
            columns = schema.columns.len(),
            ms = start.elapsed().as_millis(),
            "schema fetched from backend"
        );

        self.cache
            .lock()
            .map_err(|e| SemaflowError::Other(anyhow!("schema cache lock: {e}")))?
            .insert(data_source.to_string(), table.to_string(), schema.clone());
        Ok(schema)
    }

    fn validate_table(&self, table: &SemanticTable, schema: TableSchema) -> Result<()> {
        let column_names: HashSet<_> = schema.columns.iter().map(|c| c.name.clone()).collect();

        for pk in &table.primary_keys {
            self.check(
                column_names.contains(pk),
                format!("primary key column {} missing on table {}", pk, table.name),
            )?;
        }

        for (name, dim) in &table.dimensions {
            // Walk the entire expression tree to validate all column references
            let mut col_refs = Vec::new();
            collect_column_refs(&dim.expr, &mut col_refs);
            for col in col_refs {
                self.check(
                    column_names.contains(&col),
                    format!("dimension {name} references missing column {col}"),
                )?;
            }
        }

        // Collect all measure names to identify measure refs in formulas
        let measure_names: HashSet<_> = table.measures.keys().cloned().collect();
        // Identify which measures are formula-based (for reference validation)
        let formula_measures: HashSet<_> = table
            .measures
            .iter()
            .filter(|(_, m)| m.is_formula())
            .map(|(n, _)| n.clone())
            .collect();

        for (name, measure) in &table.measures {
            // For simple measures, validate all column references in expr
            if let Some(expr) = &measure.expr {
                let mut col_refs = Vec::new();
                collect_column_refs(expr, &mut col_refs);
                for col in col_refs {
                    self.check(
                        column_names.contains(&col),
                        format!("measure {name} references missing column {col}"),
                    )?;
                }
            }

            // Validate filter expression if present
            if let Some(filter) = &measure.filter {
                let mut col_refs = Vec::new();
                collect_column_refs(filter, &mut col_refs);
                for col in col_refs {
                    self.check(
                        column_names.contains(&col),
                        format!("measure {name} filter references missing column {col}"),
                    )?;
                }
            }

            // For formula measures, validate the formula
            if let Some(formula) = &measure.formula {
                // Parse the raw formula string
                let ast = parse_formula(&formula.raw).map_err(|e| {
                    SemaflowError::Validation(format!(
                        "Formula parse error in measure '{}': {}",
                        name, e
                    ))
                })?;

                // Validate the AST
                self.validate_formula_ast(
                    name,
                    &ast,
                    &measure_names,
                    &formula_measures,
                    &column_names,
                )?;
            }
        }

        if let Some(time_dim) = &table.time_dimension {
            self.check(
                column_names.contains(time_dim),
                format!("time_dimension {time_dim} missing in table {}", table.name),
            )?;
        }

        for (name, measure) in &table.measures {
            if let Some(post) = &measure.post_expr {
                // Validate measure references
                let mut measure_refs = Vec::new();
                collect_measure_refs(post, &mut measure_refs);
                for r in measure_refs {
                    let dep = table.measures.get(&r).ok_or_else(|| {
                        SemaflowError::Validation(format!(
                            "measure {name} post_expr references unknown measure {r}"
                        ))
                    })?;
                    self.check(
                        dep.post_expr.is_none(),
                        format!("derived measure {name} cannot reference derived measure {r}"),
                    )?;
                }

                // Validate column references in post_expr
                let mut col_refs = Vec::new();
                collect_column_refs(post, &mut col_refs);
                for col in col_refs {
                    self.check(
                        column_names.contains(&col),
                        format!("measure {name} post_expr references missing column {col}"),
                    )?;
                }
            }
        }

        Ok(())
    }

    fn validate_flow(&self, flow: &SemanticFlow, registry: &FlowRegistry) -> Result<()> {
        let base_table = registry
            .get_table(&flow.base_table.semantic_table)
            .ok_or_else(|| {
                SemaflowError::Validation(format!(
                    "flow {} base table {} not found",
                    flow.name, flow.base_table.semantic_table
                ))
            })?;

        let base_ds = &base_table.data_source;

        let mut aliases = HashSet::new();
        aliases.insert(flow.base_table.alias.clone());
        let mut alias_to_table = std::collections::HashMap::new();
        alias_to_table.insert(flow.base_table.alias.clone(), base_table);

        for (join_name, join) in &flow.joins {
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

        for (join_name, join) in &flow.joins {
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

    /// Validate a formula AST, checking references and columns.
    fn validate_formula_ast(
        &self,
        measure_name: &str,
        ast: &FormulaAst,
        all_measures: &HashSet<String>,
        formula_measures: &HashSet<String>,
        column_names: &HashSet<String>,
    ) -> Result<()> {
        match ast {
            FormulaAst::Aggregation { column, filter, .. } => {
                // Aggregations reference columns - validate they exist
                // Handle qualified names like "o.amount" → just check "amount"
                let col_name = column.split('.').next_back().unwrap_or(column);
                self.check(
                    column_names.contains(col_name),
                    format!(
                        "Measure '{}' formula references unknown column '{}'.",
                        measure_name, column
                    ),
                )?;

                // Validate filter if present
                if let Some(f) = filter {
                    self.validate_formula_ast(
                        measure_name,
                        f,
                        all_measures,
                        formula_measures,
                        column_names,
                    )?;
                }
            }

            FormulaAst::MeasureRef { name } => {
                // Explicit measure reference - validate it exists and is simple
                self.check(
                    all_measures.contains(name),
                    format!(
                        "Measure '{}' formula references unknown measure '{}'.\n\
                         Available measures: {:?}",
                        measure_name,
                        name,
                        all_measures.iter().collect::<Vec<_>>()
                    ),
                )?;

                self.check(
                    name != measure_name,
                    format!(
                        "Measure '{}' formula cannot reference itself - this would create infinite recursion.",
                        measure_name
                    ),
                )?;

                self.check(
                    !formula_measures.contains(name),
                    format!(
                        "Measure '{}' formula references '{}', which is also a formula measure.\n\
                         Formula measures can only reference simple measures (those with 'expr' + 'agg').",
                        measure_name, name
                    ),
                )?;
            }

            FormulaAst::Column { column } => {
                // During parsing, bare identifiers become Column nodes.
                // If the name matches a measure, it's a measure reference;
                // otherwise it's a column reference.
                let col_name = column.split('.').next_back().unwrap_or(column);

                if all_measures.contains(column) {
                    // It's a measure reference
                    self.check(
                        column != measure_name,
                        format!(
                            "Measure '{}' formula cannot reference itself - this would create infinite recursion.",
                            measure_name
                        ),
                    )?;

                    self.check(
                        !formula_measures.contains(column),
                        format!(
                            "Measure '{}' formula references '{}', which is also a formula measure.\n\
                             Formula measures can only reference simple measures (those with 'expr' + 'agg').",
                            measure_name, column
                        ),
                    )?;
                } else {
                    // It's a column reference - validate it exists
                    self.check(
                        column_names.contains(col_name),
                        format!(
                            "Measure '{}' formula references unknown column or measure '{}'.\n\
                             Not found in table columns or measure names.",
                            measure_name, column
                        ),
                    )?;
                }
            }

            FormulaAst::Literal { .. } => {
                // Literals are always valid
            }

            FormulaAst::Binary { left, right, .. } => {
                self.validate_formula_ast(
                    measure_name,
                    left,
                    all_measures,
                    formula_measures,
                    column_names,
                )?;
                self.validate_formula_ast(
                    measure_name,
                    right,
                    all_measures,
                    formula_measures,
                    column_names,
                )?;
            }

            FormulaAst::Function { args, .. } => {
                for arg in args {
                    self.validate_formula_ast(
                        measure_name,
                        arg,
                        all_measures,
                        formula_measures,
                        column_names,
                    )?;
                }
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

fn table_has_column(table: &SemanticTable, col: &str) -> bool {
    if table.primary_keys.contains(&col.to_string()) {
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
        .any(|d| simple_column_name(&d.expr) == Some(col))
}
