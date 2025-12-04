use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use glob::glob;
use serde::Serialize;

use crate::error::{Result, SemaflowError};
use crate::flows::{Aggregation, Expr, FlowTableRef, SemanticFlow, SemanticTable};

#[derive(Debug, Default, Clone)]
pub struct FlowRegistry {
    pub tables: HashMap<String, SemanticTable>,
    pub flows: HashMap<String, SemanticFlow>,
}

impl FlowRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_parts(tables: Vec<SemanticTable>, flows: Vec<SemanticFlow>) -> Self {
        let mut registry = FlowRegistry::new();
        for table in tables {
            registry.tables.insert(table.name.clone(), table);
        }
        for flow in flows {
            registry.flows.insert(flow.name.clone(), flow);
        }
        registry
    }

    /// Load tables/flows from disk. Accepts either:
    /// - a directory containing `tables/` and `flows/` subdirectories
    /// - a directory with YAML files directly inside (used for both tables and flows)
    pub fn load_from_dir<P: AsRef<Path>>(root: P) -> Result<Self> {
        let mut registry = FlowRegistry::new();
        let root = root.as_ref();
        let tables_dir = root.join("tables");
        let flows_dir = root.join("flows");

        let tables_path = if tables_dir.exists() {
            tables_dir
        } else {
            root.to_path_buf()
        };
        let flows_path = if flows_dir.exists() {
            flows_dir
        } else {
            root.to_path_buf()
        };

        registry.load_tables(tables_path)?;
        registry.load_flows(flows_path)?;
        Ok(registry)
    }

    fn load_tables(&mut self, dir: PathBuf) -> Result<()> {
        if !dir.exists() {
            return Err(SemaflowError::Validation(format!(
                "tables directory not found or empty: {}",
                dir.display()
            )));
        }
        let mut loaded = false;
        for entry in glob(&format!("{}/*.yml", dir.display()))
            .map_err(|e| SemaflowError::Other(e.into()))?
            .flatten()
        {
            loaded |= self.load_table_file(&entry)?;
        }
        for entry in glob(&format!("{}/*.yaml", dir.display()))
            .map_err(|e| SemaflowError::Other(e.into()))?
            .flatten()
        {
            loaded |= self.load_table_file(&entry)?;
        }
        if !loaded {
            return Err(SemaflowError::Validation(format!(
                "no semantic tables found in {}",
                dir.display()
            )));
        }
        Ok(())
    }

    fn load_table_file(&mut self, path: &Path) -> Result<bool> {
        let contents = fs::read_to_string(path)?;
        match serde_yaml::from_str::<SemanticTable>(&contents) {
            Ok(table) => {
                self.tables.insert(table.name.clone(), table);
                Ok(true)
            }
            Err(e) => Err(SemaflowError::Validation(format!(
                "failed to parse table {}: {e}",
                path.display()
            ))),
        }
    }

    fn load_flows(&mut self, dir: PathBuf) -> Result<()> {
        if !dir.exists() {
            return Err(SemaflowError::Validation(format!(
                "flows directory not found or empty: {}",
                dir.display()
            )));
        }
        let mut loaded = false;
        for entry in glob(&format!("{}/*.yml", dir.display()))
            .map_err(|e| SemaflowError::Other(e.into()))?
            .flatten()
        {
            loaded |= self.load_flow_file(&entry)?;
        }
        for entry in glob(&format!("{}/*.yaml", dir.display()))
            .map_err(|e| SemaflowError::Other(e.into()))?
            .flatten()
        {
            loaded |= self.load_flow_file(&entry)?;
        }
        if !loaded {
            return Err(SemaflowError::Validation(format!(
                "no semantic flows found in {}",
                dir.display()
            )));
        }
        Ok(())
    }

    fn load_flow_file(&mut self, path: &Path) -> Result<bool> {
        let contents = fs::read_to_string(path)?;
        match serde_yaml::from_str::<SemanticFlow>(&contents) {
            Ok(flow) => {
                self.flows.insert(flow.name.clone(), flow);
                Ok(true)
            }
            Err(e) => Err(SemaflowError::Validation(format!(
                "failed to parse flow {}: {e}",
                path.display()
            ))),
        }
    }

    pub fn get_table(&self, name: &str) -> Option<&SemanticTable> {
        self.tables.get(name)
    }

    pub fn get_flow(&self, name: &str) -> Option<&SemanticFlow> {
        self.flows.get(name)
    }

    /// List flow names and descriptions for discovery endpoints.
    pub fn list_flow_summaries(&self) -> Vec<FlowSummary> {
        self.flows
            .values()
            .map(|m| FlowSummary {
                name: m.name.clone(),
                description: m.description.clone(),
            })
            .collect()
    }

    /// Return a flow's schema (dimensions, measures, joins) including descriptions.
    pub fn flow_schema(&self, name: &str) -> Result<FlowSchema> {
        let flow = self
            .get_flow(name)
            .ok_or_else(|| SemaflowError::Validation(format!("unknown flow {name}")))?;
        let base_table = self
            .tables
            .get(&flow.base_table.semantic_table)
            .ok_or_else(|| {
                SemaflowError::Validation(format!(
                    "flow {} base table {} not found",
                    flow.name, flow.base_table.semantic_table
                ))
            })?;

        let mut dimensions = Vec::new();
        let mut measures = Vec::new();

        collect_fields(&flow.base_table, base_table, &mut dimensions, &mut measures);

        for (join_name, join) in &flow.joins {
            let table = self.tables.get(&join.semantic_table).ok_or_else(|| {
                SemaflowError::Validation(format!(
                    "join {join_name} references missing table {}",
                    join.semantic_table
                ))
            })?;
            let join_ref = FlowTableRef {
                semantic_table: join.semantic_table.clone(),
                alias: join.alias.clone(),
            };
            collect_fields(&join_ref, table, &mut dimensions, &mut measures);
        }

        Ok(FlowSchema {
            name: flow.name.clone(),
            description: flow.description.clone(),
            base_table: flow.base_table.clone(),
            data_source: base_table.data_source.clone(),
            time_dimension: base_table.time_dimension.clone(),
            smallest_time_grain: base_table
                .smallest_time_grain
                .as_ref()
                .map(|g| format!("{:?}", g)),
            dimensions,
            measures,
        })
    }
}

fn collect_fields(
    table_ref: &FlowTableRef,
    table: &SemanticTable,
    dimensions: &mut Vec<DimensionInfo>,
    measures: &mut Vec<MeasureInfo>,
) {
    for (name, dim) in &table.dimensions {
        let qualified = format!("{}.{}", table_ref.alias, name);
        dimensions.push(DimensionInfo {
            name: name.clone(),
            qualified_name: qualified,
            description: dim.description.clone(),
            data_type: dim.data_type.clone(),
            semantic_table: table_ref.semantic_table.clone(),
            table_alias: table_ref.alias.clone(),
            expr: dim.expression.clone(),
        });
    }
    for (name, measure) in &table.measures {
        let qualified = format!("{}.{}", table_ref.alias, name);
        measures.push(MeasureInfo {
            name: name.clone(),
            qualified_name: qualified,
            description: measure.description.clone(),
            data_type: measure.data_type.clone(),
            semantic_table: table_ref.semantic_table.clone(),
            table_alias: table_ref.alias.clone(),
            expr: measure.expr.clone(),
            agg: measure.agg.clone(),
            filter: measure.filter.clone(),
            post_expr: measure.post_expr.clone(),
        });
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct FlowSummary {
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FlowSchema {
    pub name: String,
    pub description: Option<String>,
    pub base_table: FlowTableRef,
    pub data_source: String,
    pub time_dimension: Option<String>,
    pub smallest_time_grain: Option<String>,
    pub dimensions: Vec<DimensionInfo>,
    pub measures: Vec<MeasureInfo>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DimensionInfo {
    pub name: String,
    pub qualified_name: String,
    pub description: Option<String>,
    pub data_type: Option<String>,
    pub semantic_table: String,
    pub table_alias: String,
    pub expr: Expr,
}

#[derive(Debug, Clone, Serialize)]
pub struct MeasureInfo {
    pub name: String,
    pub qualified_name: String,
    pub description: Option<String>,
    pub data_type: Option<String>,
    pub semantic_table: String,
    pub table_alias: String,
    pub expr: Expr,
    pub agg: Aggregation,
    pub filter: Option<Expr>,
    pub post_expr: Option<Expr>,
}
