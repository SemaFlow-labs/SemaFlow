use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use glob::glob;

use crate::error::{Result, SemaflowError};
use crate::models::{SemanticModel, SemanticTable};

#[derive(Debug, Default, Clone)]
pub struct ModelRegistry {
    pub tables: HashMap<String, SemanticTable>,
    pub models: HashMap<String, SemanticModel>,
}

impl ModelRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_parts(tables: Vec<SemanticTable>, models: Vec<SemanticModel>) -> Self {
        let mut registry = ModelRegistry::new();
        for table in tables {
            registry.tables.insert(table.name.clone(), table);
        }
        for model in models {
            registry.models.insert(model.name.clone(), model);
        }
        registry
    }

    pub fn load_from_dir<P: AsRef<Path>>(root: P) -> Result<Self> {
        let mut registry = ModelRegistry::new();
        registry.load_tables(root.as_ref().join("tables"))?;
        registry.load_models(root.as_ref().join("models"))?;
        Ok(registry)
    }

    fn load_tables(&mut self, dir: PathBuf) -> Result<()> {
        if !dir.exists() {
            return Err(SemaflowError::Validation(format!(
                "tables directory not found: {}",
                dir.display()
            )));
        }
        for entry in glob(&format!("{}/*.yml", dir.display()))
            .map_err(|e| SemaflowError::Other(e.into()))?
            .flatten()
        {
            self.load_table_file(&entry)?;
        }
        for entry in glob(&format!("{}/*.yaml", dir.display()))
            .map_err(|e| SemaflowError::Other(e.into()))?
            .flatten()
        {
            self.load_table_file(&entry)?;
        }
        Ok(())
    }

    fn load_table_file(&mut self, path: &Path) -> Result<()> {
        let contents = fs::read_to_string(path)?;
        let table: SemanticTable = serde_yaml::from_str(&contents)?;
        self.tables.insert(table.name.clone(), table);
        Ok(())
    }

    fn load_models(&mut self, dir: PathBuf) -> Result<()> {
        if !dir.exists() {
            return Err(SemaflowError::Validation(format!(
                "models directory not found: {}",
                dir.display()
            )));
        }
        for entry in glob(&format!("{}/*.yml", dir.display()))
            .map_err(|e| SemaflowError::Other(e.into()))?
            .flatten()
        {
            self.load_model_file(&entry)?;
        }
        for entry in glob(&format!("{}/*.yaml", dir.display()))
            .map_err(|e| SemaflowError::Other(e.into()))?
            .flatten()
        {
            self.load_model_file(&entry)?;
        }
        Ok(())
    }

    fn load_model_file(&mut self, path: &Path) -> Result<()> {
        let contents = fs::read_to_string(path)?;
        let model: SemanticModel = serde_yaml::from_str(&contents)?;
        self.models.insert(model.name.clone(), model);
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<&SemanticTable> {
        self.tables.get(name)
    }

    pub fn get_model(&self, name: &str) -> Option<&SemanticModel> {
        self.models.get(name)
    }
}
