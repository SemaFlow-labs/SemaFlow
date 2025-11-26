use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct ForeignKey {
    pub from_column: String,
    pub to_table: String,
    pub to_column: String,
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub columns: Vec<ColumnSchema>,
    pub primary_keys: Vec<String>,
    pub foreign_keys: Vec<ForeignKey>,
}

#[derive(Debug, Default, Clone)]
pub struct SchemaCache {
    schemas: HashMap<(String, String), TableSchema>,
}

impl SchemaCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, data_source: String, table: String, schema: TableSchema) {
        self.schemas.insert((data_source, table), schema);
    }

    pub fn get(&self, data_source: &str, table: &str) -> Option<&TableSchema> {
        self.schemas
            .get(&(data_source.to_string(), table.to_string()))
    }

    pub fn contains(&self, data_source: &str, table: &str) -> bool {
        self.schemas
            .contains_key(&(data_source.to_string(), table.to_string()))
    }
}
