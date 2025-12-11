use crate::data_sources::ConnectionManager;
use crate::error::{Result, SemaflowError};
use crate::flows::QueryRequest;
use crate::registry::FlowRegistry;
use crate::sql_ast::SqlRenderer;

mod analysis;
mod builders;
mod components;
mod filters;
mod grain;
mod joins;
mod measures;
mod plan;
mod planner;
mod render;
mod resolve;

pub struct SqlBuilder;

impl Default for SqlBuilder {
    fn default() -> Self {
        Self
    }
}

impl SqlBuilder {
    /// Build SQL using a provided dialect (useful for tests).
    pub fn build_with_dialect(
        &self,
        registry: &FlowRegistry,
        request: &QueryRequest,
        dialect: &dyn crate::dialect::Dialect,
    ) -> Result<String> {
        let flow = registry
            .get_flow(&request.flow)
            .ok_or_else(|| SemaflowError::Validation(format!("unknown flow {}", request.flow)))?;

        let supports_filtered_aggregates = if std::env::var("SEMAFLOW_DISABLE_FILTERED_AGG")
            .ok()
            .as_deref()
            == Some("1")
        {
            false
        } else {
            dialect.supports_filtered_aggregates()
        };

        let query = planner::build_query(flow, registry, request, supports_filtered_aggregates)?;
        let renderer = SqlRenderer::new(dialect);
        Ok(renderer.render_select(&query))
    }

    /// Build SQL by resolving the flow's data source to choose a dialect.
    pub fn build_for_request(
        &self,
        registry: &FlowRegistry,
        connections: &ConnectionManager,
        request: &QueryRequest,
    ) -> Result<String> {
        let flow = registry
            .get_flow(&request.flow)
            .ok_or_else(|| SemaflowError::Validation(format!("unknown flow {}", request.flow)))?;
        let base_table = registry
            .get_table(&flow.base_table.semantic_table)
            .ok_or_else(|| {
                SemaflowError::Validation(format!(
                    "flow {} base table {} not found",
                    flow.name, flow.base_table.semantic_table
                ))
            })?;
        let data_source = connections.get(&base_table.data_source).ok_or_else(|| {
            SemaflowError::Validation(format!(
                "data source {} not registered",
                base_table.data_source
            ))
        })?;
        self.build_with_dialect(registry, request, data_source.dialect())
    }
}
