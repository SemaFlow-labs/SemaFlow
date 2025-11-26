use std::collections::HashMap;

use crate::data_sources::DataSourceRegistry;
use crate::dialect::Dialect;
use crate::error::{Result, SemaflowError};
use crate::models::{
    Aggregation, Expr, Filter, FilterOp, QueryRequest, SemanticModel, SemanticTable, SortDirection,
};
use crate::registry::ModelRegistry;

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
        registry: &ModelRegistry,
        request: &QueryRequest,
        dialect: &dyn Dialect,
    ) -> Result<String> {
        let model = registry
            .get_model(&request.model)
            .ok_or_else(|| SemaflowError::Validation(format!("unknown model {}", request.model)))?;

        let alias_to_table = self.build_alias_map(model, registry)?;

        let mut select_parts = Vec::new();
        let mut group_by_parts = Vec::new();

        let base_table = alias_to_table.get(&model.base_table.alias).ok_or_else(|| {
            SemaflowError::Validation(format!(
                "missing base table alias {}",
                model.base_table.alias
            ))
        })?;

        for dim_name in &request.dimensions {
            let (_table, alias, dimension) =
                self.resolve_dimension(dim_name, model, registry, &alias_to_table)?;
            let expr_sql = render_expr(&dimension.expression, dialect, &alias);
            let alias_sql = dialect.quote_ident(dim_name);
            select_parts.push(format!("{expr_sql} AS {alias_sql}"));
            group_by_parts.push(expr_sql);
        }

        for measure_name in &request.measures {
            let (_table, alias, measure) =
                self.resolve_measure(measure_name, model, registry, &alias_to_table)?;
            let base_expr = render_expr(&measure.expr, dialect, &alias);
            let expr_sql = aggregation_sql(&measure.agg, &base_expr);
            let alias_sql = dialect.quote_ident(measure_name);
            select_parts.push(format!("{expr_sql} AS {alias_sql}"));
        }

        if select_parts.is_empty() {
            return Err(SemaflowError::Validation(
                "query requires at least one dimension or measure".to_string(),
            ));
        }

        let mut sql = format!(
            "SELECT {} FROM {} {}",
            select_parts.join(", "),
            dialect.quote_ident(&base_table.table),
            dialect.quote_ident(&model.base_table.alias)
        );

        for join in model.joins.values() {
            let join_kw = match join.join_type {
                crate::models::JoinType::Inner => "JOIN",
                crate::models::JoinType::Left => "LEFT JOIN",
                crate::models::JoinType::Right => "RIGHT JOIN",
                crate::models::JoinType::Full => "FULL JOIN",
            };
            let join_table = alias_to_table.get(&join.alias).ok_or_else(|| {
                SemaflowError::Validation(format!(
                    "missing semantic table for join alias {}",
                    join.alias
                ))
            })?;
            let on_clause = join
                .join_keys
                .iter()
                .map(|k| {
                    format!(
                        "{} = {}",
                        qualify_col(&join.to_table, &k.left, dialect),
                        qualify_col(&join.alias, &k.right, dialect)
                    )
                })
                .collect::<Vec<_>>()
                .join(" AND ");
            sql.push_str(&format!(
                " {join_kw} {} {} ON {}",
                dialect.quote_ident(&join_table.table),
                dialect.quote_ident(&join.alias),
                on_clause
            ));
        }

        if !request.filters.is_empty() {
            let mut clauses = Vec::new();
            for filter in &request.filters {
                let (expr, kind) = self.resolve_field_expression(
                    &filter.field,
                    model,
                    registry,
                    &alias_to_table,
                    dialect,
                )?;
                if matches!(kind, FieldKind::Measure) {
                    return Err(SemaflowError::Validation(
                        "filters on measures are not supported (row-level filters only)"
                            .to_string(),
                    ));
                }
                clauses.push(render_filter(&expr, filter, dialect));
            }
            sql.push_str(&format!(" WHERE {}", clauses.join(" AND ")));
        }

        if !group_by_parts.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", group_by_parts.join(", ")));
        }

        if !request.order.is_empty() {
            let mut parts = Vec::new();
            for item in &request.order {
                let (expr, _) = self.resolve_field_expression(
                    &item.column,
                    model,
                    registry,
                    &alias_to_table,
                    dialect,
                )?;
                let dir = match item.direction {
                    SortDirection::Asc => "ASC",
                    SortDirection::Desc => "DESC",
                };
                parts.push(format!("{expr} {dir}"));
            }
            sql.push_str(&format!(" ORDER BY {}", parts.join(", ")));
        }

        if let Some(limit) = request.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }
        if let Some(offset) = request.offset {
            sql.push_str(&format!(" OFFSET {}", offset));
        }

        Ok(sql)
    }

    /// Build SQL by resolving the model's data source to choose a dialect.
    pub fn build_for_request(
        &self,
        registry: &ModelRegistry,
        data_sources: &DataSourceRegistry,
        request: &QueryRequest,
    ) -> Result<String> {
        let model = registry
            .get_model(&request.model)
            .ok_or_else(|| SemaflowError::Validation(format!("unknown model {}", request.model)))?;
        let base_table = registry
            .get_table(&model.base_table.semantic_table)
            .ok_or_else(|| {
                SemaflowError::Validation(format!(
                    "model {} base table {} not found",
                    model.name, model.base_table.semantic_table
                ))
            })?;
        let data_source = data_sources.get(&base_table.data_source).ok_or_else(|| {
            SemaflowError::Validation(format!(
                "data source {} not registered",
                base_table.data_source
            ))
        })?;
        self.build_with_dialect(registry, request, data_source.dialect.as_ref())
    }

    fn build_alias_map<'a>(
        &self,
        model: &'a SemanticModel,
        registry: &'a ModelRegistry,
    ) -> Result<HashMap<String, &'a SemanticTable>> {
        let mut map = HashMap::new();
        let base = registry
            .get_table(&model.base_table.semantic_table)
            .ok_or_else(|| {
                SemaflowError::Validation(format!(
                    "unknown semantic table {}",
                    model.base_table.semantic_table
                ))
            })?;
        map.insert(model.base_table.alias.clone(), base);

        for join in model.joins.values() {
            let table = registry.get_table(&join.semantic_table).ok_or_else(|| {
                SemaflowError::Validation(format!("unknown semantic table {}", join.semantic_table))
            })?;
            map.insert(join.alias.clone(), table);
        }
        Ok(map)
    }

    fn resolve_dimension<'a>(
        &self,
        name: &str,
        model: &'a SemanticModel,
        registry: &'a ModelRegistry,
        alias_map: &HashMap<String, &'a SemanticTable>,
    ) -> Result<(&'a SemanticTable, String, &'a crate::models::Dimension)> {
        self.resolve_dimension_inner(name, model, registry, alias_map)
            .ok_or_else(|| SemaflowError::Validation(format!("unknown dimension {name}")))
    }

    fn resolve_measure<'a>(
        &self,
        name: &str,
        model: &'a SemanticModel,
        registry: &'a ModelRegistry,
        alias_map: &HashMap<String, &'a SemanticTable>,
    ) -> Result<(&'a SemanticTable, String, &'a crate::models::Measure)> {
        self.resolve_measure_inner(name, model, registry, alias_map)
            .ok_or_else(|| SemaflowError::Validation(format!("unknown measure {name}")))
    }

    fn resolve_dimension_inner<'a>(
        &self,
        name: &str,
        model: &'a SemanticModel,
        registry: &'a ModelRegistry,
        alias_map: &HashMap<String, &'a SemanticTable>,
    ) -> Option<(&'a SemanticTable, String, &'a crate::models::Dimension)> {
        if let Some(dim) = registry
            .get_table(&model.base_table.semantic_table)?
            .dimensions
            .get(name)
        {
            return Some((
                registry
                    .get_table(&model.base_table.semantic_table)
                    .unwrap(),
                model.base_table.alias.clone(),
                dim,
            ));
        }
        for join in model.joins.values() {
            let table = alias_map.get(&join.alias)?;
            if let Some(dim) = table.dimensions.get(name) {
                return Some((table, join.alias.clone(), dim));
            }
        }
        None
    }

    fn resolve_measure_inner<'a>(
        &self,
        name: &str,
        model: &'a SemanticModel,
        registry: &'a ModelRegistry,
        alias_map: &HashMap<String, &'a SemanticTable>,
    ) -> Option<(&'a SemanticTable, String, &'a crate::models::Measure)> {
        if let Some(measure) = registry
            .get_table(&model.base_table.semantic_table)?
            .measures
            .get(name)
        {
            return Some((
                registry
                    .get_table(&model.base_table.semantic_table)
                    .unwrap(),
                model.base_table.alias.clone(),
                measure,
            ));
        }
        for join in model.joins.values() {
            let table = alias_map.get(&join.alias)?;
            if let Some(measure) = table.measures.get(name) {
                return Some((table, join.alias.clone(), measure));
            }
        }
        None
    }

    fn resolve_field_expression(
        &self,
        name: &str,
        model: &SemanticModel,
        registry: &ModelRegistry,
        alias_map: &HashMap<String, &SemanticTable>,
        dialect: &dyn Dialect,
    ) -> Result<(String, FieldKind)> {
        if let Some((_, alias, dim)) =
            self.resolve_dimension_inner(name, model, registry, alias_map)
        {
            let expr = render_expr(&dim.expression, dialect, &alias);
            return Ok((expr, FieldKind::Dimension));
        }
        if let Some((_, _, _)) = self.resolve_measure_inner(name, model, registry, alias_map) {
            return Ok((dialect.quote_ident(name), FieldKind::Measure));
        }
        Err(SemaflowError::Validation(format!(
            "field {name} not found in model {}",
            model.name
        )))
    }
}

fn aggregation_sql(agg: &Aggregation, expr: &str) -> String {
    match agg {
        Aggregation::Sum => format!("SUM({expr})"),
        Aggregation::Count => format!("COUNT({expr})"),
        Aggregation::CountDistinct => format!("COUNT(DISTINCT {expr})"),
        Aggregation::Min => format!("MIN({expr})"),
        Aggregation::Max => format!("MAX({expr})"),
        Aggregation::Avg => format!("AVG({expr})"),
    }
}

fn render_filter<D: Dialect + ?Sized>(expr: &str, filter: &Filter, dialect: &D) -> String {
    let rendered_value = render_value(&filter.value, dialect);
    let op = match filter.op {
        FilterOp::Eq => "=",
        FilterOp::Neq => "!=",
        FilterOp::Gt => ">",
        FilterOp::Gte => ">=",
        FilterOp::Lt => "<",
        FilterOp::Lte => "<=",
        FilterOp::Like => "LIKE",
        FilterOp::In => "IN",
        FilterOp::NotIn => "NOT IN",
    };

    match filter.op {
        FilterOp::In | FilterOp::NotIn => format!("{expr} {op} ({rendered_value})"),
        _ => format!("{expr} {op} {rendered_value}"),
    }
}

fn render_value<D: Dialect + ?Sized>(value: &serde_json::Value, dialect: &D) -> String {
    match value {
        serde_json::Value::Array(items) => {
            let rendered: Vec<String> = items.iter().map(|v| dialect.render_literal(v)).collect();
            rendered.join(", ")
        }
        _ => dialect.render_literal(value),
    }
}

fn render_expr<D: Dialect + ?Sized>(expr: &Expr, dialect: &D, alias: &str) -> String {
    match expr {
        Expr::Column { column } => qualify_col(alias, column, dialect),
        Expr::Literal { value } => dialect.render_literal(value),
        Expr::Func { func, args } => {
            let rendered_args: Vec<String> = args
                .iter()
                .map(|arg| render_expr(arg, dialect, alias))
                .collect();
            dialect.render_function(func, rendered_args)
        }
        Expr::Case {
            branches,
            else_expr,
        } => {
            let mut parts = Vec::new();
            parts.push("CASE".to_string());
            for branch in branches {
                let when_expr = render_expr(&branch.when, dialect, alias);
                let then_expr = render_expr(&branch.then, dialect, alias);
                parts.push(format!(" WHEN {when_expr} THEN {then_expr}"));
            }
            let else_sql = render_expr(else_expr, dialect, alias);
            parts.push(format!(" ELSE {else_sql} END"));
            parts.join("")
        }
        Expr::Binary { op, left, right } => {
            let l = render_expr(left, dialect, alias);
            let r = render_expr(right, dialect, alias);
            let op_sql = match op {
                crate::models::BinaryOp::Add => "+",
                crate::models::BinaryOp::Subtract => "-",
                crate::models::BinaryOp::Multiply => "*",
                crate::models::BinaryOp::Divide => "/",
                crate::models::BinaryOp::Modulo => "%",
            };
            format!("({l} {op_sql} {r})")
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum FieldKind {
    Dimension,
    Measure,
}

fn qualify_col<D: Dialect + ?Sized>(alias: &str, column: &str, dialect: &D) -> String {
    format!(
        "{}.{}",
        dialect.quote_ident(alias),
        dialect.quote_ident(column)
    )
}
