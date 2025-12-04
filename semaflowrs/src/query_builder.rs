use std::collections::{HashMap, HashSet};

use crate::data_sources::ConnectionManager;
use crate::error::{Result, SemaflowError};
use crate::flows::{Expr, Filter, FilterOp, QueryRequest, SemanticFlow, SemanticTable};
use crate::registry::FlowRegistry;
use crate::sql_ast::{
    Join, OrderItem, SelectItem, SelectQuery, SqlBinaryOperator, SqlExpr, SqlJoinType, SqlRenderer,
    TableRef,
};

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

        let alias_to_table = self.build_alias_map(flow, registry)?;
        let mut required_aliases: HashSet<String> = HashSet::new();

        let base_table = alias_to_table.get(&flow.base_table.alias).ok_or_else(|| {
            SemaflowError::Validation(format!(
                "missing base table alias {}",
                flow.base_table.alias
            ))
        })?;
        required_aliases.insert(flow.base_table.alias.clone());

        let mut query = SelectQuery::default();
        query.from = TableRef {
            name: base_table.table.clone(),
            alias: Some(flow.base_table.alias.clone()),
        };

        for dim_name in &request.dimensions {
            let (_table, alias, dimension) =
                self.resolve_dimension(dim_name, flow, registry, &alias_to_table)?;
            required_aliases.insert(alias.clone());
            let expr = expr_to_sql(&dimension.expression, &alias);
            query.group_by.push(expr.clone());
            query.select.push(SelectItem {
                expr,
                alias: Some(dim_name.clone()),
            });
        }

        for measure_name in &request.measures {
            let (_table, alias, measure) =
                self.resolve_measure(measure_name, flow, registry, &alias_to_table)?;
            required_aliases.insert(alias.clone());
            let base_expr = expr_to_sql(&measure.expr, &alias);
            let agg_expr = SqlExpr::Aggregate {
                agg: measure.agg.clone(),
                expr: Box::new(base_expr),
            };
            query.select.push(SelectItem {
                expr: agg_expr,
                alias: Some(measure_name.clone()),
            });
        }

        if query.select.is_empty() {
            return Err(SemaflowError::Validation(
                "query requires at least one dimension or measure".to_string(),
            ));
        }

        if !request.filters.is_empty() {
            for filter in &request.filters {
                let (expr, kind, alias) =
                    self.resolve_field_expression(&filter.field, flow, registry, &alias_to_table)?;
                if matches!(kind, FieldKind::Measure) {
                    return Err(SemaflowError::Validation(
                        "filters on measures are not supported (row-level filters only)"
                            .to_string(),
                    ));
                }
                if let Some(alias) = alias {
                    required_aliases.insert(alias);
                }
                query.filters.push(render_filter_expr(expr, filter));
            }
        }

        if !request.order.is_empty() {
            for item in &request.order {
                let (expr, _, alias) =
                    self.resolve_field_expression(&item.column, flow, registry, &alias_to_table)?;
                if let Some(alias) = alias {
                    required_aliases.insert(alias);
                }
                query.order_by.push(OrderItem {
                    expr,
                    direction: item.direction.clone(),
                });
            }
        }

        query.limit = request.limit.map(|v| v as u64);
        query.offset = request.offset.map(|v| v as u64);

        let required_joins = self.select_required_joins(flow, &required_aliases, &alias_to_table)?;
        for join in required_joins {
            let join_table = alias_to_table.get(&join.alias).ok_or_else(|| {
                SemaflowError::Validation(format!(
                    "missing semantic table for join alias {}",
                    join.alias
                ))
            })?;
            let mut on_clause = Vec::new();
            for k in &join.join_keys {
                on_clause.push(SqlExpr::BinaryOp {
                    op: SqlBinaryOperator::Eq,
                    left: Box::new(SqlExpr::Column {
                        table: Some(join.to_table.clone()),
                        name: k.left.clone(),
                    }),
                    right: Box::new(SqlExpr::Column {
                        table: Some(join.alias.clone()),
                        name: k.right.clone(),
                    }),
                });
            }
            query.joins.push(Join {
                join_type: match join.join_type {
                    crate::flows::JoinType::Inner => SqlJoinType::Inner,
                    crate::flows::JoinType::Left => SqlJoinType::Left,
                    crate::flows::JoinType::Right => SqlJoinType::Right,
                    crate::flows::JoinType::Full => SqlJoinType::Full,
                },
                table: TableRef {
                    name: join_table.table.clone(),
                    alias: Some(join.alias.clone()),
                },
                on: on_clause,
            });
        }

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

    fn build_alias_map<'a>(
        &self,
        flow: &'a SemanticFlow,
        registry: &'a FlowRegistry,
    ) -> Result<HashMap<String, &'a SemanticTable>> {
        let mut map = HashMap::new();
        let base = registry
            .get_table(&flow.base_table.semantic_table)
            .ok_or_else(|| {
                SemaflowError::Validation(format!(
                    "unknown semantic table {}",
                    flow.base_table.semantic_table
                ))
            })?;
        map.insert(flow.base_table.alias.clone(), base);

        for join in flow.joins.values() {
            let table = registry.get_table(&join.semantic_table).ok_or_else(|| {
                SemaflowError::Validation(format!("unknown semantic table {}", join.semantic_table))
            })?;
            map.insert(join.alias.clone(), table);
        }
        Ok(map)
    }

    fn select_required_joins<'a>(
        &self,
        flow: &'a SemanticFlow,
        required_aliases: &HashSet<String>,
        alias_to_table: &HashMap<String, &'a SemanticTable>,
    ) -> Result<Vec<&'a crate::flows::FlowJoin>> {
        let base_alias = &flow.base_table.alias;
        let mut join_by_alias: HashMap<&str, &crate::flows::FlowJoin> = HashMap::new();
        for join in flow.joins.values() {
            join_by_alias.insert(join.alias.as_str(), join);
        }

        let mut needed: HashSet<String> = HashSet::new();
        let mut stack: Vec<String> = required_aliases
            .iter()
            .filter(|a| *a != base_alias)
            .cloned()
            .collect();
        // Always include joins that are not safe to prune (e.g., inner or unknown cardinality).
        for join in flow.joins.values() {
            if !self.safe_to_prune(join, alias_to_table) && join.alias != *base_alias {
                stack.push(join.alias.clone());
            }
        }
        while let Some(alias) = stack.pop() {
            if !needed.insert(alias.clone()) {
                continue;
            }
            let join = join_by_alias.get(alias.as_str()).ok_or_else(|| {
                SemaflowError::Validation(format!("missing join definition for alias {}", alias))
            })?;
            if join.to_table != *base_alias {
                stack.push(join.to_table.clone());
            }
        }

        let mut ordered = Vec::new();
        let mut visited: HashSet<String> = HashSet::new();
        for join in flow.joins.values() {
            if needed.contains(&join.alias) {
                self.visit_join(&join.alias, base_alias, &join_by_alias, &mut visited, &mut ordered)?;
            }
        }
        Ok(ordered)
    }

    fn safe_to_prune(
        &self,
        join: &crate::flows::FlowJoin,
        alias_to_table: &HashMap<String, &SemanticTable>,
    ) -> bool {
        if join.join_type != crate::flows::JoinType::Left {
            return false;
        }
        if let Some(table) = alias_to_table.get(&join.alias) {
            if join.join_keys.len() == 1 && join.join_keys[0].right == table.primary_key {
                return true;
            }
        }
        false
    }

    fn visit_join<'a>(
        &self,
        alias: &str,
        base_alias: &str,
        join_by_alias: &HashMap<&'a str, &'a crate::flows::FlowJoin>,
        visited: &mut HashSet<String>,
        ordered: &mut Vec<&'a crate::flows::FlowJoin>,
    ) -> Result<()> {
        if visited.contains(alias) {
            return Ok(());
        }
        let join = *join_by_alias.get(alias).ok_or_else(|| {
            SemaflowError::Validation(format!("missing join definition for alias {}", alias))
        })?;
        if join.to_table != base_alias {
            self.visit_join(&join.to_table, base_alias, join_by_alias, visited, ordered)?;
        }
        visited.insert(alias.to_string());
        ordered.push(join);
        Ok(())
    }

    fn resolve_dimension<'a>(
        &self,
        name: &str,
        flow: &'a SemanticFlow,
        registry: &'a FlowRegistry,
        alias_map: &HashMap<String, &'a SemanticTable>,
    ) -> Result<(&'a SemanticTable, String, &'a crate::flows::Dimension)> {
        match self.resolve_dimension_inner(name, flow, registry, alias_map)? {
            Some(found) => Ok(found),
            None => Err(SemaflowError::Validation(format!("unknown dimension {name}"))),
        }
    }

    fn resolve_measure<'a>(
        &self,
        name: &str,
        flow: &'a SemanticFlow,
        registry: &'a FlowRegistry,
        alias_map: &HashMap<String, &'a SemanticTable>,
    ) -> Result<(&'a SemanticTable, String, &'a crate::flows::Measure)> {
        match self.resolve_measure_inner(name, flow, registry, alias_map)? {
            Some(found) => Ok(found),
            None => Err(SemaflowError::Validation(format!("unknown measure {name}"))),
        }
    }

    fn resolve_dimension_inner<'a>(
        &self,
        name: &str,
        flow: &'a SemanticFlow,
        registry: &'a FlowRegistry,
        alias_map: &HashMap<String, &'a SemanticTable>,
    ) -> Result<Option<(&'a SemanticTable, String, &'a crate::flows::Dimension)>> {
        // Qualified lookups are unambiguous; search only the referenced alias.
        if let Some((alias, field)) = parse_qualified(name) {
            if alias == flow.base_table.alias {
                if let Some(base_table) = registry.get_table(&flow.base_table.semantic_table) {
                    if let Some(dim) = base_table.dimensions.get(field) {
                        return Ok(Some((base_table, alias.to_string(), dim)));
                    }
                }
            }
            if let Some(table) = alias_map.get(alias) {
                if let Some(dim) = table.dimensions.get(field) {
                    return Ok(Some((table, alias.to_string(), dim)));
                }
            }
            return Ok(None);
        }

        let mut matches = Vec::new();
        if let Some(base_table) = registry.get_table(&flow.base_table.semantic_table) {
            if let Some(dim) = base_table.dimensions.get(name) {
                matches.push((base_table, flow.base_table.alias.clone(), dim));
            }
        }
        for join in flow.joins.values() {
            if let Some(table) = alias_map.get(&join.alias) {
                if let Some(dim) = table.dimensions.get(name) {
                    matches.push((*table, join.alias.clone(), dim));
                }
            }
        }

        if matches.len() > 1 {
            let aliases: Vec<String> = matches.iter().map(|(_, alias, _)| alias.clone()).collect();
            return Err(SemaflowError::Validation(format!(
                "ambiguous dimension {name}; found on aliases {}",
                aliases.join(", ")
            )));
        }

        Ok(matches.into_iter().next())
    }

    fn resolve_measure_inner<'a>(
        &self,
        name: &str,
        flow: &'a SemanticFlow,
        registry: &'a FlowRegistry,
        alias_map: &HashMap<String, &'a SemanticTable>,
    ) -> Result<Option<(&'a SemanticTable, String, &'a crate::flows::Measure)>> {
        // Qualified lookups are unambiguous; search only the referenced alias.
        if let Some((alias, field)) = parse_qualified(name) {
            if alias == flow.base_table.alias {
                if let Some(base_table) = registry.get_table(&flow.base_table.semantic_table) {
                    if let Some(measure) = base_table.measures.get(field) {
                        return Ok(Some((base_table, alias.to_string(), measure)));
                    }
                }
            }
            if let Some(table) = alias_map.get(alias) {
                if let Some(measure) = table.measures.get(field) {
                    return Ok(Some((table, alias.to_string(), measure)));
                }
            }
            return Ok(None);
        }

        let mut matches = Vec::new();
        if let Some(base_table) = registry.get_table(&flow.base_table.semantic_table) {
            if let Some(measure) = base_table.measures.get(name) {
                matches.push((base_table, flow.base_table.alias.clone(), measure));
            }
        }
        for join in flow.joins.values() {
            if let Some(table) = alias_map.get(&join.alias) {
                if let Some(measure) = table.measures.get(name) {
                    matches.push((*table, join.alias.clone(), measure));
                }
            }
        }

        if matches.len() > 1 {
            let aliases: Vec<String> = matches.iter().map(|(_, alias, _)| alias.clone()).collect();
            return Err(SemaflowError::Validation(format!(
                "ambiguous measure {name}; found on aliases {}",
                aliases.join(", ")
            )));
        }

        Ok(matches.into_iter().next())
    }

    fn resolve_field_expression(
        &self,
        name: &str,
        flow: &SemanticFlow,
        registry: &FlowRegistry,
        alias_map: &HashMap<String, &SemanticTable>,
    ) -> Result<(SqlExpr, FieldKind, Option<String>)> {
        if let Some((_, alias, dim)) =
            self.resolve_dimension_inner(name, flow, registry, alias_map)?
        {
            let expr = expr_to_sql(&dim.expression, &alias);
            return Ok((expr, FieldKind::Dimension, Some(alias)));
        }
        if let Some((_, alias, _)) =
            self.resolve_measure_inner(name, flow, registry, alias_map)?
        {
            return Ok((
                SqlExpr::Column {
                    table: None,
                    name: name.to_string(),
                },
                FieldKind::Measure,
                Some(alias),
            ));
        }
        Err(SemaflowError::Validation(format!(
            "field {name} not found in flow {}",
            flow.name
        )))
    }
}

fn parse_qualified(name: &str) -> Option<(&str, &str)> {
    let mut parts = name.splitn(2, '.');
    let alias = parts.next()?;
    let field = parts.next()?;
    if alias.is_empty() || field.is_empty() {
        return None;
    }
    Some((alias, field))
}

fn render_filter_expr(base_expr: SqlExpr, filter: &Filter) -> SqlExpr {
    match filter.op {
        FilterOp::In | FilterOp::NotIn => {
            let list = match &filter.value {
                serde_json::Value::Array(items) => {
                    items.iter().map(|v| SqlExpr::Literal(v.clone())).collect()
                }
                other => vec![SqlExpr::Literal(other.clone())],
            };
            SqlExpr::InList {
                expr: Box::new(base_expr),
                list,
                negated: matches!(filter.op, FilterOp::NotIn),
            }
        }
        _ => {
            let op = match filter.op {
                FilterOp::Eq => SqlBinaryOperator::Eq,
                FilterOp::Neq => SqlBinaryOperator::Neq,
                FilterOp::Gt => SqlBinaryOperator::Gt,
                FilterOp::Gte => SqlBinaryOperator::Gte,
                FilterOp::Lt => SqlBinaryOperator::Lt,
                FilterOp::Lte => SqlBinaryOperator::Lte,
                FilterOp::Like => SqlBinaryOperator::Like,
                FilterOp::ILike => SqlBinaryOperator::ILike,
                FilterOp::In | FilterOp::NotIn => unreachable!(),
            };
            SqlExpr::BinaryOp {
                op,
                left: Box::new(base_expr),
                right: Box::new(SqlExpr::Literal(filter.value.clone())),
            }
        }
    }
}

fn expr_to_sql(expr: &Expr, alias: &str) -> SqlExpr {
    match expr {
        Expr::Column { column } => SqlExpr::Column {
            table: Some(alias.to_string()),
            name: column.clone(),
        },
        Expr::Literal { value } => SqlExpr::Literal(value.clone()),
        Expr::Func { func, args } => SqlExpr::Function {
            func: func.clone(),
            args: args.iter().map(|a| expr_to_sql(a, alias)).collect(),
        },
        Expr::Case {
            branches,
            else_expr,
        } => SqlExpr::Case {
            branches: branches
                .iter()
                .map(|b| (expr_to_sql(&b.when, alias), expr_to_sql(&b.then, alias)))
                .collect(),
            else_expr: Box::new(expr_to_sql(else_expr, alias)),
        },
        Expr::Binary { op, left, right } => {
            let op = match op {
                crate::flows::BinaryOp::Add => SqlBinaryOperator::Add,
                crate::flows::BinaryOp::Subtract => SqlBinaryOperator::Subtract,
                crate::flows::BinaryOp::Multiply => SqlBinaryOperator::Multiply,
                crate::flows::BinaryOp::Divide => SqlBinaryOperator::Divide,
                crate::flows::BinaryOp::Modulo => SqlBinaryOperator::Modulo,
            };
            SqlExpr::BinaryOp {
                op,
                left: Box::new(expr_to_sql(left, alias)),
                right: Box::new(expr_to_sql(right, alias)),
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum FieldKind {
    Dimension,
    Measure,
}
