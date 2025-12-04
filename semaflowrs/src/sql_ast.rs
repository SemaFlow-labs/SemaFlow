use serde_json::Value;

use crate::dialect::Dialect;
use crate::flows::{Aggregation, Function, SortDirection};

#[derive(Debug, Clone)]
pub enum SqlExpr {
    Column {
        table: Option<String>,
        name: String,
    },
    Literal(Value),
    Function {
        func: Function,
        args: Vec<SqlExpr>,
    },
    Case {
        branches: Vec<(SqlExpr, SqlExpr)>,
        else_expr: Box<SqlExpr>,
    },
    BinaryOp {
        op: SqlBinaryOperator,
        left: Box<SqlExpr>,
        right: Box<SqlExpr>,
    },
    Aggregate {
        agg: Aggregation,
        expr: Box<SqlExpr>,
    },
    InList {
        expr: Box<SqlExpr>,
        list: Vec<SqlExpr>,
        negated: bool,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum SqlBinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    And,
    Or,
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
    ILike,
}

#[derive(Debug, Clone)]
pub struct SelectItem {
    pub expr: SqlExpr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub enum SqlJoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone)]
pub struct Join {
    pub join_type: SqlJoinType,
    pub table: TableRef,
    pub on: Vec<SqlExpr>,
}

#[derive(Debug, Clone)]
pub struct OrderItem {
    pub expr: SqlExpr,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, Default)]
pub struct SelectQuery {
    pub select: Vec<SelectItem>,
    pub from: TableRef,
    pub joins: Vec<Join>,
    pub filters: Vec<SqlExpr>,
    pub group_by: Vec<SqlExpr>,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

pub struct SqlRenderer<'d> {
    dialect: &'d dyn Dialect,
}

impl<'d> SqlRenderer<'d> {
    pub fn new(dialect: &'d dyn Dialect) -> Self {
        Self { dialect }
    }

    pub fn render_select(&self, query: &SelectQuery) -> String {
        let select_items: Vec<String> = query
            .select
            .iter()
            .map(|item| {
                let expr_sql = self.render_expr(&item.expr);
                match &item.alias {
                    Some(alias) => format!("{expr_sql} AS {}", self.dialect.quote_ident(alias)),
                    None => expr_sql,
                }
            })
            .collect();

        let mut sql = format!(
            "SELECT {} FROM {}",
            select_items.join(", "),
            self.render_table_ref(&query.from)
        );

        for join in &query.joins {
            let join_kw = match join.join_type {
                SqlJoinType::Inner => "JOIN",
                SqlJoinType::Left => "LEFT JOIN",
                SqlJoinType::Right => "RIGHT JOIN",
                SqlJoinType::Full => "FULL JOIN",
            };
            let on_clause: Vec<String> = join.on.iter().map(|e| self.render_expr(e)).collect();
            sql.push_str(&format!(
                " {join_kw} {} ON {}",
                self.render_table_ref(&join.table),
                on_clause.join(" AND ")
            ));
        }

        if !query.filters.is_empty() {
            let filters: Vec<String> = query.filters.iter().map(|f| self.render_expr(f)).collect();
            sql.push_str(&format!(" WHERE {}", filters.join(" AND ")));
        }

        if !query.group_by.is_empty() {
            let groups: Vec<String> = query.group_by.iter().map(|g| self.render_expr(g)).collect();
            sql.push_str(&format!(" GROUP BY {}", groups.join(", ")));
        }

        if !query.order_by.is_empty() {
            let orders: Vec<String> = query
                .order_by
                .iter()
                .map(|o| {
                    let expr = self.render_expr(&o.expr);
                    let dir = match o.direction {
                        SortDirection::Asc => "ASC",
                        SortDirection::Desc => "DESC",
                    };
                    format!("{expr} {dir}")
                })
                .collect();
            sql.push_str(&format!(" ORDER BY {}", orders.join(", ")));
        }

        if let Some(limit) = query.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }
        if let Some(offset) = query.offset {
            sql.push_str(&format!(" OFFSET {}", offset));
        }

        sql
    }

    fn render_table_ref(&self, table: &TableRef) -> String {
        match &table.alias {
            Some(alias) => format!(
                "{} {}",
                self.dialect.quote_ident(&table.name),
                self.dialect.quote_ident(alias)
            ),
            None => self.dialect.quote_ident(&table.name),
        }
    }

    fn render_expr(&self, expr: &SqlExpr) -> String {
        match expr {
            SqlExpr::Column { table, name } => match table {
                Some(t) => format!(
                    "{}.{}",
                    self.dialect.quote_ident(t),
                    self.dialect.quote_ident(name)
                ),
                None => self.dialect.quote_ident(name),
            },
            SqlExpr::Literal(v) => self.dialect.render_literal(v),
            SqlExpr::Function { func, args } => {
                let rendered_args: Vec<String> = args.iter().map(|a| self.render_expr(a)).collect();
                self.dialect.render_function(func, rendered_args)
            }
            SqlExpr::Case {
                branches,
                else_expr,
            } => {
                let mut parts = Vec::new();
                parts.push("CASE".to_string());
                for (when, then) in branches {
                    parts.push(format!(
                        " WHEN {} THEN {}",
                        self.render_expr(when),
                        self.render_expr(then)
                    ));
                }
                parts.push(format!(" ELSE {} END", self.render_expr(else_expr)));
                parts.join("")
            }
            SqlExpr::BinaryOp { op, left, right } => {
                let op_sql = match op {
                    SqlBinaryOperator::Add => "+",
                    SqlBinaryOperator::Subtract => "-",
                    SqlBinaryOperator::Multiply => "*",
                    SqlBinaryOperator::Divide => "/",
                    SqlBinaryOperator::Modulo => "%",
                    SqlBinaryOperator::And => "AND",
                    SqlBinaryOperator::Or => "OR",
                    SqlBinaryOperator::Eq => "=",
                    SqlBinaryOperator::Neq => "!=",
                    SqlBinaryOperator::Gt => ">",
                    SqlBinaryOperator::Gte => ">=",
                    SqlBinaryOperator::Lt => "<",
                    SqlBinaryOperator::Lte => "<=",
                    SqlBinaryOperator::Like => "LIKE",
                    SqlBinaryOperator::ILike => "ILIKE",
                };
                format!(
                    "({} {} {})",
                    self.render_expr(left),
                    op_sql,
                    self.render_expr(right)
                )
            }
            SqlExpr::Aggregate { agg, expr } => self
                .dialect
                .render_aggregation(agg, &self.render_expr(expr)),
            SqlExpr::InList {
                expr,
                list,
                negated,
            } => {
                let rendered_values: Vec<String> =
                    list.iter().map(|v| self.render_expr(v)).collect();
                let not_kw = if *negated { "NOT " } else { "" };
                format!(
                    "{} {}IN ({})",
                    self.render_expr(expr),
                    not_kw,
                    rendered_values.join(", ")
                )
            }
        }
    }
}
