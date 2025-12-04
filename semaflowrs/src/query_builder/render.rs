use crate::flows::{BinaryOp, Expr};
use crate::sql_ast::{SqlBinaryOperator, SqlExpr};

pub(crate) fn expr_to_sql(expr: &Expr, alias: &str) -> SqlExpr {
    match expr {
        Expr::Column { column } => SqlExpr::Column {
            table: Some(alias.to_string()),
            name: column.clone(),
        },
        Expr::Literal { value } => SqlExpr::Literal(value.clone()),
        Expr::MeasureRef { name } => SqlExpr::Column {
            table: None,
            name: name.clone(),
        },
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
                BinaryOp::Add => SqlBinaryOperator::Add,
                BinaryOp::Subtract => SqlBinaryOperator::Subtract,
                BinaryOp::Multiply => SqlBinaryOperator::Multiply,
                BinaryOp::Divide => SqlBinaryOperator::Divide,
                BinaryOp::Modulo => SqlBinaryOperator::Modulo,
                BinaryOp::Eq => SqlBinaryOperator::Eq,
                BinaryOp::Neq => SqlBinaryOperator::Neq,
                BinaryOp::Gt => SqlBinaryOperator::Gt,
                BinaryOp::Gte => SqlBinaryOperator::Gte,
                BinaryOp::Lt => SqlBinaryOperator::Lt,
                BinaryOp::Lte => SqlBinaryOperator::Lte,
                BinaryOp::And => SqlBinaryOperator::And,
                BinaryOp::Or => SqlBinaryOperator::Or,
            };
            SqlExpr::BinaryOp {
                op,
                left: Box::new(expr_to_sql(left, alias)),
                right: Box::new(expr_to_sql(right, alias)),
            }
        }
    }
}

pub(crate) fn render_post_expr(
    expr: &Expr,
    measure_resolver: &mut impl FnMut(&str) -> crate::error::Result<SqlExpr>,
) -> crate::error::Result<SqlExpr> {
    match expr {
        Expr::MeasureRef { name } => measure_resolver(name),
        Expr::Column { column } => Ok(SqlExpr::Column {
            table: None,
            name: column.clone(),
        }),
        Expr::Literal { value } => Ok(SqlExpr::Literal(value.clone())),
        Expr::Func { func, args } => Ok(SqlExpr::Function {
            func: func.clone(),
            args: args
                .iter()
                .map(|a| render_post_expr(a, measure_resolver))
                .collect::<crate::error::Result<Vec<_>>>()?,
        }),
        Expr::Case {
            branches,
            else_expr,
        } => {
            let rendered = branches
                .iter()
                .map(|b| {
                    Ok((
                        render_post_expr(&b.when, measure_resolver)?,
                        render_post_expr(&b.then, measure_resolver)?,
                    ))
                })
                .collect::<crate::error::Result<Vec<_>>>()?;
            Ok(SqlExpr::Case {
                branches: rendered,
                else_expr: Box::new(render_post_expr(else_expr, measure_resolver)?),
            })
        }
        Expr::Binary { op, left, right } => {
            let op = match op {
                BinaryOp::Add => SqlBinaryOperator::Add,
                BinaryOp::Subtract => SqlBinaryOperator::Subtract,
                BinaryOp::Multiply => SqlBinaryOperator::Multiply,
                BinaryOp::Divide => SqlBinaryOperator::Divide,
                BinaryOp::Modulo => SqlBinaryOperator::Modulo,
                BinaryOp::Eq => SqlBinaryOperator::Eq,
                BinaryOp::Neq => SqlBinaryOperator::Neq,
                BinaryOp::Gt => SqlBinaryOperator::Gt,
                BinaryOp::Gte => SqlBinaryOperator::Gte,
                BinaryOp::Lt => SqlBinaryOperator::Lt,
                BinaryOp::Lte => SqlBinaryOperator::Lte,
                BinaryOp::And => SqlBinaryOperator::And,
                BinaryOp::Or => SqlBinaryOperator::Or,
            };
            Ok(SqlExpr::BinaryOp {
                op,
                left: Box::new(render_post_expr(left, measure_resolver)?),
                right: Box::new(render_post_expr(right, measure_resolver)?),
            })
        }
    }
}
