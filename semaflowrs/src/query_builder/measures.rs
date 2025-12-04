use std::collections::HashMap;

use crate::error::{Result, SemaflowError};
use crate::flows::{Expr, Measure};
use crate::sql_ast::SqlExpr;

use super::render::expr_to_sql;
use super::render::render_post_expr;

pub(crate) fn validate_no_measure_refs(expr: &Expr) -> Result<()> {
    match expr {
        Expr::MeasureRef { name } => Err(SemaflowError::Validation(format!(
            "measure references are not allowed in filters ({name})"
        ))),
        Expr::Column { .. } | Expr::Literal { .. } => Ok(()),
        Expr::Func { args, .. } => args.iter().try_for_each(validate_no_measure_refs),
        Expr::Case {
            branches,
            else_expr,
        } => {
            for b in branches {
                validate_no_measure_refs(&b.when)?;
                validate_no_measure_refs(&b.then)?;
            }
            validate_no_measure_refs(else_expr)
        }
        Expr::Binary { left, right, .. } => {
            validate_no_measure_refs(left)?;
            validate_no_measure_refs(right)
        }
    }
}

pub(crate) fn apply_measure_filter(
    measure: &Measure,
    base_expr: SqlExpr,
    alias: &str,
) -> Result<SqlExpr> {
    let filtered_expr = if let Some(filter) = &measure.filter {
        let filter = normalize_freeform(filter);
        let filter_sql = expr_to_sql(&filter, alias);
        SqlExpr::Case {
            branches: vec![(filter_sql, base_expr)],
            else_expr: Box::new(SqlExpr::Literal(serde_json::Value::Null)),
        }
    } else {
        base_expr
    };
    Ok(SqlExpr::Aggregate {
        agg: measure.agg.clone(),
        expr: Box::new(filtered_expr),
    })
}

pub(crate) fn resolve_measure_with_posts(
    name: &str,
    lookup: &HashMap<String, (&str, &Measure)>,
    base_exprs: &HashMap<String, SqlExpr>,
    cache: &mut HashMap<String, SqlExpr>,
    stack: &mut Vec<String>,
) -> Result<SqlExpr> {
    if let Some(expr) = cache.get(name) {
        return Ok(expr.clone());
    }
    if stack.contains(&name.to_string()) {
        return Err(SemaflowError::Validation(format!(
            "cyclic measure reference involving {name}"
        )));
    }
    let (_alias, measure) = lookup
        .get(name)
        .ok_or_else(|| SemaflowError::Validation(format!("unknown measure {name}")))?;
    if measure.post_expr.is_some() && !stack.is_empty() {
        return Err(SemaflowError::Validation(format!(
            "derived measures cannot be referenced ({name})"
        )));
    }
    if let Some(post) = &measure.post_expr {
        let post = normalize_freeform(post);
        stack.push(name.to_string());
        let mut resolver =
            |ref_name: &str| resolve_measure_with_posts(ref_name, lookup, base_exprs, cache, stack);
        let expr = render_post_expr(&post, &mut resolver)?;
        stack.pop();
        cache.insert(name.to_string(), expr.clone());
        Ok(expr)
    } else if let Some(base) = base_exprs.get(name) {
        cache.insert(name.to_string(), base.clone());
        Ok(base.clone())
    } else {
        Err(SemaflowError::Validation(format!(
            "measure {name} missing base expression"
        )))
    }
}

pub(crate) fn collect_measure_refs(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::MeasureRef { name } => out.push(name.clone()),
        Expr::Func { args, .. } => args.iter().for_each(|a| collect_measure_refs(a, out)),
        Expr::Case {
            branches,
            else_expr,
        } => {
            for b in branches {
                collect_measure_refs(&b.when, out);
                collect_measure_refs(&b.then, out);
            }
            collect_measure_refs(else_expr, out);
        }
        Expr::Binary { left, right, .. } => {
            collect_measure_refs(left, out);
            collect_measure_refs(right, out);
        }
        _ => {}
    }
}

pub(crate) fn normalize_freeform(expr: &Expr) -> Expr {
    if let Expr::Column { column } = expr {
        if column.contains(' ')
            || column.contains('=')
            || column.contains('(')
            || column.contains(')')
        {
            if let Ok(parsed) = crate::expr_parser::parse_expr(column) {
                return parsed;
            }
        }
    }
    expr.clone()
}
