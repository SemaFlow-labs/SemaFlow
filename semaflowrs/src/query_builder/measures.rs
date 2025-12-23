use std::collections::HashMap;

use crate::error::{Result, SemaflowError};
use crate::flows::{BinaryOp, Expr, FormulaAst, Function, Measure};
use crate::sql_ast::{SqlBinaryOperator, SqlExpr};

use super::render::expr_to_sql;
use super::render::render_post_expr;

// Re-export from shared module
pub(crate) use crate::expr_utils::collect_measure_refs;

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

/// Apply filter to a simple measure and wrap in aggregate.
/// Panics if called on a formula measure (use is_simple() to check).
pub(crate) fn apply_measure_filter(
    measure: &Measure,
    base_expr: SqlExpr,
    alias: &str,
    supports_filtered_aggregates: bool,
) -> Result<SqlExpr> {
    // This function only works for simple measures (with agg)
    let agg = measure.agg.as_ref().expect(
        "apply_measure_filter called on formula measure - this is a bug, use measure.is_simple() to check"
    );

    if let Some(filter) = &measure.filter {
        let filter = normalize_freeform(filter);
        let filter_sql = expr_to_sql(&filter, alias);
        if supports_filtered_aggregates {
            Ok(SqlExpr::FilteredAggregate {
                agg: agg.clone(),
                expr: Box::new(base_expr),
                filter: Box::new(filter_sql),
            })
        } else {
            let filtered_expr = SqlExpr::Case {
                branches: vec![(filter_sql, base_expr)],
                else_expr: Box::new(SqlExpr::Literal(serde_json::Value::Null)),
            };
            Ok(SqlExpr::Aggregate {
                agg: agg.clone(),
                expr: Box::new(filtered_expr),
            })
        }
    } else {
        Ok(SqlExpr::Aggregate {
            agg: agg.clone(),
            expr: Box::new(base_expr),
        })
    }
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

// ============================================================================
// Formula SQL Generation (for complex measures)
// ============================================================================

/// Convert a FormulaAst to SqlExpr.
///
/// The `alias` parameter is the table alias for qualifying columns (e.g., "o" → "o"."amount").
/// The `measure_resolver` is called for measure references, allowing resolution from base measures.
///
/// Division operations are automatically wrapped in NULLIF for safety.
#[allow(dead_code)] // Prepared for formula measure integration
pub(crate) fn formula_to_sql(
    ast: &FormulaAst,
    alias: &str,
    measure_resolver: &mut impl FnMut(&str) -> Result<SqlExpr>,
) -> Result<SqlExpr> {
    match ast {
        FormulaAst::Aggregation {
            agg,
            column,
            filter,
        } => {
            let base_expr = SqlExpr::Column {
                table: Some(alias.to_string()),
                name: column.clone(),
            };

            // Handle filtered aggregation
            if let Some(filter_ast) = filter {
                let filter_sql = formula_to_sql(filter_ast, alias, measure_resolver)?;
                Ok(SqlExpr::FilteredAggregate {
                    agg: agg.clone(),
                    expr: Box::new(base_expr),
                    filter: Box::new(filter_sql),
                })
            } else {
                Ok(SqlExpr::Aggregate {
                    agg: agg.clone(),
                    expr: Box::new(base_expr),
                })
            }
        }

        FormulaAst::MeasureRef { name } => measure_resolver(name),

        FormulaAst::Column { column } => {
            // Handle qualified columns like "o.amount" vs simple "amount"
            if let Some((table, col)) = column.split_once('.') {
                Ok(SqlExpr::Column {
                    table: Some(table.to_string()),
                    name: col.to_string(),
                })
            } else {
                Ok(SqlExpr::Column {
                    table: Some(alias.to_string()),
                    name: column.clone(),
                })
            }
        }

        FormulaAst::Literal { value } => Ok(SqlExpr::Literal(value.clone())),

        FormulaAst::Binary { op, left, right } => {
            let left_sql = formula_to_sql(left, alias, measure_resolver)?;
            let right_sql = formula_to_sql(right, alias, measure_resolver)?;

            let sql_op = match op {
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

            // Wrap division in NULLIF for safety: x / y → x / NULLIF(y, 0)
            let right_sql = if matches!(op, BinaryOp::Divide) {
                SqlExpr::Function {
                    func: Function::NullIf,
                    args: vec![right_sql, SqlExpr::Literal(serde_json::json!(0))],
                }
            } else {
                right_sql
            };

            Ok(SqlExpr::BinaryOp {
                op: sql_op,
                left: Box::new(left_sql),
                right: Box::new(right_sql),
            })
        }

        FormulaAst::Function { name, args } => {
            let sql_args: Vec<SqlExpr> = args
                .iter()
                .map(|a| formula_to_sql(a, alias, measure_resolver))
                .collect::<Result<Vec<_>>>()?;

            // Map function names to Function enum
            let func = match name.to_lowercase().as_str() {
                "round" => Function::Round,
                "abs" => Function::Abs,
                "floor" => Function::Floor,
                "ceil" => Function::Ceil,
                "coalesce" => Function::Coalesce,
                "ifnull" => Function::IfNull,
                "nullif" => Function::NullIf,
                "safe_divide" => Function::SafeDivide,
                "greatest" => Function::Greatest,
                "least" => Function::Least,
                "lower" => Function::Lower,
                "upper" => Function::Upper,
                "length" => Function::Length,
                "trim" => Function::Trim,
                "concat" => Function::Concat,
                "power" => Function::Power,
                "sqrt" => Function::Sqrt,
                "ln" => Function::Ln,
                "log10" => Function::Log10,
                "exp" => Function::Exp,
                "sign" => Function::Sign,
                unknown => {
                    return Err(SemaflowError::Validation(format!(
                        "Unknown function '{}' in formula. Supported: round, abs, floor, ceil, \
                         coalesce, ifnull, nullif, safe_divide, greatest, least, lower, upper, \
                         length, trim, concat, power, sqrt, ln, log10, exp, sign",
                        unknown
                    )));
                }
            };

            Ok(SqlExpr::Function {
                func,
                args: sql_args,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr_parser::parse_formula;
    use crate::flows::Aggregation;

    /// Helper to resolve measure refs - just returns a placeholder column for testing
    fn mock_resolver(name: &str) -> Result<SqlExpr> {
        Ok(SqlExpr::Column {
            table: None,
            name: name.to_string(),
        })
    }

    #[test]
    fn formula_sum_aggregation() {
        let ast = parse_formula("sum(amount)").unwrap();
        let sql = formula_to_sql(&ast, "o", &mut mock_resolver).unwrap();

        if let SqlExpr::Aggregate { agg, expr } = sql {
            assert!(matches!(agg, Aggregation::Sum));
            if let SqlExpr::Column { table, name } = *expr {
                assert_eq!(table, Some("o".to_string()));
                assert_eq!(name, "amount");
            } else {
                panic!("Expected column in aggregate");
            }
        } else {
            panic!("Expected aggregate expression");
        }
    }

    #[test]
    fn formula_division_with_nullif() {
        let ast = parse_formula("sum(a) / count(b)").unwrap();
        let sql = formula_to_sql(&ast, "t", &mut mock_resolver).unwrap();

        // Should produce: SUM(t.a) / NULLIF(COUNT(t.b), 0)
        if let SqlExpr::BinaryOp { op, left, right } = sql {
            assert!(matches!(op, SqlBinaryOperator::Divide));
            assert!(matches!(*left, SqlExpr::Aggregate { .. }));
            // Right side should be NULLIF wrapped
            if let SqlExpr::Function { func, args } = *right {
                assert!(matches!(func, Function::NullIf));
                assert_eq!(args.len(), 2);
            } else {
                panic!("Expected NULLIF function for divisor");
            }
        } else {
            panic!("Expected binary operation");
        }
    }

    #[test]
    fn formula_function_call() {
        let ast = parse_formula("round(sum(amount), 2)").unwrap();
        let sql = formula_to_sql(&ast, "o", &mut mock_resolver).unwrap();

        if let SqlExpr::Function { func, args } = sql {
            assert!(matches!(func, Function::Round));
            assert_eq!(args.len(), 2);
            assert!(matches!(&args[0], SqlExpr::Aggregate { .. }));
            assert!(matches!(&args[1], SqlExpr::Literal(_)));
        } else {
            panic!("Expected function call");
        }
    }

    #[test]
    fn formula_qualified_column() {
        let ast = parse_formula("o.amount").unwrap();
        let sql = formula_to_sql(&ast, "ignored", &mut mock_resolver).unwrap();

        if let SqlExpr::Column { table, name } = sql {
            assert_eq!(table, Some("o".to_string()));
            assert_eq!(name, "amount");
        } else {
            panic!("Expected column");
        }
    }

    #[test]
    fn formula_arithmetic() {
        let ast = parse_formula("sum(a) + sum(b) * 2").unwrap();
        let sql = formula_to_sql(&ast, "t", &mut mock_resolver).unwrap();

        // Should be: (SUM(a) + (SUM(b) * 2)) due to precedence
        assert!(matches!(
            sql,
            SqlExpr::BinaryOp {
                op: SqlBinaryOperator::Add,
                ..
            }
        ));
    }

    #[test]
    fn formula_unknown_function_error() {
        let ast = parse_formula("unknown_func(x)").unwrap();
        let result = formula_to_sql(&ast, "t", &mut mock_resolver);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Unknown function 'unknown_func'"));
    }
}
