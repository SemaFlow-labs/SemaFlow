use crate::error::SemaflowError;
use crate::flows::{BinaryOp, Expr, Function};

/// Extremely small, safe parser for concise filter/post_expr strings.
/// Supports:
/// - safe_divide(arg1, arg2)
/// - simple binary comparisons on identifiers/literals (==, !=, >, >=, <, <=)
/// - bare identifiers or string/number literals
pub fn parse_expr(input: &str) -> Result<Expr, SemaflowError> {
    let s = input.trim();
    if let Some(expr) = parse_safe_divide(s) {
        return Ok(expr);
    }
    if let Some(expr) = parse_binary(s) {
        return Ok(expr);
    }
    if let Some(expr) = parse_literal(s) {
        return Ok(expr);
    }
    if is_ident(s) {
        return Ok(Expr::Column {
            column: s.to_string(),
        });
    }
    Err(SemaflowError::Validation(format!(
        "unable to parse expression '{s}'"
    )))
}

fn parse_safe_divide(s: &str) -> Option<Expr> {
    let body = s.strip_prefix("safe_divide(")?.strip_suffix(')')?;
    let parts: Vec<&str> = body.split(',').map(|p| p.trim()).collect();
    if parts.len() != 2 {
        return None;
    }
    Some(Expr::Func {
        func: Function::SafeDivide,
        args: parts
            .iter()
            .map(|p| {
                if is_ident(p) {
                    Expr::MeasureRef {
                        name: p.to_string(),
                    }
                } else {
                    Expr::Column {
                        column: p.to_string(),
                    }
                }
            })
            .collect(),
    })
}

fn parse_binary(s: &str) -> Option<Expr> {
    for op in ["==", "!=", ">=", "<=", ">", "<"] {
        if let Some(idx) = s.find(op) {
            let (left, right_with_op) = s.split_at(idx);
            let right = &right_with_op[op.len()..];
            let left = left.trim();
            let right = right.trim();
            let right_expr = parse_literal(right).or_else(|| {
                Some(Expr::Column {
                    column: right.to_string(),
                })
            })?;
            let bop = match op {
                "==" => BinaryOp::Eq,
                "!=" => BinaryOp::Neq,
                ">" => BinaryOp::Gt,
                ">=" => BinaryOp::Gte,
                "<" => BinaryOp::Lt,
                "<=" => BinaryOp::Lte,
                _ => return None,
            };
            return Some(Expr::Binary {
                op: bop,
                left: Box::new(Expr::Column {
                    column: left.to_string(),
                }),
                right: Box::new(right_expr),
            });
        }
    }
    None
}

fn parse_literal(s: &str) -> Option<Expr> {
    if let Some(stripped) = s.strip_prefix('\'').and_then(|r| r.strip_suffix('\'')) {
        return Some(Expr::Literal {
            value: serde_json::Value::String(stripped.to_string()),
        });
    }
    if let Ok(v) = s.parse::<i64>() {
        return Some(Expr::Literal {
            value: serde_json::Value::Number(v.into()),
        });
    }
    if let Ok(v) = s.parse::<f64>() {
        if let Some(num) = serde_json::Number::from_f64(v) {
            return Some(Expr::Literal {
                value: serde_json::Value::Number(num),
            });
        }
    }
    None
}

fn is_ident(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
}
