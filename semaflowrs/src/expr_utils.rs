//! Expression utility functions.
//!
//! Shared helpers for traversing and analyzing semantic expressions.

use crate::flows::Expr;

/// Recursively collect all measure references from an expression.
///
/// This is used for:
/// - Auto-including dependent measures in query building
/// - Validating that derived measures don't reference other derived measures
pub fn collect_measure_refs(expr: &Expr, out: &mut Vec<String>) {
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
        Expr::Column { .. } | Expr::Literal { .. } => {}
    }
}

/// Extract a simple column name from an expression if it's a direct column reference.
pub fn simple_column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column { column } => Some(column.as_str()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flows::BinaryOp;

    #[test]
    fn collects_measure_refs_from_binary() {
        let expr = Expr::Binary {
            op: BinaryOp::Divide,
            left: Box::new(Expr::MeasureRef {
                name: "sum_amount".to_string(),
            }),
            right: Box::new(Expr::MeasureRef {
                name: "count_orders".to_string(),
            }),
        };
        let mut refs = Vec::new();
        collect_measure_refs(&expr, &mut refs);
        assert_eq!(refs, vec!["sum_amount", "count_orders"]);
    }

    #[test]
    fn simple_column_extracts_name() {
        let expr = Expr::Column {
            column: "amount".to_string(),
        };
        assert_eq!(simple_column_name(&expr), Some("amount"));
    }

    #[test]
    fn simple_column_returns_none_for_complex() {
        let expr = Expr::MeasureRef {
            name: "total".to_string(),
        };
        assert_eq!(simple_column_name(&expr), None);
    }
}
