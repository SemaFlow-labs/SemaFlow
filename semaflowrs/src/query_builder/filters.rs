use crate::flows::{Filter, FilterOp};
use crate::sql_ast::{SqlBinaryOperator, SqlExpr};

pub(crate) fn render_filter_expr(base_expr: SqlExpr, filter: &Filter) -> SqlExpr {
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
