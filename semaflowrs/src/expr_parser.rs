use crate::error::SemaflowError;
use crate::flows::{Aggregation, BinaryOp, Expr, FormulaAst, Function};

// ============================================================================
// Formula Parser (for complex measures)
// ============================================================================

/// Token types for the formula lexer
#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Number(f64),
    StringLit(String),
    LParen,
    RParen,
    Comma,
    Plus,
    Minus,
    Star,
    Slash,
    Gt,
    Gte,
    Lt,
    Lte,
    Eq,
    Neq,
}

/// Tokenizer for formula strings
struct Lexer<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn advance(&mut self) -> Option<char> {
        let c = self.peek_char()?;
        self.pos += c.len_utf8();
        Some(c)
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek_char() {
            if c.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn read_ident(&mut self) -> String {
        let start = self.pos;
        while let Some(c) = self.peek_char() {
            if c.is_ascii_alphanumeric() || c == '_' || c == '.' {
                self.advance();
            } else {
                break;
            }
        }
        self.input[start..self.pos].to_string()
    }

    fn read_number(&mut self) -> Result<f64, SemaflowError> {
        let start = self.pos;
        while let Some(c) = self.peek_char() {
            if c.is_ascii_digit() || c == '.' {
                self.advance();
            } else {
                break;
            }
        }
        let s = &self.input[start..self.pos];
        s.parse::<f64>()
            .map_err(|_| SemaflowError::Validation(format!("invalid number: {}", s)))
    }

    fn read_string(&mut self) -> Result<String, SemaflowError> {
        let quote = self.advance().unwrap(); // consume opening quote
        let start = self.pos;
        while let Some(c) = self.peek_char() {
            if c == quote {
                let s = self.input[start..self.pos].to_string();
                self.advance(); // consume closing quote
                return Ok(s);
            }
            self.advance();
        }
        Err(SemaflowError::Validation("unterminated string".to_string()))
    }

    fn next_token(&mut self) -> Result<Option<Token>, SemaflowError> {
        self.skip_whitespace();
        let c = match self.peek_char() {
            Some(c) => c,
            None => return Ok(None),
        };

        let token = match c {
            '(' => {
                self.advance();
                Token::LParen
            }
            ')' => {
                self.advance();
                Token::RParen
            }
            ',' => {
                self.advance();
                Token::Comma
            }
            '+' => {
                self.advance();
                Token::Plus
            }
            '-' => {
                self.advance();
                Token::Minus
            }
            '*' => {
                self.advance();
                Token::Star
            }
            '/' => {
                self.advance();
                Token::Slash
            }
            '>' => {
                self.advance();
                if self.peek_char() == Some('=') {
                    self.advance();
                    Token::Gte
                } else {
                    Token::Gt
                }
            }
            '<' => {
                self.advance();
                if self.peek_char() == Some('=') {
                    self.advance();
                    Token::Lte
                } else {
                    Token::Lt
                }
            }
            '=' => {
                self.advance();
                if self.peek_char() == Some('=') {
                    self.advance();
                }
                Token::Eq
            }
            '!' => {
                self.advance();
                if self.peek_char() == Some('=') {
                    self.advance();
                    Token::Neq
                } else {
                    return Err(SemaflowError::Validation(format!(
                        "unexpected character '!' at position {}",
                        self.pos
                    )));
                }
            }
            '\'' | '"' => Token::StringLit(self.read_string()?),
            c if c.is_ascii_digit() => Token::Number(self.read_number()?),
            c if c.is_ascii_alphabetic() || c == '_' => Token::Ident(self.read_ident()),
            _ => {
                return Err(SemaflowError::Validation(format!(
                    "unexpected character '{}' at position {}",
                    c, self.pos
                )))
            }
        };
        Ok(Some(token))
    }

    fn tokenize(&mut self) -> Result<Vec<Token>, SemaflowError> {
        let mut tokens = Vec::new();
        while let Some(tok) = self.next_token()? {
            tokens.push(tok);
        }
        Ok(tokens)
    }
}

/// Recursive descent parser for formula expressions
struct FormulaParser {
    tokens: Vec<Token>,
    pos: usize,
    raw: String,
}

impl FormulaParser {
    fn new(raw: String, tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            pos: 0,
            raw,
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<&Token> {
        let tok = self.tokens.get(self.pos);
        self.pos += 1;
        tok
    }

    fn expect(&mut self, expected: &Token) -> Result<(), SemaflowError> {
        match self.peek() {
            Some(t) if t == expected => {
                self.advance();
                Ok(())
            }
            Some(t) => Err(SemaflowError::Validation(format!(
                "Formula parse error in '{}': expected {:?}, found {:?}",
                self.raw, expected, t
            ))),
            None => Err(SemaflowError::Validation(format!(
                "Formula parse error in '{}': unexpected end of expression",
                self.raw
            ))),
        }
    }

    /// Parse the complete formula
    fn parse(&mut self) -> Result<FormulaAst, SemaflowError> {
        let expr = self.parse_comparison()?;
        if self.pos < self.tokens.len() {
            return Err(SemaflowError::Validation(format!(
                "Formula parse error in '{}': unexpected token {:?} at end",
                self.raw,
                self.tokens.get(self.pos)
            )));
        }
        Ok(expr)
    }

    /// Parse comparison operators (lowest precedence)
    fn parse_comparison(&mut self) -> Result<FormulaAst, SemaflowError> {
        let left = self.parse_additive()?;

        let op = match self.peek() {
            Some(Token::Gt) => BinaryOp::Gt,
            Some(Token::Gte) => BinaryOp::Gte,
            Some(Token::Lt) => BinaryOp::Lt,
            Some(Token::Lte) => BinaryOp::Lte,
            Some(Token::Eq) => BinaryOp::Eq,
            Some(Token::Neq) => BinaryOp::Neq,
            _ => return Ok(left),
        };

        self.advance();
        let right = self.parse_additive()?;
        Ok(FormulaAst::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        })
    }

    /// Parse + and - (left-associative)
    fn parse_additive(&mut self) -> Result<FormulaAst, SemaflowError> {
        let mut left = self.parse_multiplicative()?;

        loop {
            let op = match self.peek() {
                Some(Token::Plus) => BinaryOp::Add,
                Some(Token::Minus) => BinaryOp::Subtract,
                _ => return Ok(left),
            };
            self.advance();
            let right = self.parse_multiplicative()?;
            left = FormulaAst::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
    }

    /// Parse * and / (left-associative, higher precedence)
    fn parse_multiplicative(&mut self) -> Result<FormulaAst, SemaflowError> {
        let mut left = self.parse_unary()?;

        loop {
            let op = match self.peek() {
                Some(Token::Star) => BinaryOp::Multiply,
                Some(Token::Slash) => BinaryOp::Divide,
                _ => return Ok(left),
            };
            self.advance();
            let right = self.parse_unary()?;
            left = FormulaAst::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
    }

    /// Parse unary minus
    fn parse_unary(&mut self) -> Result<FormulaAst, SemaflowError> {
        if let Some(Token::Minus) = self.peek() {
            self.advance();
            let expr = self.parse_unary()?;
            return Ok(FormulaAst::Binary {
                op: BinaryOp::Multiply,
                left: Box::new(FormulaAst::Literal {
                    value: serde_json::json!(-1),
                }),
                right: Box::new(expr),
            });
        }
        self.parse_primary()
    }

    /// Parse primary expressions: literals, identifiers, function calls, parentheses
    fn parse_primary(&mut self) -> Result<FormulaAst, SemaflowError> {
        match self.peek().cloned() {
            Some(Token::Number(n)) => {
                self.advance();
                if let Some(num) = serde_json::Number::from_f64(n) {
                    Ok(FormulaAst::Literal {
                        value: serde_json::Value::Number(num),
                    })
                } else {
                    Err(SemaflowError::Validation(format!(
                        "Invalid number in formula: {}",
                        n
                    )))
                }
            }
            Some(Token::StringLit(s)) => {
                self.advance();
                Ok(FormulaAst::Literal {
                    value: serde_json::Value::String(s),
                })
            }
            Some(Token::Ident(name)) => {
                self.advance();
                // Check for function call
                if let Some(Token::LParen) = self.peek() {
                    self.advance();
                    let args = self.parse_args()?;
                    self.expect(&Token::RParen)?;

                    // Check if it's an aggregation function
                    if let Some(agg) = parse_aggregation(&name) {
                        if args.len() != 1 {
                            return Err(SemaflowError::Validation(format!(
                                "Aggregation '{}' requires exactly 1 argument, got {}",
                                name,
                                args.len()
                            )));
                        }
                        // Extract column name from the argument
                        let col_name = match &args[0] {
                            FormulaAst::Column { column } => column.clone(),
                            FormulaAst::MeasureRef { name } => name.clone(),
                            _ => {
                                return Err(SemaflowError::Validation(format!(
                                    "Aggregation '{}' requires a column or measure reference",
                                    name
                                )))
                            }
                        };
                        return Ok(FormulaAst::Aggregation {
                            agg,
                            column: col_name,
                            filter: None,
                        });
                    }

                    // It's a regular function
                    Ok(FormulaAst::Function { name, args })
                } else {
                    // It's a column or measure reference
                    // We'll determine which during validation when measure names are known
                    Ok(FormulaAst::Column { column: name })
                }
            }
            Some(Token::LParen) => {
                self.advance();
                let expr = self.parse_comparison()?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            Some(tok) => Err(SemaflowError::Validation(format!(
                "Formula parse error in '{}': unexpected token {:?}",
                self.raw, tok
            ))),
            None => Err(SemaflowError::Validation(format!(
                "Formula parse error in '{}': unexpected end of expression",
                self.raw
            ))),
        }
    }

    /// Parse comma-separated arguments
    fn parse_args(&mut self) -> Result<Vec<FormulaAst>, SemaflowError> {
        let mut args = Vec::new();

        // Empty args
        if let Some(Token::RParen) = self.peek() {
            return Ok(args);
        }

        args.push(self.parse_comparison()?);

        while let Some(Token::Comma) = self.peek() {
            self.advance();
            args.push(self.parse_comparison()?);
        }

        Ok(args)
    }
}

/// Check if a name is an aggregation function
fn parse_aggregation(name: &str) -> Option<Aggregation> {
    match name.to_lowercase().as_str() {
        "sum" => Some(Aggregation::Sum),
        "count" => Some(Aggregation::Count),
        "count_distinct" => Some(Aggregation::CountDistinct),
        "min" => Some(Aggregation::Min),
        "max" => Some(Aggregation::Max),
        "avg" => Some(Aggregation::Avg),
        "median" => Some(Aggregation::Median),
        "stddev" => Some(Aggregation::Stddev),
        "variance" => Some(Aggregation::Variance),
        _ => None,
    }
}

/// Parse a formula string into a FormulaAst
pub fn parse_formula(raw: &str) -> Result<FormulaAst, SemaflowError> {
    let mut lexer = Lexer::new(raw);
    let tokens = lexer.tokenize()?;
    let mut parser = FormulaParser::new(raw.to_string(), tokens);
    parser.parse()
}

// ============================================================================
// Simple Expression Parser (for filters/post_expr)
// ============================================================================

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_column() {
        let ast = parse_formula("amount").unwrap();
        assert!(matches!(ast, FormulaAst::Column { column } if column == "amount"));
    }

    #[test]
    fn parse_qualified_column() {
        let ast = parse_formula("o.amount").unwrap();
        assert!(matches!(ast, FormulaAst::Column { column } if column == "o.amount"));
    }

    #[test]
    fn parse_number_literal() {
        let ast = parse_formula("42").unwrap();
        if let FormulaAst::Literal { value } = ast {
            // Numbers are parsed as f64, so check as float
            assert_eq!(value.as_f64().unwrap() as i64, 42);
        } else {
            panic!("expected literal");
        }
    }

    #[test]
    fn parse_float_literal() {
        let ast = parse_formula("3.14").unwrap();
        if let FormulaAst::Literal { value } = ast {
            let f = value.as_f64().unwrap();
            assert!((f - 3.14).abs() < 0.001);
        } else {
            panic!("expected literal");
        }
    }

    #[test]
    fn parse_string_literal() {
        let ast = parse_formula("'hello'").unwrap();
        assert!(matches!(ast, FormulaAst::Literal { value } if value.as_str() == Some("hello")));
    }

    #[test]
    fn parse_sum_aggregation() {
        let ast = parse_formula("sum(amount)").unwrap();
        match ast {
            FormulaAst::Aggregation { agg, column, .. } => {
                assert_eq!(agg, Aggregation::Sum);
                assert_eq!(column, "amount");
            }
            _ => panic!("expected aggregation, got {:?}", ast),
        }
    }

    #[test]
    fn parse_count_distinct() {
        let ast = parse_formula("count_distinct(customer_id)").unwrap();
        match ast {
            FormulaAst::Aggregation { agg, column, .. } => {
                assert_eq!(agg, Aggregation::CountDistinct);
                assert_eq!(column, "customer_id");
            }
            _ => panic!("expected aggregation"),
        }
    }

    #[test]
    fn parse_simple_division() {
        let ast = parse_formula("a / b").unwrap();
        match ast {
            FormulaAst::Binary { op, left, right } => {
                assert_eq!(op, BinaryOp::Divide);
                assert!(matches!(*left, FormulaAst::Column { column } if column == "a"));
                assert!(matches!(*right, FormulaAst::Column { column } if column == "b"));
            }
            _ => panic!("expected binary"),
        }
    }

    #[test]
    fn parse_arithmetic_expression() {
        // (a + b) * c
        let ast = parse_formula("(a + b) * c").unwrap();
        match ast {
            FormulaAst::Binary { op, left, right } => {
                assert_eq!(op, BinaryOp::Multiply);
                match *left {
                    FormulaAst::Binary { op, .. } => assert_eq!(op, BinaryOp::Add),
                    _ => panic!("expected binary inside parens"),
                }
                assert!(matches!(*right, FormulaAst::Column { column } if column == "c"));
            }
            _ => panic!("expected binary"),
        }
    }

    #[test]
    fn parse_operator_precedence() {
        // a + b * c should be a + (b * c)
        let ast = parse_formula("a + b * c").unwrap();
        match ast {
            FormulaAst::Binary { op, left, right } => {
                assert_eq!(op, BinaryOp::Add);
                assert!(matches!(*left, FormulaAst::Column { .. }));
                match *right {
                    FormulaAst::Binary { op, .. } => assert_eq!(op, BinaryOp::Multiply),
                    _ => panic!("expected multiply on right"),
                }
            }
            _ => panic!("expected binary"),
        }
    }

    #[test]
    fn parse_function_call() {
        let ast = parse_formula("round(x, 2)").unwrap();
        match ast {
            FormulaAst::Function { name, args } => {
                assert_eq!(name, "round");
                assert_eq!(args.len(), 2);
            }
            _ => panic!("expected function"),
        }
    }

    #[test]
    fn parse_nested_expression() {
        // round(sum(amount) / count(id), 2)
        let ast = parse_formula("round(sum(amount) / count(id), 2)").unwrap();
        match ast {
            FormulaAst::Function { name, args } => {
                assert_eq!(name, "round");
                assert_eq!(args.len(), 2);
                // First arg should be the division
                match &args[0] {
                    FormulaAst::Binary { op, left, right } => {
                        assert_eq!(*op, BinaryOp::Divide);
                        assert!(
                            matches!(&**left, FormulaAst::Aggregation { agg, .. } if *agg == Aggregation::Sum)
                        );
                        assert!(
                            matches!(&**right, FormulaAst::Aggregation { agg, .. } if *agg == Aggregation::Count)
                        );
                    }
                    _ => panic!("expected division as first arg"),
                }
            }
            _ => panic!("expected function"),
        }
    }

    #[test]
    fn parse_comparison() {
        let ast = parse_formula("a > 10").unwrap();
        match ast {
            FormulaAst::Binary { op, .. } => {
                assert_eq!(op, BinaryOp::Gt);
            }
            _ => panic!("expected comparison"),
        }
    }

    #[test]
    fn parse_unary_minus() {
        let ast = parse_formula("-5").unwrap();
        // Should be -1 * 5
        match ast {
            FormulaAst::Binary { op, left, right } => {
                assert_eq!(op, BinaryOp::Multiply);
                assert!(matches!(*left, FormulaAst::Literal { .. }));
                assert!(matches!(*right, FormulaAst::Literal { .. }));
            }
            _ => panic!("expected binary"),
        }
    }

    #[test]
    fn parse_error_unclosed_paren() {
        let result = parse_formula("(a + b");
        assert!(result.is_err());
    }

    #[test]
    fn parse_error_invalid_token() {
        let result = parse_formula("a @ b");
        assert!(result.is_err());
    }
}
