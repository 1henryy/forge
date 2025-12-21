import pytest

from forge.sql.tokenizer import Tokenizer
from forge.sql.tokens import TokenType
from forge.sql.parser import (
    Parser, SelectStatement, SQLStar, SQLColumnRef, SQLNumber, SQLString,
    SQLBinaryOp, SQLFunction, SQLBooleanLiteral, SQLUnaryOp, SQLCast,
    SelectColumn, OrderByExpr, JoinClause,
)


class TestTokenizer:
    def setup_method(self):
        self.tok = Tokenizer()

    def test_simple_select(self):
        tokens = self.tok.tokenize("SELECT * FROM t")
        types = [t.token_type for t in tokens]
        assert types == [
            TokenType.SELECT, TokenType.STAR, TokenType.FROM,
            TokenType.IDENTIFIER, TokenType.EOF,
        ]

    def test_numbers(self):
        tokens = self.tok.tokenize("42 3.14")
        assert tokens[0].token_type == TokenType.NUMBER
        assert tokens[0].value == "42"
        assert tokens[1].token_type == TokenType.NUMBER
        assert tokens[1].value == "3.14"

    def test_string_literal(self):
        tokens = self.tok.tokenize("'hello world'")
        assert tokens[0].token_type == TokenType.STRING
        assert tokens[0].value == "hello world"

    def test_operators(self):
        tokens = self.tok.tokenize("= != < > <= >= + - * /")
        types = [t.token_type for t in tokens[:-1]]
        assert types == [
            TokenType.EQ, TokenType.NEQ, TokenType.LT, TokenType.GT,
            TokenType.LTEQ, TokenType.GTEQ, TokenType.PLUS, TokenType.MINUS,
            TokenType.STAR, TokenType.SLASH,
        ]

    def test_neq_angle_brackets(self):
        tokens = self.tok.tokenize("<>")
        assert tokens[0].token_type == TokenType.NEQ
        assert tokens[0].value == "<>"

    def test_keywords_case_insensitive(self):
        tokens = self.tok.tokenize("select FROM Where")
        assert tokens[0].token_type == TokenType.SELECT
        assert tokens[1].token_type == TokenType.FROM
        assert tokens[2].token_type == TokenType.WHERE

    def test_identifier(self):
        tokens = self.tok.tokenize("my_table")
        assert tokens[0].token_type == TokenType.IDENTIFIER
        assert tokens[0].value == "my_table"

    def test_quoted_identifier(self):
        tokens = self.tok.tokenize('"my column"')
        assert tokens[0].token_type == TokenType.IDENTIFIER
        assert tokens[0].value == "my column"

    def test_semicolon(self):
        tokens = self.tok.tokenize("SELECT 1;")
        types = [t.token_type for t in tokens]
        assert TokenType.SEMICOLON in types

    def test_parentheses(self):
        tokens = self.tok.tokenize("COUNT(x)")
        types = [t.token_type for t in tokens]
        assert TokenType.LPAREN in types
        assert TokenType.RPAREN in types

    def test_boolean_literals(self):
        tokens = self.tok.tokenize("TRUE FALSE")
        assert tokens[0].token_type == TokenType.TRUE
        assert tokens[1].token_type == TokenType.FALSE

    def test_line_comment(self):
        tokens = self.tok.tokenize("SELECT -- comment\n42")
        types = [t.token_type for t in tokens]
        assert TokenType.SELECT in types
        assert TokenType.NUMBER in types

    def test_unterminated_string(self):
        with pytest.raises(SyntaxError, match="Unterminated string"):
            self.tok.tokenize("'hello")

    def test_unexpected_character(self):
        with pytest.raises(SyntaxError, match="Unexpected character"):
            self.tok.tokenize("SELECT @")

    def test_dot_and_comma(self):
        tokens = self.tok.tokenize("a.b, c")
        types = [t.token_type for t in tokens[:-1]]
        assert types == [
            TokenType.IDENTIFIER, TokenType.DOT, TokenType.IDENTIFIER,
            TokenType.COMMA, TokenType.IDENTIFIER,
        ]

    def test_explain_keyword(self):
        tokens = self.tok.tokenize("EXPLAIN SELECT 1")
        assert tokens[0].token_type == TokenType.EXPLAIN


class TestParser:
    def _parse(self, sql):
        tokens = Tokenizer().tokenize(sql)
        return Parser(tokens).parse()

    def test_select_star(self):
        stmt = self._parse("SELECT * FROM t")
        assert len(stmt.columns) == 1
        assert isinstance(stmt.columns[0].expr, SQLStar)
        assert stmt.from_table == "t"

    def test_select_columns(self):
        stmt = self._parse("SELECT a, b FROM t")
        assert len(stmt.columns) == 2
        assert isinstance(stmt.columns[0].expr, SQLColumnRef)
        assert stmt.columns[0].expr.column == "a"
        assert stmt.columns[1].expr.column == "b"

    def test_select_with_alias(self):
        stmt = self._parse("SELECT a AS x FROM t")
        assert stmt.columns[0].alias == "x"

    def test_select_with_implicit_alias(self):
        stmt = self._parse("SELECT a x FROM t")
        assert stmt.columns[0].alias == "x"

    def test_select_where(self):
        stmt = self._parse("SELECT * FROM t WHERE x > 10")
        assert stmt.where is not None
        assert isinstance(stmt.where, SQLBinaryOp)
        assert stmt.where.op == ">"

    def test_select_group_by(self):
        stmt = self._parse("SELECT name, COUNT(*) FROM t GROUP BY name")
        assert len(stmt.group_by) == 1
        assert isinstance(stmt.group_by[0], SQLColumnRef)

    def test_select_order_by(self):
        stmt = self._parse("SELECT * FROM t ORDER BY x ASC, y DESC")
        assert len(stmt.order_by) == 2
        assert stmt.order_by[0].ascending is True
        assert stmt.order_by[1].ascending is False

    def test_select_limit(self):
        stmt = self._parse("SELECT * FROM t LIMIT 10")
        assert stmt.limit == 10

    def test_select_join(self):
        stmt = self._parse("SELECT * FROM a JOIN b ON a.id = b.id")
        assert len(stmt.joins) == 1
        assert stmt.joins[0].join_type == "INNER"
        assert stmt.joins[0].table == "b"

    def test_left_join(self):
        stmt = self._parse("SELECT * FROM a LEFT JOIN b ON a.id = b.id")
        assert stmt.joins[0].join_type == "LEFT"

    def test_inner_join_explicit(self):
        stmt = self._parse("SELECT * FROM a INNER JOIN b ON a.id = b.id")
        assert stmt.joins[0].join_type == "INNER"

    def test_join_with_alias(self):
        stmt = self._parse("SELECT * FROM a JOIN b AS bb ON a.id = bb.id")
        assert stmt.joins[0].table == "b"
        assert stmt.joins[0].table_alias == "bb"

    def test_function_call(self):
        stmt = self._parse("SELECT SUM(x) FROM t")
        assert isinstance(stmt.columns[0].expr, SQLFunction)
        assert stmt.columns[0].expr.name == "SUM"

    def test_count_star(self):
        stmt = self._parse("SELECT COUNT(*) FROM t")
        func = stmt.columns[0].expr
        assert isinstance(func, SQLFunction)
        assert func.name == "COUNT"
        assert isinstance(func.args[0], SQLStar)

    def test_precedence_add_mul(self):
        stmt = self._parse("SELECT 1 + 2 * 3 FROM t")
        expr = stmt.columns[0].expr
        assert isinstance(expr, SQLBinaryOp)
        assert expr.op == "+"
        assert isinstance(expr.left, SQLNumber)
        assert expr.left.value == "1"
        assert isinstance(expr.right, SQLBinaryOp)
        assert expr.right.op == "*"

    def test_precedence_and_or(self):
        stmt = self._parse("SELECT * FROM t WHERE a = 1 OR b = 2 AND c = 3")
        w = stmt.where
        assert isinstance(w, SQLBinaryOp)
        assert w.op == "OR"
        assert isinstance(w.right, SQLBinaryOp)
        assert w.right.op == "AND"

    def test_parenthesized_expression(self):
        stmt = self._parse("SELECT (1 + 2) * 3 FROM t")
        expr = stmt.columns[0].expr
        assert isinstance(expr, SQLBinaryOp)
        assert expr.op == "*"
        assert isinstance(expr.left, SQLBinaryOp)
        assert expr.left.op == "+"

    def test_unary_minus(self):
        stmt = self._parse("SELECT -5 FROM t")
        expr = stmt.columns[0].expr
        assert isinstance(expr, SQLUnaryOp)
        assert expr.op == "-"

    def test_not_expression(self):
        stmt = self._parse("SELECT * FROM t WHERE NOT x = 1")
        assert isinstance(stmt.where, SQLUnaryOp)
        assert stmt.where.op == "NOT"

    def test_boolean_literal(self):
        stmt = self._parse("SELECT * FROM t WHERE x = TRUE")
        assert isinstance(stmt.where, SQLBinaryOp)
        right = stmt.where.right
        assert isinstance(right, SQLBooleanLiteral)
        assert right.value is True

    def test_string_literal_in_where(self):
        stmt = self._parse("SELECT * FROM t WHERE name = 'alice'")
        right = stmt.where.right
        assert isinstance(right, SQLString)
        assert right.value == "alice"

    def test_qualified_column(self):
        stmt = self._parse("SELECT t.x FROM t")
        expr = stmt.columns[0].expr
        assert isinstance(expr, SQLColumnRef)
        assert expr.table == "t"
        assert expr.column == "x"

    def test_cast_expression(self):
        stmt = self._parse("SELECT CAST(x AS INTEGER) FROM t")
        expr = stmt.columns[0].expr
        assert isinstance(expr, SQLCast)
        assert expr.data_type == "INTEGER"

    def test_explain(self):
        stmt = self._parse("EXPLAIN SELECT * FROM t")
        assert stmt.is_explain is True

    def test_semicolon_optional(self):
        stmt = self._parse("SELECT * FROM t;")
        assert stmt.from_table == "t"

    def test_having(self):
        stmt = self._parse("SELECT name, COUNT(*) FROM t GROUP BY name HAVING COUNT(*) > 1")
        assert stmt.having is not None

    def test_table_alias(self):
        stmt = self._parse("SELECT * FROM t AS tbl")
        assert stmt.from_alias == "tbl"

    def test_multiple_joins(self):
        stmt = self._parse(
            "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id"
        )
        assert len(stmt.joins) == 2

    def test_unexpected_token_error(self):
        with pytest.raises(SyntaxError):
            self._parse("SELECT FROM")
