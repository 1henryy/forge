# __MARKER_3__
# __MARKER_4__
from __future__ import annotations

from dataclasses import dataclass, field

from .tokens import Token, TokenType


# SQL AST nodes

class SQLExpr:
    pass


@dataclass
class SQLIdentifier(SQLExpr):
    name: str


@dataclass
class SQLNumber(SQLExpr):
    value: str


@dataclass
class SQLString(SQLExpr):
    value: str


@dataclass
class SQLBooleanLiteral(SQLExpr):
    value: bool


@dataclass
class SQLBinaryOp(SQLExpr):
    op: str
    left: SQLExpr
    right: SQLExpr


@dataclass
class SQLUnaryOp(SQLExpr):
    op: str
    operand: SQLExpr


@dataclass
class SQLFunction(SQLExpr):
    name: str
    args: list[SQLExpr]


@dataclass
class SQLCast(SQLExpr):
    expr: SQLExpr
    data_type: str


@dataclass
class SQLStar(SQLExpr):
    pass


@dataclass
class SQLColumnRef(SQLExpr):
    table: str | None
    column: str


@dataclass
class SQLNull(SQLExpr):
    pass


# Statement types

@dataclass
class SelectColumn:
    expr: SQLExpr
    alias: str | None = None


@dataclass
class JoinClause:
    join_type: str
    table: str
    table_alias: str | None = None
    on_expr: SQLExpr | None = None


@dataclass
class OrderByExpr:
    expr: SQLExpr
    ascending: bool = True


@dataclass
class SelectStatement:
    columns: list[SelectColumn] = field(default_factory=list)
    from_table: str | None = None
    from_alias: str | None = None
    joins: list[JoinClause] = field(default_factory=list)
    where: SQLExpr | None = None
    group_by: list[SQLExpr] = field(default_factory=list)
    having: SQLExpr | None = None
    order_by: list[OrderByExpr] = field(default_factory=list)
    limit: int | None = None
    is_explain: bool = False


# Precedence levels

PREC_OR = 1
PREC_AND = 2
PREC_NOT = 3
PREC_COMPARISON = 4
PREC_ADDITION = 5
PREC_MULTIPLICATION = 6
PREC_UNARY = 7

_BINARY_OP_PREC: dict[TokenType, int] = {
    TokenType.OR: PREC_OR,
    TokenType.AND: PREC_AND,
    TokenType.EQ: PREC_COMPARISON,
    TokenType.NEQ: PREC_COMPARISON,
    TokenType.LT: PREC_COMPARISON,
    TokenType.GT: PREC_COMPARISON,
    TokenType.LTEQ: PREC_COMPARISON,
    TokenType.GTEQ: PREC_COMPARISON,
    TokenType.PLUS: PREC_ADDITION,
    TokenType.MINUS: PREC_ADDITION,
    TokenType.STAR: PREC_MULTIPLICATION,
    TokenType.SLASH: PREC_MULTIPLICATION,
}

_OP_STR: dict[TokenType, str] = {
    TokenType.OR: "OR", TokenType.AND: "AND",
    TokenType.EQ: "=", TokenType.NEQ: "!=",
    TokenType.LT: "<", TokenType.GT: ">",
    TokenType.LTEQ: "<=", TokenType.GTEQ: ">=",
    TokenType.PLUS: "+", TokenType.MINUS: "-",
    TokenType.STAR: "*", TokenType.SLASH: "/",
}


class Parser:
    def __init__(self, tokens: list[Token]) -> None:
        self._tokens = tokens
        self._pos = 0

    def _peek(self) -> Token:
        return self._tokens[self._pos]

    def _advance(self) -> Token:
        tok = self._tokens[self._pos]
        self._pos += 1
        return tok

    def _expect(self, tt: TokenType) -> Token:
        tok = self._peek()
        if tok.token_type != tt:
            raise SyntaxError(
                f"Expected {tt.name} but got {tok.token_type.name} ({tok.value!r}) "
                f"at position {tok.position}"
            )
        return self._advance()

    def _match(self, *types: TokenType) -> Token | None:
        if self._peek().token_type in types:
            return self._advance()
        return None

    def _at(self, *types: TokenType) -> bool:
        return self._peek().token_type in types

    def parse(self) -> SelectStatement:
        is_explain = False
        if self._match(TokenType.EXPLAIN):
            is_explain = True
        stmt = self._parse_select()
        stmt.is_explain = is_explain
        self._match(TokenType.SEMICOLON)
        self._expect(TokenType.EOF)
        return stmt

    def _parse_select(self) -> SelectStatement:
        self._expect(TokenType.SELECT)
        stmt = SelectStatement()
        stmt.columns = self._parse_select_columns()

        if self._match(TokenType.FROM):
            stmt.from_table, stmt.from_alias = self._parse_table_ref()
            stmt.joins = self._parse_joins()

        if self._match(TokenType.WHERE):
            stmt.where = self._parse_expr(0)

        if self._at(TokenType.GROUP):
            self._advance()
            self._expect(TokenType.BY)
            stmt.group_by = self._parse_expr_list()

        if self._match(TokenType.HAVING):
            stmt.having = self._parse_expr(0)

        if self._at(TokenType.ORDER):
            self._advance()
            self._expect(TokenType.BY)
            stmt.order_by = self._parse_order_by_list()

        if self._match(TokenType.LIMIT):
            tok = self._expect(TokenType.NUMBER)
            stmt.limit = int(tok.value)

        return stmt

    def _parse_select_columns(self) -> list[SelectColumn]:
        cols: list[SelectColumn] = []
        cols.append(self._parse_select_column())
        while self._match(TokenType.COMMA):
            cols.append(self._parse_select_column())
        return cols

    def _parse_select_column(self) -> SelectColumn:
        if self._at(TokenType.STAR):
            self._advance()
            return SelectColumn(expr=SQLStar())

        expr = self._parse_expr(0)
        alias: str | None = None
        if self._match(TokenType.AS):
            alias = self._expect(TokenType.IDENTIFIER).value
        elif self._at(TokenType.IDENTIFIER):
            alias = self._advance().value
        return SelectColumn(expr=expr, alias=alias)

    def _parse_table_ref(self) -> tuple[str, str | None]:
        name = self._expect(TokenType.IDENTIFIER).value
        alias: str | None = None
        if self._match(TokenType.AS):
            alias = self._expect(TokenType.IDENTIFIER).value
        elif self._at(TokenType.IDENTIFIER) and not self._at_join_keyword():
            alias = self._advance().value
        return name, alias

    def _at_join_keyword(self) -> bool:
        return self._at(
            TokenType.JOIN, TokenType.INNER, TokenType.LEFT,
            TokenType.RIGHT, TokenType.CROSS,
            TokenType.WHERE, TokenType.GROUP, TokenType.ORDER,
            TokenType.LIMIT, TokenType.HAVING,
        )

    def _parse_joins(self) -> list[JoinClause]:
        joins: list[JoinClause] = []
        while True:
            join_type: str | None = None
            if self._match(TokenType.INNER):
                self._expect(TokenType.JOIN)
                join_type = "INNER"
            elif self._match(TokenType.LEFT):
                self._expect(TokenType.JOIN)
                join_type = "LEFT"
            elif self._match(TokenType.RIGHT):
                self._expect(TokenType.JOIN)
                join_type = "RIGHT"
            elif self._match(TokenType.CROSS):
                self._expect(TokenType.JOIN)
                join_type = "CROSS"
            elif self._match(TokenType.JOIN):
                join_type = "INNER"
            else:
                break

            table_name = self._expect(TokenType.IDENTIFIER).value
            table_alias: str | None = None
            if self._match(TokenType.AS):
                table_alias = self._expect(TokenType.IDENTIFIER).value
            elif self._at(TokenType.IDENTIFIER) and not self._at(TokenType.ON) and not self._at_join_keyword():
                table_alias = self._advance().value

            on_expr: SQLExpr | None = None
            if join_type != "CROSS" and self._match(TokenType.ON):
                on_expr = self._parse_expr(0)

            joins.append(JoinClause(
                join_type=join_type,
                table=table_name,
                table_alias=table_alias,
                on_expr=on_expr,
            ))
        return joins

    def _parse_expr_list(self) -> list[SQLExpr]:
        exprs: list[SQLExpr] = []
        exprs.append(self._parse_expr(0))
        while self._match(TokenType.COMMA):
            exprs.append(self._parse_expr(0))
        return exprs

    def _parse_order_by_list(self) -> list[OrderByExpr]:
        items: list[OrderByExpr] = []
        items.append(self._parse_order_by_item())
        while self._match(TokenType.COMMA):
            items.append(self._parse_order_by_item())
        return items

    def _parse_order_by_item(self) -> OrderByExpr:
        expr = self._parse_expr(0)
        ascending = True
        if self._match(TokenType.ASC):
            ascending = True
        elif self._match(TokenType.DESC):
            ascending = False
        return OrderByExpr(expr=expr, ascending=ascending)

    # --- Pratt expression parser ---

    def _parse_expr(self, min_prec: int) -> SQLExpr:
        left = self._parse_prefix()

        while True:
            tok = self._peek()
            prec = _BINARY_OP_PREC.get(tok.token_type)
            if prec is None or prec < min_prec:
                break
            op_tok = self._advance()
            op_str = _OP_STR[op_tok.token_type]
            right = self._parse_expr(prec + 1)
            left = SQLBinaryOp(op=op_str, left=left, right=right)

        return left

    def _parse_prefix(self) -> SQLExpr:
        tok = self._peek()

        if tok.token_type == TokenType.NOT:
            self._advance()
            operand = self._parse_expr(PREC_NOT)
            return SQLUnaryOp(op="NOT", operand=operand)

        if tok.token_type == TokenType.MINUS:
            self._advance()
            operand = self._parse_expr(PREC_UNARY)
            return SQLUnaryOp(op="-", operand=operand)

        if tok.token_type == TokenType.LPAREN:
            self._advance()
            expr = self._parse_expr(0)
            self._expect(TokenType.RPAREN)
            return expr

        if tok.token_type == TokenType.NUMBER:
            self._advance()
            return SQLNumber(value=tok.value)

        if tok.token_type == TokenType.STRING:
            self._advance()
            return SQLString(value=tok.value)

        if tok.token_type == TokenType.TRUE:
            self._advance()
            return SQLBooleanLiteral(value=True)

        if tok.token_type == TokenType.FALSE:
            self._advance()
            return SQLBooleanLiteral(value=False)

        if tok.token_type == TokenType.NULL:
            self._advance()
            return SQLNull()

        if tok.token_type == TokenType.CAST:
            return self._parse_cast()

        if tok.token_type == TokenType.STAR:
            self._advance()
            return SQLStar()

        if tok.token_type == TokenType.IDENTIFIER:
            return self._parse_identifier_expr()

        if tok.token_type in (
            TokenType.INT, TokenType.INTEGER, TokenType.FLOAT,
            TokenType.DOUBLE, TokenType.VARCHAR, TokenType.BOOLEAN,
        ):
            self._advance()
            return SQLIdentifier(name=tok.value)

        raise SyntaxError(
            f"Unexpected token {tok.token_type.name} ({tok.value!r}) "
            f"at position {tok.position}"
        )

    def _parse_cast(self) -> SQLCast:
        self._expect(TokenType.CAST)
        self._expect(TokenType.LPAREN)
        expr = self._parse_expr(0)
        self._expect(TokenType.AS)
        dt_tok = self._advance()
        data_type = dt_tok.value.upper()
        self._expect(TokenType.RPAREN)
        return SQLCast(expr=expr, data_type=data_type)

    def _parse_identifier_expr(self) -> SQLExpr:
        name_tok = self._advance()
        name = name_tok.value

        # function call: name(...)
        if self._at(TokenType.LPAREN):
            self._advance()
            args: list[SQLExpr] = []
            if not self._at(TokenType.RPAREN):
                if self._at(TokenType.STAR):
                    self._advance()
                    args.append(SQLStar())
                else:
                    args = self._parse_expr_list()
            self._expect(TokenType.RPAREN)
            return SQLFunction(name=name.upper(), args=args)

        # qualified column: table.column or table.*
        if self._at(TokenType.DOT):
            self._advance()
            if self._at(TokenType.STAR):
                self._advance()
                return SQLColumnRef(table=name, column="*")
            col_tok = self._expect(TokenType.IDENTIFIER)
            return SQLColumnRef(table=name, column=col_tok.value)

        return SQLColumnRef(table=None, column=name)
