from enum import Enum, auto


class TokenType(Enum):
    # Literals
    IDENTIFIER = auto()
    NUMBER = auto()
    STRING = auto()

    # Keywords
    SELECT = auto()
    FROM = auto()
    WHERE = auto()
    AND = auto()
    OR = auto()
    NOT = auto()
    AS = auto()
    JOIN = auto()
    INNER = auto()
    LEFT = auto()
    RIGHT = auto()
    CROSS = auto()
    ON = auto()
    GROUP = auto()
    BY = auto()
    HAVING = auto()
    ORDER = auto()
    ASC = auto()
    DESC = auto()
    LIMIT = auto()
    INSERT = auto()
    CREATE = auto()
    TABLE = auto()
    TRUE = auto()
    FALSE = auto()
    NULL = auto()
    CAST = auto()
    INT = auto()
    INTEGER = auto()
    FLOAT = auto()
    DOUBLE = auto()
    VARCHAR = auto()
    BOOLEAN = auto()
    EXPLAIN = auto()

    # Operators
    STAR = auto()         # *
    COMMA = auto()        # ,
    DOT = auto()          # .
    LPAREN = auto()       # (
    RPAREN = auto()       # )
    SEMICOLON = auto()    # ;
    EQ = auto()           # =
    NEQ = auto()          # != or <>
    LT = auto()           # <
    GT = auto()           # >
    LTEQ = auto()         # <=
    GTEQ = auto()         # >=
    PLUS = auto()         # +
    MINUS = auto()        # -
    SLASH = auto()        # /

    # Special
    EOF = auto()


KEYWORDS = {
    "select": TokenType.SELECT, "from": TokenType.FROM, "where": TokenType.WHERE,
    "and": TokenType.AND, "or": TokenType.OR, "not": TokenType.NOT, "as": TokenType.AS,
    "join": TokenType.JOIN, "inner": TokenType.INNER, "left": TokenType.LEFT,
    "right": TokenType.RIGHT, "cross": TokenType.CROSS, "on": TokenType.ON,
    "group": TokenType.GROUP, "by": TokenType.BY, "having": TokenType.HAVING,
    "order": TokenType.ORDER, "asc": TokenType.ASC, "desc": TokenType.DESC,
    "limit": TokenType.LIMIT, "true": TokenType.TRUE, "false": TokenType.FALSE,
    "null": TokenType.NULL, "cast": TokenType.CAST, "int": TokenType.INT,
    "integer": TokenType.INTEGER, "float": TokenType.FLOAT, "double": TokenType.DOUBLE,
    "varchar": TokenType.VARCHAR, "boolean": TokenType.BOOLEAN, "explain": TokenType.EXPLAIN,
    "insert": TokenType.INSERT, "create": TokenType.CREATE, "table": TokenType.TABLE,
}


class Token:
    def __init__(self, token_type: TokenType, value: str, position: int = 0):
        self.token_type = token_type
        self.value = value
        self.position = position

    def __repr__(self) -> str:
        return f"Token({self.token_type}, {self.value!r})"
