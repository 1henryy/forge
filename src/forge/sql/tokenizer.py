from __future__ import annotations

from .tokens import KEYWORDS, Token, TokenType


class Tokenizer:
    def tokenize(self, sql: str) -> list[Token]:
        tokens: list[Token] = []
        i = 0
        n = len(sql)

        while i < n:
            ch = sql[i]

            if ch.isspace():
                i += 1
                continue

            # line comments
            if ch == '-' and i + 1 < n and sql[i + 1] == '-':
                while i < n and sql[i] != '\n':
                    i += 1
                continue

            # two-char operators first
            if ch == '!' and i + 1 < n and sql[i + 1] == '=':
                tokens.append(Token(TokenType.NEQ, "!=", i))
                i += 2
                continue

            if ch == '<':
                if i + 1 < n and sql[i + 1] == '=':
                    tokens.append(Token(TokenType.LTEQ, "<=", i))
                    i += 2
                elif i + 1 < n and sql[i + 1] == '>':
                    tokens.append(Token(TokenType.NEQ, "<>", i))
                    i += 2
                else:
                    tokens.append(Token(TokenType.LT, "<", i))
                    i += 1
                continue

            if ch == '>':
                if i + 1 < n and sql[i + 1] == '=':
                    tokens.append(Token(TokenType.GTEQ, ">=", i))
                    i += 2
                else:
                    tokens.append(Token(TokenType.GT, ">", i))
                    i += 1
                continue

            # single-char operators
            _singles = {
                '*': TokenType.STAR, ',': TokenType.COMMA, '.': TokenType.DOT,
                '(': TokenType.LPAREN, ')': TokenType.RPAREN, ';': TokenType.SEMICOLON,
                '=': TokenType.EQ, '+': TokenType.PLUS, '-': TokenType.MINUS,
                '/': TokenType.SLASH,
            }
            if ch in _singles:
                tokens.append(Token(_singles[ch], ch, i))
                i += 1
                continue

            # quoted identifier
            if ch == '"':
                start = i
                i += 1
                while i < n and sql[i] != '"':
                    i += 1
                if i >= n:
                    raise SyntaxError(f"Unterminated quoted identifier at position {start}")
                tokens.append(Token(TokenType.IDENTIFIER, sql[start + 1:i], start))
                i += 1
                continue

            # string literal
            if ch == "'":
                start = i
                i += 1
                while i < n and sql[i] != "'":
                    i += 1
                if i >= n:
                    raise SyntaxError(f"Unterminated string literal at position {start}")
                tokens.append(Token(TokenType.STRING, sql[start + 1:i], start))
                i += 1
                continue

            # numbers
            if ch.isdigit():
                start = i
                while i < n and sql[i].isdigit():
                    i += 1
                if i < n and sql[i] == '.' and (i + 1 < n and sql[i + 1].isdigit()):
                    i += 1
                    while i < n and sql[i].isdigit():
                        i += 1
                tokens.append(Token(TokenType.NUMBER, sql[start:i], start))
                continue

            # identifiers / keywords
            if ch.isalpha() or ch == '_':
                start = i
                while i < n and (sql[i].isalnum() or sql[i] == '_'):
                    i += 1
                word = sql[start:i]
                tt = KEYWORDS.get(word.lower(), TokenType.IDENTIFIER)
                tokens.append(Token(tt, word, start))
                continue

            raise SyntaxError(f"Unexpected character {ch!r} at position {i}")

        tokens.append(Token(TokenType.EOF, "", n))
        return tokens
