"""
SQL词法分析器
"""

import re
from enum import Enum
from typing import List, NamedTuple


class TokenType(Enum):
    # 关键字
    SELECT = "SELECT"
    FROM = "FROM"
    WHERE = "WHERE"
    INSERT = "INSERT"
    INTO = "INTO"
    VALUES = "VALUES"
    CREATE = "CREATE"
    TABLE = "TABLE"
    INTEGER = "INTEGER"
    VARCHAR = "VARCHAR"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"
    CHAR = "CHAR"
    DECIMAL = "DECIMAL"
    DATE = "DATE"
    TIME = "TIME"
    DATETIME = "DATETIME"
    BIGINT = "BIGINT"
    TINYINT = "TINYINT"
    TEXT = "TEXT"
    PRIMARY = "PRIMARY"
    KEY = "KEY"
    NOT = "NOT"
    NULL = "NULL"
    AND = "AND"
    OR = "OR"
    STAR = "STAR"
    DOT = "DOT"
    UNIQUE = "UNIQUE"
    INDEX = "INDEX"
    ON = "ON"
    DROP = "DROP"
    JOIN = "JOIN"
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    SET = "SET"
    DEFAULT = "DEFAULT"
    CHECK = "CHECK"
    FOREIGN = "FOREIGN"
    REFERENCES = "REFERENCES"
    BEGIN = "BEGIN"
    START = "START"
    TRANSACTION = "TRANSACTION"
    COMMIT = "COMMIT"
    ROLLBACK = "ROLLBACK"
    AUTOCOMMIT = "AUTOCOMMIT"
    SESSION = "SESSION"
    ISOLATION = "ISOLATION"
    LEVEL = "LEVEL"
    READ = "READ"
    COMMITTED_KW = "COMMITTED_KW"
    UNCOMMITTED_KW = "UNCOMMITTED_KW"
    REPEATABLE = "REPEATABLE"
    SERIALIZABLE = "SERIALIZABLE"
    TRUNCATE = "TRUNCATE"
    VIEW = "VIEW"
    AS = "AS"
    # 新增：分组与排序
    GROUP = "GROUP"
    BY = "BY"
    ORDER = "ORDER"
    ASC = "ASC"
    DESC = "DESC"
    # 在TokenType枚举中添加
    USER = "USER"
    IDENTIFIED = "IDENTIFIED"
    GRANT = "GRANT"
    REVOKE = "REVOKE"
    TO = "TO"
    ALL = "ALL"
    PRIVILEGES = "PRIVILEGES"
    # 新增：IF EXISTS 支持
    IF = "IF"
    EXISTS = "EXISTS"

    # 布尔值
    TRUE = "TRUE"
    FALSE = "FALSE"

    # 标识符和字面量
    IDENTIFIER = "IDENTIFIER"
    NUMBER = "NUMBER"
    STRING = "STRING"

    # 运算符
    EQUALS = "="
    LESS_THAN = "<"
    GREATER_THAN = ">"
    LESS_EQUAL = "<="
    GREATER_EQUAL = ">="
    NOT_EQUAL = "!="

    # 分隔符
    COMMA = ","
    SEMICOLON = ";"  # 仅英文分号;被识别为语句结束符，中文分号；不被识别
    LEFT_PAREN = "("
    RIGHT_PAREN = ")"

    # 特殊
    WHITESPACE = "WHITESPACE"
    EOF = "EOF"


class Token(NamedTuple):
    type: TokenType
    value: str
    line: int
    column: int


class SQLLexer:
    """SQL词法分析器"""

    KEYWORDS = {
        "SELECT": TokenType.SELECT,
        "FROM": TokenType.FROM,
        "WHERE": TokenType.WHERE,
        "INSERT": TokenType.INSERT,
        "INTO": TokenType.INTO,
        "VALUES": TokenType.VALUES,
        "CREATE": TokenType.CREATE,
        "TABLE": TokenType.TABLE,
        "INTEGER": TokenType.INTEGER,
        "VARCHAR": TokenType.VARCHAR,
        "FLOAT": TokenType.FLOAT,
        "BOOLEAN": TokenType.BOOLEAN,
        "PRIMARY": TokenType.PRIMARY,
        "CHAR": TokenType.CHAR,
        "DECIMAL": TokenType.DECIMAL,
        "DATE": TokenType.DATE,
        "TIME": TokenType.TIME,
        "DATETIME": TokenType.DATETIME,
        "BIGINT": TokenType.BIGINT,
        "TINYINT": TokenType.TINYINT,
        "TEXT": TokenType.TEXT,
        "KEY": TokenType.KEY,
        "NOT": TokenType.NOT,
        "NULL": TokenType.NULL,
        "AND": TokenType.AND,
        "OR": TokenType.OR,
        "TRUE": TokenType.TRUE,
        "FALSE": TokenType.FALSE,
        "UNIQUE": TokenType.UNIQUE,
        "INDEX": TokenType.INDEX,
        "ON": TokenType.ON,
        "DROP": TokenType.DROP,
        "JOIN": TokenType.JOIN,
        "INNER": TokenType.INNER,
        "LEFT": TokenType.LEFT,
        "RIGHT": TokenType.RIGHT,
        "COUNT": TokenType.COUNT,
        "SUM": TokenType.SUM,
        "AVG": TokenType.AVG,
        "MIN": TokenType.MIN,
        "MAX": TokenType.MAX,
        "UPDATE": TokenType.UPDATE,
        "DELETE": TokenType.DELETE,
        "SET": TokenType.SET,
        "DEFAULT": TokenType.DEFAULT,
        "CHECK": TokenType.CHECK,
        "FOREIGN": TokenType.FOREIGN,
        "REFERENCES": TokenType.REFERENCES,
        "BEGIN": TokenType.BEGIN,
        "START": TokenType.START,
        "TRANSACTION": TokenType.TRANSACTION,
        "COMMIT": TokenType.COMMIT,
        "ROLLBACK": TokenType.ROLLBACK,
        "AUTOCOMMIT": TokenType.AUTOCOMMIT,
        "SESSION": TokenType.SESSION,
        "ISOLATION": TokenType.ISOLATION,
        "LEVEL": TokenType.LEVEL,
        "READ": TokenType.READ,
        "COMMITTED": TokenType.COMMITTED_KW,
        "UNCOMMITTED": TokenType.UNCOMMITTED_KW,
        "REPEATABLE": TokenType.REPEATABLE,
        "SERIALIZABLE": TokenType.SERIALIZABLE,
        "TRUNCATE": TokenType.TRUNCATE,
        "VIEW": TokenType.VIEW,
        "AS": TokenType.AS,
        # 新增：分组与排序
        "GROUP": TokenType.GROUP,
        "BY": TokenType.BY,
        "ORDER": TokenType.ORDER,
        "ASC": TokenType.ASC,
        "DESC": TokenType.DESC,
        # 添加这些用户管理关键词：
        "USER": TokenType.USER,
        "IDENTIFIED": TokenType.IDENTIFIED,
        "GRANT": TokenType.GRANT,
        "REVOKE": TokenType.REVOKE,
        "TO": TokenType.TO,
        "ALL": TokenType.ALL,
        "PRIVILEGES": TokenType.PRIVILEGES,
        # 新增：IF EXISTS 支持
        "IF": TokenType.IF,
        "EXISTS": TokenType.EXISTS,
    }

    def __init__(self, sql: str):
        self.sql = sql
        self.position = 0
        self.line = 1
        self.column = 1
        self.tokens: List[Token] = []

    def tokenize(self) -> List[Token]:
        """将SQL文本分解为Token列表"""
        # DEBUG: 记录tokenize开始
        # print(f"[LEXER DEBUG] tokenize start: sql={self.sql}")
        while self.position < len(self.sql):
            self._skip_whitespace()

            if self.position >= len(self.sql):
                break

            char = self.sql[self.position]

            if char == '#':
                # 跳过注释到行尾
                while self.position < len(self.sql) and self.sql[self.position] != '\n':
                    self.position += 1
                continue

            if char.isalpha() or char == "_":
                self._read_identifier_or_keyword()
            elif char.isdigit() or (char == "-" and self._lookahead_is_digit()):
                self._read_number()
            elif char in ("'", '"'):
                self._read_string(char)
            elif char == "=":
                self._add_single_char_token(TokenType.EQUALS, char)
            elif char == "<":
                self._read_less_than()
            elif char == ">":
                self._read_greater_than()
            elif char == "!":
                self._read_not_equal()
            elif char == ",":
                self._add_single_char_token(TokenType.COMMA, char)
            elif char == ";":
                self._add_single_char_token(TokenType.SEMICOLON, char)
            elif char == "(":
                self._add_single_char_token(TokenType.LEFT_PAREN, char)
            elif char == ")":
                self._add_single_char_token(TokenType.RIGHT_PAREN, char)
            elif char == "*":
                self._add_single_char_token(TokenType.STAR, char)
            elif char == ".":
                self._read_dot()
            else:
                raise SyntaxError(
                    f"未识别的字符 '{char}' 在行 {self.line}, 列 {self.column}"
                )

        self._add_token(TokenType.EOF, "")
        # DEBUG: 记录tokenize结束与tokens
        # try:
        #     token_summary = [(t.type.name, t.value) for t in self.tokens]
        # except Exception:
        #     token_summary = [str(t) for t in self.tokens]
        # print(f"[LEXER DEBUG] tokenize end: tokens={token_summary}")
        return self.tokens

    def _skip_whitespace(self):
        """跳过空白字符"""
        while self.position < len(self.sql) and self.sql[self.position].isspace():
            if self.sql[self.position] == "\n":
                self.line += 1
                self.column = 1
            else:
                self.column += 1
            self.position += 1

    def _lookahead_is_digit(self) -> bool:
        return (self.position + 1 < len(self.sql)) and self.sql[self.position + 1].isdigit()

    def _read_identifier_or_keyword(self):
        """读取标识符或关键字"""
        start = self.position
        start_column = self.column

        while self.position < len(self.sql) and (
            self.sql[self.position].isalnum() or self.sql[self.position] == "_"
        ):
            self.position += 1
            self.column += 1

        value = self.sql[start : self.position]
        token_type = self.KEYWORDS.get(value.upper(), TokenType.IDENTIFIER)
        token = Token(token_type, value, self.line, start_column)
        self.tokens.append(token)

    def _read_number(self):
        """读取数字（整数或浮点数），支持可选的负号"""
        start = self.position
        start_column = self.column
        has_dot = False

        # 处理可选的前导负号
        if self.sql[self.position] == "-":
            self.position += 1
            self.column += 1

        while self.position < len(self.sql):
            char = self.sql[self.position]
            if char.isdigit():
                self.position += 1
                self.column += 1
            elif char == "." and not has_dot and self._is_decimal_dot():
                has_dot = True
                self.position += 1
                self.column += 1
            else:
                break

        value = self.sql[start : self.position]
        token = Token(TokenType.NUMBER, value, self.line, start_column)
        self.tokens.append(token)

    def _is_decimal_dot(self):
        """判断点号是否是小数点（后面跟着数字）"""
        if self.position + 1 < len(self.sql):
            return self.sql[self.position + 1].isdigit()
        return False

    def _read_dot(self):
        """处理点号 - 可能是小数点或DOT操作符"""
        if self._is_decimal_dot():
            self._read_number()  # 作为小数处理
        else:
            self._add_single_char_token(TokenType.DOT, ".")

    def _read_string(self, quote_char: str):
        """读取字符串字面量"""
        # print(f"[LEXER DEBUG] _read_string called with quote_char={quote_char} at pos={self.position}, line={self.line}, col={self.column}")
        start_column = self.column
        self.position += 1  # 跳过开始引号
        self.column += 1

        value = ""
        while self.position < len(self.sql):
            char = self.sql[self.position]

            if char == quote_char:
                # 结束引号
                self.position += 1
                self.column += 1
                # print(f"[LEXER DEBUG] _read_string finished, value='{value}'")
                token = Token(TokenType.STRING, value, self.line, start_column)
                self.tokens.append(token)
                return
            elif char == "\\":
                # 转义字符
                self.position += 1
                self.column += 1
                if self.position < len(self.sql):
                    escaped_char = self.sql[self.position]
                    if escaped_char == "n":
                        value += "\n"
                    elif escaped_char == "t":
                        value += "\t"
                    elif escaped_char == "\\":
                        value += "\\"
                    elif escaped_char == quote_char:
                        value += quote_char
                    else:
                        value += escaped_char
                    self.position += 1
                    self.column += 1
            else:
                value += char
                self.position += 1
                if char == "\n":
                    self.line += 1
                    self.column = 1
                else:
                    self.column += 1

        raise SyntaxError(f"未闭合的字符串，开始于行 {self.line}")

    def _read_less_than(self):
        """读取 < 或 <="""
        start_column = self.column
        if self.position + 1 < len(self.sql) and self.sql[self.position + 1] == "=":
            token = Token(TokenType.LESS_EQUAL, "<=", self.line, start_column)
            self.tokens.append(token)
            self.position += 2
            self.column += 2
        else:
            self._add_single_char_token(TokenType.LESS_THAN, "<")

    def _read_greater_than(self):
        """读取 > 或 >="""
        start_column = self.column
        if self.position + 1 < len(self.sql) and self.sql[self.position + 1] == "=":
            token = Token(TokenType.GREATER_EQUAL, ">=", self.line, start_column)
            self.tokens.append(token)
            self.position += 2
            self.column += 2
        else:
            self._add_single_char_token(TokenType.GREATER_THAN, ">")

    def _read_not_equal(self):
        """读取 !="""
        start_column = self.column
        if self.position + 1 < len(self.sql) and self.sql[self.position + 1] == "=":
            token = Token(TokenType.NOT_EQUAL, "!=", self.line, start_column)
            self.tokens.append(token)
            self.position += 2
            self.column += 2
        else:
            raise SyntaxError(f"未识别的字符 '!' 在行 {self.line}, 列 {self.column}")

    def _add_token(self, token_type: TokenType, value: str):
        """添加Token（不移动位置）"""
        token = Token(token_type, value, self.line, self.column)
        self.tokens.append(token)

    def _add_single_char_token(self, token_type: TokenType, char: str):
        """添加单字符Token并移动位置"""
        token = Token(token_type, char, self.line, self.column)
        self.tokens.append(token)
        self.position += 1
        self.column += 1
