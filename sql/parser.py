"""
SQL语法分析器
"""

from typing import List, Union
from sql import Token, TokenType
from .ast_nodes import *


class SQLParser:
    """SQL语法分析器"""

    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.position = 0
        self.current_token = tokens[0] if tokens else None

    def parse(self) -> Statement:
        """解析SQL语句"""
        if not self.current_token or self.current_token.type == TokenType.EOF:
            raise SyntaxError("空的SQL语句")

        if self.current_token.type == TokenType.CREATE:
            return self._parse_create_statement()  # 改为这个新方法
        elif self.current_token.type == TokenType.DROP:
            return self._parse_drop_statement()  # 添加DROP支持
        elif self.current_token.type == TokenType.INSERT:
            return self._parse_insert()
        elif self.current_token.type == TokenType.SELECT:
            return self._parse_select()
        else:
            raise SyntaxError(f"不支持的语句类型: {self.current_token.value}")

    def _advance(self):
        """移动到下一个token"""
        if self.position < len(self.tokens) - 1:
            self.position += 1
            self.current_token = self.tokens[self.position]

    def _expect(self, expected_type: TokenType) -> Token:
        """期望特定类型的token"""
        if not self.current_token or self.current_token.type != expected_type:
            raise SyntaxError(
                f"期望 {expected_type.value}, 但得到 {self.current_token.value if self.current_token else 'EOF'}"
            )
        token = self.current_token
        self._advance()
        return token

    def _parse_create_table(self) -> CreateTableStatement:
        """解析 CREATE TABLE 语句"""
        self._expect(TokenType.CREATE)
        self._expect(TokenType.TABLE)

        table_name = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.LEFT_PAREN)

        columns = []
        while self.current_token.type != TokenType.RIGHT_PAREN:
            column = self._parse_column_definition()
            columns.append(column)

            if self.current_token.type == TokenType.COMMA:
                self._advance()
            elif self.current_token.type != TokenType.RIGHT_PAREN:
                raise SyntaxError("列定义之间需要逗号分隔")

        self._expect(TokenType.RIGHT_PAREN)
        return CreateTableStatement(table_name, columns)

    def _parse_column_definition(self) -> dict:
        """解析列定义"""
        column_name = self._expect(TokenType.IDENTIFIER).value

        # 解析数据类型
        if self.current_token.type in (
            TokenType.INTEGER,
            TokenType.VARCHAR,
            TokenType.FLOAT,
            TokenType.BOOLEAN,
        ):
            data_type = self.current_token.value.upper()
            self._advance()
        else:
            raise SyntaxError(f"期望数据类型，但得到 {self.current_token.value}")

        # 解析长度（对于VARCHAR）
        length = None
        if data_type == "VARCHAR" and self.current_token.type == TokenType.LEFT_PAREN:
            self._advance()
            length = int(self._expect(TokenType.NUMBER).value)
            self._expect(TokenType.RIGHT_PAREN)

        # 解析约束
        constraints = []
        while self.current_token and self.current_token.type in (
            TokenType.PRIMARY,
            TokenType.NOT,
            TokenType.NULL,
        ):
            if self.current_token.type == TokenType.PRIMARY:
                self._advance()
                self._expect(TokenType.KEY)
                constraints.append("PRIMARY KEY")
            elif self.current_token.type == TokenType.NOT:
                self._advance()
                self._expect(TokenType.NULL)
                constraints.append("NOT NULL")
            elif self.current_token.type == TokenType.NULL:
                self._advance()
                constraints.append("NULL")

        return {
            "name": column_name,
            "type": data_type,
            "length": length,
            "constraints": constraints,
        }

    def _parse_insert(self) -> InsertStatement:
        """解析 INSERT 语句"""
        self._expect(TokenType.INSERT)
        self._expect(TokenType.INTO)

        table_name = self._expect(TokenType.IDENTIFIER).value

        # 解析列名（可选）
        columns = []
        if self.current_token.type == TokenType.LEFT_PAREN:
            self._advance()
            while self.current_token.type != TokenType.RIGHT_PAREN:
                columns.append(self._expect(TokenType.IDENTIFIER).value)
                if self.current_token.type == TokenType.COMMA:
                    self._advance()
                elif self.current_token.type != TokenType.RIGHT_PAREN:
                    raise SyntaxError("列名之间需要逗号分隔")
            self._expect(TokenType.RIGHT_PAREN)

        self._expect(TokenType.VALUES)

        # 解析值列表
        values = []
        while True:
            self._expect(TokenType.LEFT_PAREN)
            row_values = []

            while self.current_token.type != TokenType.RIGHT_PAREN:
                value = self._parse_expression()
                row_values.append(value)

                if self.current_token.type == TokenType.COMMA:
                    self._advance()
                elif self.current_token.type != TokenType.RIGHT_PAREN:
                    raise SyntaxError("值之间需要逗号分隔")

            self._expect(TokenType.RIGHT_PAREN)
            values.append(row_values)

            # 检查是否还有更多值行
            if self.current_token.type == TokenType.COMMA:
                self._advance()
            else:
                break

        return InsertStatement(table_name, columns, values)

    def _parse_select(self) -> SelectStatement:
        """解析 SELECT 语句"""
        self._expect(TokenType.SELECT)

        # 解析选择列表
        columns = []
        while True:
            if self.current_token.type == TokenType.STAR:
                columns.append("*")
                self._advance()
            else:
                column_name = self._expect(TokenType.IDENTIFIER).value
                # 检查是否有表前缀
                if self.current_token and self.current_token.type == TokenType.DOT:
                    self._advance()
                    table_name = column_name
                    column_name = self._expect(TokenType.IDENTIFIER).value
                    columns.append(ColumnRef(column_name, table_name))
                else:
                    columns.append(ColumnRef(column_name))

            if self.current_token.type == TokenType.COMMA:
                self._advance()
            else:
                break

        self._expect(TokenType.FROM)
        table_name = self._expect(TokenType.IDENTIFIER).value

        # WHERE 子句
        where_clause = None
        if self.current_token and self.current_token.type == TokenType.WHERE:
            self._advance()
            where_clause = self._parse_where_expression()

        return SelectStatement(columns, table_name, where_clause)

    def _parse_where_expression(self) -> Expression:
        """解析 WHERE 表达式"""
        return self._parse_or_expression()

    def _parse_or_expression(self) -> Expression:
        """解析 OR 表达式"""
        left = self._parse_and_expression()

        while self.current_token and self.current_token.type == TokenType.OR:
            operator = self.current_token.value
            self._advance()
            right = self._parse_and_expression()
            left = LogicalOp(left, operator, right)

        return left

    def _parse_and_expression(self) -> Expression:
        """解析 AND 表达式"""
        left = self._parse_comparison_expression()

        while self.current_token and self.current_token.type == TokenType.AND:
            operator = self.current_token.value
            self._advance()
            right = self._parse_comparison_expression()
            left = LogicalOp(left, operator, right)

        return left

    def _parse_comparison_expression(self) -> Expression:
        """解析比较表达式"""
        left = self._parse_expression()

        if self.current_token and self.current_token.type in (
            TokenType.EQUALS,
            TokenType.LESS_THAN,
            TokenType.GREATER_THAN,
            TokenType.LESS_EQUAL,
            TokenType.GREATER_EQUAL,
            TokenType.NOT_EQUAL,
        ):
            operator = self.current_token.value
            self._advance()
            right = self._parse_expression()
            return BinaryOp(left, operator, right)

        return left

    def _parse_expression(self) -> Expression:
        """解析基本表达式"""
        if self.current_token.type == TokenType.IDENTIFIER:
            column_name = self.current_token.value
            self._advance()

            # 检查表前缀
            if self.current_token and self.current_token.type == TokenType.DOT:
                self._advance()
                table_name = column_name
                column_name = self._expect(TokenType.IDENTIFIER).value
                return ColumnRef(column_name, table_name)
            else:
                return ColumnRef(column_name)

        elif self.current_token.type == TokenType.NUMBER:
            value = self.current_token.value
            self._advance()

            if "." in value:
                return Literal(float(value), "FLOAT")
            else:
                return Literal(int(value), "INTEGER")

        elif self.current_token.type == TokenType.STRING:
            value = self.current_token.value
            self._advance()
            return Literal(value, "STRING")

        elif self.current_token.type == TokenType.NULL:
            self._advance()
            return Literal(None, "NULL")

        # 添加布尔值处理
        elif self.current_token.type == TokenType.TRUE:
            self._advance()
            return Literal(True, "BOOLEAN")

        elif self.current_token.type == TokenType.FALSE:
            self._advance()
            return Literal(False, "BOOLEAN")

        else:
            raise SyntaxError(f"不期望的token: {self.current_token.value}")

    def _parse_create_statement(self) -> Statement:
        """解析CREATE语句（TABLE或INDEX）"""
        self._expect(TokenType.CREATE)

        # 检查是否是UNIQUE INDEX
        is_unique = False
        if self.current_token and self.current_token.type == TokenType.UNIQUE:
            is_unique = True
            self._advance()

        if self.current_token.type == TokenType.TABLE:
            # 回退一步，让原有的解析方法处理
            self.position -= 1
            self.current_token = self.tokens[self.position]
            return self._parse_create_table()
        elif self.current_token.type == TokenType.INDEX:
            return self._parse_create_index(is_unique)
        else:
            raise SyntaxError(f"期望TABLE或INDEX，但得到{self.current_token.value}")

    def _parse_create_index(self, is_unique: bool = False) -> CreateIndexStatement:
        """解析CREATE [UNIQUE] INDEX语句"""
        self._expect(TokenType.INDEX)

        index_name = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.ON)
        table_name = self._expect(TokenType.IDENTIFIER).value

        self._expect(TokenType.LEFT_PAREN)
        column_name = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.RIGHT_PAREN)

        return CreateIndexStatement(index_name, table_name, column_name, is_unique)

    def _parse_drop_statement(self) -> Statement:
        """解析DROP语句"""
        self._expect(TokenType.DROP)

        if self.current_token.type == TokenType.INDEX:
            self._advance()
            index_name = self._expect(TokenType.IDENTIFIER).value
            return DropIndexStatement(index_name)
        else:
            raise SyntaxError(
                f"目前只支持DROP INDEX，但得到DROP {self.current_token.value}"
            )
