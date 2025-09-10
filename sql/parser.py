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
        elif self.current_token.type == TokenType.UPDATE:  # 新增
            return self._parse_update()
        elif self.current_token.type == TokenType.DELETE:  # 新增
            return self._parse_delete()
        elif self.current_token.type == TokenType.TRUNCATE:
            return self._parse_truncate()
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
        """解析列定义，支持DEFAULT、CHECK、FOREIGN KEY"""
        column_name = self._expect(TokenType.IDENTIFIER).value

        # 解析数据类型
        if self.current_token.type in (
            TokenType.INTEGER,
            TokenType.VARCHAR,
            TokenType.FLOAT,
            TokenType.BOOLEAN,
            TokenType.CHAR,
            TokenType.DECIMAL,
            TokenType.DATE,
            TokenType.TIME,
            TokenType.DATETIME,
            TokenType.BIGINT,
            TokenType.TINYINT,
            TokenType.TEXT,
        ):
            data_type = self.current_token.value.upper()
            self._advance()
        else:
            raise SyntaxError(f"期望数据类型，但得到 {self.current_token.value}")

        # 解析长度（对于VARCHAR）
        length = None
        precision = None
        scale = None
        if (
            data_type in ("VARCHAR", "CHAR")
            and self.current_token.type == TokenType.LEFT_PAREN
        ):
            self._advance()
            length = int(self._expect(TokenType.NUMBER).value)
            self._expect(TokenType.RIGHT_PAREN)
        elif data_type == "DECIMAL" and self.current_token.type == TokenType.LEFT_PAREN:
            self._advance()
            precision = int(self._expect(TokenType.NUMBER).value)
            if self.current_token.type == TokenType.COMMA:
                self._advance()
                scale = int(self._expect(TokenType.NUMBER).value)
            self._expect(TokenType.RIGHT_PAREN)

        # 解析约束
        constraints = []
        default = None
        check = None
        foreign_key = None
        while self.current_token and self.current_token.type in (
            TokenType.PRIMARY,
            TokenType.NOT,
            TokenType.NULL,
            TokenType.UNIQUE,
            TokenType.DEFAULT,
            TokenType.CHECK,
            TokenType.FOREIGN,
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
            elif self.current_token.type == TokenType.UNIQUE:
                self._advance()
                constraints.append("UNIQUE")
            elif self.current_token.type == TokenType.DEFAULT:
                self._advance()
                # 支持数字、字符串、布尔、NULL
                if self.current_token.type == TokenType.NUMBER:
                    default = self.current_token.value
                    self._advance()
                elif self.current_token.type == TokenType.STRING:
                    default = self.current_token.value
                    self._advance()
                elif self.current_token.type == TokenType.TRUE:
                    default = True
                    self._advance()
                elif self.current_token.type == TokenType.FALSE:
                    default = False
                    self._advance()
                elif self.current_token.type == TokenType.NULL:
                    default = None
                    self._advance()
                else:
                    raise SyntaxError(f"不支持的DEFAULT值: {self.current_token.value}")
            elif self.current_token.type == TokenType.CHECK:
                self._advance()
                self._expect(TokenType.LEFT_PAREN)
                check = self._parse_expression()
                self._expect(TokenType.RIGHT_PAREN)
            elif self.current_token.type == TokenType.FOREIGN:
                self._advance()
                self._expect(TokenType.KEY)
                self._expect(TokenType.REFERENCES)
                ref_table = self._expect(TokenType.IDENTIFIER).value
                self._expect(TokenType.LEFT_PAREN)
                ref_column = self._expect(TokenType.IDENTIFIER).value
                self._expect(TokenType.RIGHT_PAREN)
                foreign_key = {"ref_table": ref_table, "ref_column": ref_column}

        return {
            "name": column_name,
            "type": data_type,
            "length": length,
            "constraints": constraints,
            "default": default,
            "check": check,
            "foreign_key": foreign_key,
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

    def _parse_truncate(self) -> TruncateTableStatement:
        """解析TRUNCATE TABLE语句"""
        self._expect(TokenType.TRUNCATE)
        self._expect(TokenType.TABLE)
        table_name = self._expect(TokenType.IDENTIFIER).value
        return TruncateTableStatement(table_name)

    def _parse_select(self) -> SelectStatement:
        """解析 SELECT 语句"""
        self._expect(TokenType.SELECT)

        # 解析选择列表
        columns = []
        while True:
            # 支持聚合函数
            if self.current_token.type in (
                TokenType.COUNT,
                TokenType.SUM,
                TokenType.AVG,
                TokenType.MIN,
                TokenType.MAX,
            ):
                func_type = self.current_token.type
                self._advance()
                self._expect(TokenType.LEFT_PAREN)
                if self.current_token.type == TokenType.STAR:
                    arg = "*"
                    self._advance()
                else:
                    arg = self._parse_expression()
                self._expect(TokenType.RIGHT_PAREN)
                columns.append(AggregateFunction(func_type.name, arg))
            elif self.current_token.type == TokenType.STAR:
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
        from_table = self._expect(TokenType.IDENTIFIER).value

        # 解析JOIN链
        join_clause = None
        left = from_table
        while self.current_token and self.current_token.type in (
            TokenType.JOIN,
            TokenType.INNER,
            TokenType.LEFT,
            TokenType.RIGHT,
        ):
            # 解析JOIN类型
            if self.current_token.type == TokenType.INNER:
                join_type = "INNER"
                self._advance()
                self._expect(TokenType.JOIN)
            elif self.current_token.type == TokenType.LEFT:
                join_type = "LEFT"
                self._advance()
                self._expect(TokenType.JOIN)
            elif self.current_token.type == TokenType.RIGHT:
                join_type = "RIGHT"
                self._advance()
                self._expect(TokenType.JOIN)
            else:
                join_type = "INNER"  # 默认INNER JOIN
                self._expect(TokenType.JOIN)

            right_table = self._expect(TokenType.IDENTIFIER).value
            self._expect(TokenType.ON)
            on_expr = self._parse_where_expression()
            left = JoinClause(left, right_table, join_type, on_expr)
        # left为最终的from_table（str或JoinClause）

        # WHERE 子句
        where_clause = None
        if self.current_token and self.current_token.type == TokenType.WHERE:
            self._advance()
            where_clause = self._parse_where_expression()

        return SelectStatement(columns, left, where_clause)

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
        elif self.current_token.type == TokenType.TABLE:
            self._advance()
            table_name = self._expect(TokenType.IDENTIFIER).value
            return DropTableStatement(table_name)
        else:
            raise SyntaxError(
                f"DROP语句支持INDEX或TABLE，但得到 {self.current_token.value}"
            )

    def _parse_update(self) -> UpdateStatement:
        """解析UPDATE语句: UPDATE table SET col1=val1, col2=val2 WHERE condition"""
        self._expect(TokenType.UPDATE)

        # 获取表名
        table_name = self._expect(TokenType.IDENTIFIER).value

        # 解析SET子句
        self._expect(TokenType.SET)
        set_clauses = []

        while True:
            # 解析 column = value
            column_name = self._expect(TokenType.IDENTIFIER).value
            self._expect(TokenType.EQUALS)
            value_expr = self._parse_expression()

            set_clauses.append({"column": column_name, "value": value_expr})

            # 检查是否有更多SET子句
            if self.current_token.type == TokenType.COMMA:
                self._expect(TokenType.COMMA)
            else:
                break

        # 可选的WHERE子句
        where_clause = None
        if self.current_token and self.current_token.type == TokenType.WHERE:
            self._expect(TokenType.WHERE)
            where_clause = self._parse_where_expression()  # 使用现有的WHERE解析方法

        return UpdateStatement(table_name, set_clauses, where_clause)

    def _parse_delete(self) -> DeleteStatement:
        """解析DELETE语句: DELETE FROM table WHERE condition"""
        self._expect(TokenType.DELETE)
        self._expect(TokenType.FROM)

        # 获取表名
        table_name = self._expect(TokenType.IDENTIFIER).value

        # 可选的WHERE子句
        where_clause = None
        if self.current_token and self.current_token.type == TokenType.WHERE:
            self._expect(TokenType.WHERE)
            where_clause = self._parse_where_expression()  # 使用现有的WHERE解析方法

        return DeleteStatement(table_name, where_clause)
