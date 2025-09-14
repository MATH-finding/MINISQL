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
        """解析SQL语句（token流已去除#注释）"""
        # 检查是否有中文分号
        for t in self.tokens:
            if t.value == '；':
                raise SyntaxError("仅支持英文分号 ';' 作为语句结束符，检测到中文分号 '；'")
        # # 语句必须以英文分号结尾
        # if not self.tokens or self.tokens[-1].type != TokenType.SEMICOLON:
        #     raise SyntaxError("SQL语句必须以英文分号 ';' 结尾")
        # DEBUG: 入口日志
        # print(f"[PARSER DEBUG] parse() entry, current_token={self.current_token}")
        if not self.current_token or self.current_token.type == TokenType.EOF:
            raise SyntaxError("空的SQL语句")
        if self.current_token.type == TokenType.CREATE:
            # CREATE USER
            if self._peek_token_type(1) == TokenType.USER:
                return self._parse_create_user()
            # CREATE VIEW
            elif self._peek_token_type(1) == TokenType.VIEW:
                # print("[PARSER DEBUG] dispatching to _parse_create_view")
                return self._parse_create_view()
            # CREATE TRIGGER
            elif self._peek_token_type(1) == TokenType.TRIGGER:
                return self._parse_create_trigger()
            # CREATE TABLE/INDEX
            else:
                # print("[PARSER DEBUG] dispatching to _parse_create_statement")
                return self._parse_create_statement()
        elif self.current_token.type == TokenType.DROP:
            # DROP USER
            if self._peek_token_type(1) == TokenType.USER:
                return self._parse_drop_user()
            # DROP VIEW
            elif self._peek_token_type(1) == TokenType.VIEW:
                # print("[PARSER DEBUG] dispatching to _parse_drop_view")
                return self._parse_drop_view()
            # DROP TRIGGER
            elif self._peek_token_type(1) == TokenType.TRIGGER:
                return self._parse_drop_trigger()
            # DROP TABLE/INDEX
            else:
                # print("[PARSER DEBUG] dispatching to _parse_drop_statement")
                return self._parse_drop_statement()
        elif self.current_token.type == TokenType.GRANT:
            return self._parse_grant()
        elif self.current_token.type == TokenType.REVOKE:
            return self._parse_revoke()
        elif self.current_token.type == TokenType.INSERT:
            # print("[PARSER DEBUG] dispatching to _parse_insert")
            return self._parse_insert()
        elif self.current_token.type == TokenType.SELECT:
            # print("[PARSER DEBUG] dispatching to _parse_select")
            return self._parse_select()
        elif self.current_token.type == TokenType.UPDATE:  # 新增
            # print("[PARSER DEBUG] dispatching to _parse_update")
            return self._parse_update()
        elif self.current_token.type == TokenType.DELETE:  # 新增
            # print("[PARSER DEBUG] dispatching to _parse_delete")
            return self._parse_delete()
        elif self.current_token.type == TokenType.BEGIN:
            return self._parse_begin()
        elif self.current_token.type == TokenType.START:
            return self._parse_start_transaction()
        elif self.current_token.type == TokenType.COMMIT:
            return self._parse_commit()
        elif self.current_token.type == TokenType.ROLLBACK:
            return self._parse_rollback()
        elif self.current_token.type == TokenType.SET:
            return self._parse_set()
        elif self.current_token.type == TokenType.TRUNCATE:
            return self._parse_truncate()
        elif self.current_token.type == TokenType.ALTER:
            return self._parse_alter_table()
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

    def _peek_token_type(self, offset):
        pos = self.position + offset
        if 0 <= pos < len(self.tokens):
            return self.tokens[pos].type
        return None

    # def _parse_create_table(self) -> CreateTableStatement:
    #     """解析 CREATE TABLE 语句，支持 IF NOT EXISTS"""
    #     self._expect(TokenType.CREATE)
    #     if_not_exists = False
    #     if self.current_token.type == TokenType.IF:
    #         self._advance()
    #         if self.current_token.type == TokenType.NOT:
    #             self._advance()
    #             self._expect(TokenType.EXISTS)
    #             if_not_exists = True
    #     self._expect(TokenType.TABLE)

    #     table_name = self._expect(TokenType.IDENTIFIER).value
    #     self._expect(TokenType.LEFT_PAREN)

    #     columns = []
    #     while self.current_token.type != TokenType.RIGHT_PAREN:
    #         column = self._parse_column_definition()
    #         columns.append(column)

    #         if self.current_token.type == TokenType.COMMA:
    #             self._advance()
    #         elif self.current_token.type != TokenType.RIGHT_PAREN:
    #             raise SyntaxError("列定义之间需要逗号分隔")

    #     self._expect(TokenType.RIGHT_PAREN)
    #     return CreateTableStatement(table_name, columns, if_not_exists=if_not_exists)

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
                # 使用 WHERE 表达式解析，以支持比较/逻辑表达式
                check = self._parse_where_expression()
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

        # GROUP BY 子句（可选）
        group_by = []
        if self.current_token and self.current_token.type == TokenType.GROUP and self._peek_token_type(1) == TokenType.BY:
            self._advance()  # GROUP
            self._advance()  # BY
            while True:
                if self.current_token.type == TokenType.IDENTIFIER:
                    name = self._expect(TokenType.IDENTIFIER).value
                    if self.current_token and self.current_token.type == TokenType.DOT:
                        self._advance()
                        table_name = name
                        col_name = self._expect(TokenType.IDENTIFIER).value
                        group_by.append(ColumnRef(col_name, table_name))
                    else:
                        group_by.append(ColumnRef(name))
                else:
                    raise SyntaxError("GROUP BY 期望列名")
                if self.current_token and self.current_token.type == TokenType.COMMA:
                    self._advance()
                else:
                    break

        # ORDER BY 子句（可选）
        order_by = []
        if self.current_token and self.current_token.type == TokenType.ORDER and self._peek_token_type(1) == TokenType.BY:
            self._advance()  # ORDER
            self._advance()  # BY
            while True:
                # 列名或标识符
                if self.current_token.type == TokenType.IDENTIFIER:
                    name = self._expect(TokenType.IDENTIFIER).value
                    if self.current_token and self.current_token.type == TokenType.DOT:
                        self._advance()
                        table_name = name
                        col_name = self._expect(TokenType.IDENTIFIER).value
                        expr = ColumnRef(col_name, table_name)
                    else:
                        expr = ColumnRef(name)
                else:
                    raise SyntaxError("ORDER BY 期望列名")
                # 方向（可选）
                direction = "ASC"
                if self.current_token and self.current_token.type in (TokenType.ASC, TokenType.DESC):
                    direction = self.current_token.value.upper()
                    self._advance()
                order_by.append(OrderItem(expr, direction))
                if self.current_token and self.current_token.type == TokenType.COMMA:
                    self._advance()
                else:
                    break

        # DEBUG: 打印SELECT解析结果摘要
        try:
            cols_repr = ", ".join(str(c) for c in columns)
        except Exception:
            cols_repr = str(columns)
        # print(f"[PARSER DEBUG] SELECT parsed: columns=[{cols_repr}], from={left}, where={where_clause}")
        return SelectStatement(columns, left, where_clause, group_by, order_by)

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
        # --- 在这里加入唯一的调试代码 ---
        # print(f"DEBUG: [Parser._parse_expression] received token: {self.current_token}")
        # --- 调试代码结束 ---

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
            value = self.current_token.value.strip("'")
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

    # def _parse_create_statement(self) -> Statement:
    #     """解析CREATE语句（TABLE或INDEX）"""
    #     self._expect(TokenType.CREATE)

    #     # 检查是否是UNIQUE INDEX
    #     is_unique = False
    #     if self.current_token and self.current_token.type == TokenType.UNIQUE:
    #         is_unique = True
    #         self._advance()

    #     if self.current_token.type == TokenType.TABLE:
    #         # 回退一步，让原有的解析方法处理
    #         self.position -= 1
    #         self.current_token = self.tokens[self.position]
    #         return self._parse_create_table()
    #     elif self.current_token.type == TokenType.INDEX:
    #         return self._parse_create_index(is_unique)
    #     else:
    #         raise SyntaxError(f"期望TABLE或INDEX，但得到{self.current_token.value}")

    def _parse_if_exists_flags(self, support_not=True):
        """辅助解析 IF [NOT] EXISTS，返回 (if_exists, if_not_exists)"""
        if_exists = False
        if_not_exists = False
        if self.current_token and self.current_token.type == TokenType.IF:
            self._advance()
            if support_not and self.current_token and self.current_token.type == TokenType.NOT:
                self._advance()
                self._expect(TokenType.EXISTS)
                if_not_exists = True
            else:
                self._expect(TokenType.EXISTS)
                if_exists = True
        return if_exists, if_not_exists

    # def parse_if_exists(support_not=True):
    #     def decorator(parse_func):
    #         def wrapper(self, *args, **kwargs):
    #             if_exists, if_not_exists = self._parse_if_exists_flags(support_not)
    #             return parse_func(self, *args, if_exists=if_exists, if_not_exists=if_not_exists, **kwargs)
    #         return wrapper
    #     return decorator

    def _parse_create_table(self):
        self._expect(TokenType.TABLE)
        # 在 CREATE TABLE 之后，在表名之前，解析 IF NOT EXISTS
        _, if_not_exists = self._parse_if_exists_flags(support_not=True)
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
        return CreateTableStatement(table_name, columns, if_not_exists=if_not_exists)

    def _parse_drop_table(self):
        self._expect(TokenType.TABLE)
        # 在 DROP TABLE 之后，在表名之前，解析 IF EXISTS
        if_exists, _ = self._parse_if_exists_flags(support_not=False)
        table_name = self._expect(TokenType.IDENTIFIER).value
        return DropTableStatement(table_name, if_exists=if_exists)

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


    def _parse_create_index(self, is_unique: bool = False):
        self._expect(TokenType.INDEX)
        # 在 CREATE INDEX 之后，在表名之前，解析 IF NOT EXISTS
        _, if_not_exists = self._parse_if_exists_flags(support_not=True)

        index_name = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.ON)
        table_name = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.LEFT_PAREN)
        column_name = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.RIGHT_PAREN)
        return CreateIndexStatement(index_name, table_name, column_name, is_unique, if_not_exists=if_not_exists)

    def _parse_drop_index(self):
        self._expect(TokenType.INDEX)
        # 在 DROP INDEX 之后，在表名之前，解析 IF EXISTS
        if_exists, _ = self._parse_if_exists_flags(support_not=False)
        index_name = self._expect(TokenType.IDENTIFIER).value
        return DropIndexStatement(index_name, if_exists=if_exists)


    def _parse_create_view(self):
        self._expect(TokenType.CREATE)
        self._expect(TokenType.VIEW)

        _, if_not_exists = self._parse_if_exists_flags(support_not=True)

        view_name = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.AS)

        # --- 修改开始 ---
        # 从当前位置开始，收集tokens直到分号或EOF
        start_pos = self.position
        while self.current_token and self.current_token.type != TokenType.SEMICOLON and self.current_token.type != TokenType.EOF:
            self._advance()

        end_pos = self.position
        definition_tokens = self.tokens[start_pos:end_pos]
        # --- 修改结束 ---

        parts = []
        for token in definition_tokens:
            # 这里原来的逻辑保持不变
            if token.type == TokenType.STRING:
                parts.append("'" + token.value.replace("'", "''") + "'")
            else:
                parts.append(token.value)

        view_definition = " ".join(parts).strip()
        return CreateViewStatement(view_name, view_definition, if_not_exists=if_not_exists)

    def _parse_drop_view(self):
        self._expect(TokenType.DROP)
        self._expect(TokenType.VIEW)
        # 在 DROP VIEW 之后，在表名之前，解析 IF EXISTS
        if_exists, _ = self._parse_if_exists_flags(support_not=False)
        view_name = self._expect(TokenType.IDENTIFIER).value
        return DropViewStatement(view_name, if_exists=if_exists)


    def _parse_create_user(self):
        self._expect(TokenType.CREATE)
        self._expect(TokenType.USER)
        # 在 CREATE USER 之后，在表名之前，解析 IF NOT EXISTS
        _, if_not_exists = self._parse_if_exists_flags(support_not=True)
        username = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.IDENTIFIED)
        self._expect(TokenType.BY)
        password = self._expect(TokenType.STRING).value.strip("'")
        return CreateUserStatement(username, password, if_not_exists=if_not_exists)

    def _parse_drop_user(self):
        self._expect(TokenType.DROP)
        self._expect(TokenType.USER)
        # 在 DROP USER 之后，在表名之前，解析 IF EXISTS
        if_exists, _ = self._parse_if_exists_flags(support_not=False)
        username = self._expect(TokenType.IDENTIFIER).value
        return DropUserStatement(username, if_exists=if_exists)

    # 在 _parse_create_statement/_parse_drop_statement 里分发到上述方法
    def _parse_create_statement(self):
        self._expect(TokenType.CREATE)
        if self.current_token.type == TokenType.UNIQUE:
            self._advance()
            return self._parse_create_index(is_unique=True)
        elif self.current_token.type == TokenType.INDEX:
            return self._parse_create_index(is_unique=False)
        elif self.current_token.type == TokenType.TABLE:
            return self._parse_create_table()
        else:
            raise SyntaxError(f"期望TABLE或INDEX，但得到{self.current_token.value}")

    def _parse_drop_statement(self):
        self._expect(TokenType.DROP)
        if self.current_token.type == TokenType.INDEX:
            return self._parse_drop_index()
        elif self.current_token.type == TokenType.TABLE:
            return self._parse_drop_table()
        else:
            raise SyntaxError(f"DROP语句支持INDEX或TABLE，但得到 {self.current_token.value}")

    def _parse_grant(self) -> GrantStatement:
        """解析 GRANT 语句"""
        self._expect(TokenType.GRANT)

        # 解析权限类型 - 修复：支持关键字作为权限名
        if self.current_token.type == TokenType.ALL:
            privilege = "ALL"
            self._advance()
            if self.current_token.type == TokenType.PRIVILEGES:
                self._advance()
        else:
            # 修复：支持SELECT、INSERT等关键字作为权限名
            if self.current_token.type in (
                TokenType.SELECT,
                TokenType.INSERT,
                TokenType.UPDATE,
                TokenType.DELETE,
                TokenType.CREATE,
                TokenType.DROP,
            ):
                privilege = self.current_token.value.upper()
                self._advance()
            else:
                privilege = self._expect(TokenType.IDENTIFIER).value.upper()

        self._expect(TokenType.ON)
        table_name = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.TO)
        username = self._expect(TokenType.IDENTIFIER).value

        return GrantStatement(privilege, table_name, username)

    def _parse_revoke(self) -> RevokeStatement:
        """解析 REVOKE 语句"""
        self._expect(TokenType.REVOKE)

        # 解析权限类型 - 修复：支持关键字作为权限名
        if self.current_token.type == TokenType.ALL:
            privilege = "ALL"
            self._advance()
            if self.current_token.type == TokenType.PRIVILEGES:
                self._advance()
        else:
            # 修复：支持SELECT、INSERT等关键字作为权限名
            if self.current_token.type in (
                TokenType.SELECT,
                TokenType.INSERT,
                TokenType.UPDATE,
                TokenType.DELETE,
                TokenType.CREATE,
                TokenType.DROP,
            ):
                privilege = self.current_token.value.upper()
                self._advance()
            else:
                privilege = self._expect(TokenType.IDENTIFIER).value.upper()

        self._expect(TokenType.ON)
        table_name = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.FROM)
        username = self._expect(TokenType.IDENTIFIER).value

        return RevokeStatement(privilege, table_name, username)

    def _parse_begin(self) -> Statement:
        self._expect(TokenType.BEGIN)
        if self.current_token and self.current_token.type == TokenType.TRANSACTION:
            self._advance()
        return BeginTransaction()

    def _parse_start_transaction(self) -> Statement:
        self._expect(TokenType.START)
        self._expect(TokenType.TRANSACTION)
        return BeginTransaction()

    def _parse_commit(self) -> Statement:
        self._expect(TokenType.COMMIT)
        return CommitTransaction()

    def _parse_rollback(self) -> Statement:
        self._expect(TokenType.ROLLBACK)
        return RollbackTransaction()

    def _parse_set(self) -> Statement:
        self._expect(TokenType.SET)
        # 两种形式： SET AUTOCOMMIT=0|1 或 SET SESSION TRANSACTION ISOLATION LEVEL ...
        if self.current_token.type == TokenType.AUTOCOMMIT:
            self._advance()
            self._expect(TokenType.EQUALS)
            if self.current_token.type == TokenType.NUMBER and self.current_token.value in ("0", "1"):
                enabled = self.current_token.value == "1"
                self._advance()
                return SetAutocommit(enabled)
            else:
                raise SyntaxError("AUTOCOMMIT 只能为 0 或 1")
        elif self.current_token.type == TokenType.SESSION:
            self._advance()
            self._expect(TokenType.TRANSACTION)
            self._expect(TokenType.ISOLATION)
            self._expect(TokenType.LEVEL)
            # 解析隔离级别
            if self.current_token.type == TokenType.READ:
                self._advance()
                if self.current_token.type == TokenType.COMMITTED_KW:
                    self._advance()
                    return SetIsolationLevel("READ COMMITTED")
                elif self.current_token.type == TokenType.UNCOMMITTED_KW:
                    self._advance()
                    return SetIsolationLevel("READ UNCOMMITTED")
                elif self.current_token.type == TokenType.REPEATABLE:
                    self._advance()
                    self._expect(TokenType.READ)
                    return SetIsolationLevel("REPEATABLE READ")
                else:
                    raise SyntaxError("未知的隔离级别: READ ...")
            elif self.current_token.type == TokenType.SERIALIZABLE:
                self._advance()
                return SetIsolationLevel("SERIALIZABLE")
            else:
                raise SyntaxError("未知的隔离级别")
        else:
            raise SyntaxError("仅支持: SET AUTOCOMMIT 或 SET SESSION TRANSACTION ISOLATION LEVEL ...")

    def _parse_create_trigger(self) -> CreateTriggerStatement:
        """解析 CREATE TRIGGER 语句"""
        self._expect(TokenType.CREATE)
        self._expect(TokenType.TRIGGER)
        
        # 触发器名称
        trigger_name = self._expect(TokenType.IDENTIFIER).value
        
        # 时机: BEFORE 或 AFTER
        if self.current_token.type not in (TokenType.BEFORE, TokenType.AFTER):
            raise SyntaxError(f"期望 BEFORE 或 AFTER，但得到 {self.current_token.value}")
        timing = self.current_token.value
        self._advance()
        
        # 事件: INSERT, UPDATE, DELETE
        if self.current_token.type not in (TokenType.INSERT, TokenType.UPDATE, TokenType.DELETE):
            raise SyntaxError(f"期望 INSERT, UPDATE 或 DELETE，但得到 {self.current_token.value}")
        event = self.current_token.value
        self._advance()
        
        # ON 关键字
        self._expect(TokenType.ON)
        
        # 表名
        table_name = self._expect(TokenType.IDENTIFIER).value
        
        # FOR EACH ROW
        self._expect(TokenType.FOR)
        self._expect(TokenType.EACH)
        self._expect(TokenType.ROW)
        
        # 触发器体（简化为单个SQL语句字符串）
        statement_tokens = []
        while (self.current_token and 
               self.current_token.type != TokenType.SEMICOLON and 
               self.current_token.type != TokenType.EOF):
            statement_tokens.append(self.current_token.value)
            self._advance()
        
        if not statement_tokens:
            raise SyntaxError("触发器体不能为空")
            
        statement = " ".join(statement_tokens)
        
        return CreateTriggerStatement(trigger_name, timing, event, table_name, statement)

    def _parse_drop_trigger(self) -> DropTriggerStatement:
        """解析 DROP TRIGGER 语句"""
        self._expect(TokenType.DROP)
        self._expect(TokenType.TRIGGER)
        
        # 检查 IF EXISTS
        if_exists = False
        if (self.current_token and self.current_token.type == TokenType.IF):
            self._advance()
            self._expect(TokenType.EXISTS)
            if_exists = True
        
        # 触发器名称
        trigger_name = self._expect(TokenType.IDENTIFIER).value
        
        return DropTriggerStatement(trigger_name, if_exists)

    def _parse_alter_table(self):
        self._expect(TokenType.ALTER)
        self._expect(TokenType.TABLE)
        table_name = self._expect(TokenType.IDENTIFIER).value
        if self.current_token.type == TokenType.ADD:
            self._advance()
            self._expect(TokenType.COLUMN)
            col_def = self._parse_column_definition()
            return AlterTableStatement(table_name, 'ADD', column_def=col_def)
        elif self.current_token.type == TokenType.DROP:
            self._advance()
            self._expect(TokenType.COLUMN)
            col_name = self._expect(TokenType.IDENTIFIER).value
            return AlterTableStatement(table_name, 'DROP', column_name=col_name)
        else:
            raise SyntaxError(f"ALTER TABLE 仅支持 ADD COLUMN 或 DROP COLUMN, 得到 {self.current_token.value}")
