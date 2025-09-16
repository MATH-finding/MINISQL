"""
SQL语法分析器
"""

from typing import List, Union
from sql import Token, TokenType
from .ast_nodes import *


class SQLParser:
    """SQL语法分析器
    负责将Token流解析为AST语法树，支持多种SQL语句类型，包括DDL、DML、事务、权限、视图、触发器等。
    """

    def __init__(self, tokens: List[Token]):
        # 初始化，保存token流和当前位置
        self.tokens = tokens
        self.position = 0
        self.current_token = tokens[0] if tokens else None

    def parse(self) -> Statement:
        """解析SQL语句（token流已去除#注释）
        主入口，根据首个token类型分发到不同的解析分支。
        """
        # 检查是否有中文分号，防止中英文符号混用导致解析异常
        for t in self.tokens:
            if t.value == '；':
                raise SyntaxError("仅支持英文分号 ';' 作为语句结束符，检测到中文分号 '；'")
        # 如果token流为空或第一个token就是EOF，说明SQL为空，直接报错
        if not self.current_token or self.current_token.type == TokenType.EOF:
            raise SyntaxError("空的SQL语句")
        # 下面根据首token类型分发到不同的解析分支，每个分支都对应一种SQL语句类型
        if self.current_token.type == TokenType.CREATE:
            # CREATE USER分支，专门处理用户创建
            if self._peek_token_type(1) == TokenType.USER:
                result = self._parse_create_user()
            # CREATE VIEW分支，专门处理视图创建
            elif self._peek_token_type(1) == TokenType.VIEW:
                result = self._parse_create_view()
            # CREATE TRIGGER分支，专门处理触发器创建
            elif self._peek_token_type(1) == TokenType.TRIGGER:
                result = self._parse_create_trigger()
            # 其他CREATE分支，统一处理TABLE/INDEX等
            else:
                result = self._parse_create_statement()
        elif self.current_token.type == TokenType.DROP:
            # DROP USER分支，专门处理用户删除
            if self._peek_token_type(1) == TokenType.USER:
                result = self._parse_drop_user()
            # DROP VIEW分支，专门处理视图删除
            elif self._peek_token_type(1) == TokenType.VIEW:
                result = self._parse_drop_view()
            # DROP TRIGGER分支，专门处理触发器删除
            elif self._peek_token_type(1) == TokenType.TRIGGER:
                result = self._parse_drop_trigger()
            # 其他DROP分支，统一处理TABLE/INDEX等
            else:
                result = self._parse_drop_statement()
        elif self.current_token.type == TokenType.GRANT:
            # 权限授予语句
            result = self._parse_grant()
        elif self.current_token.type == TokenType.REVOKE:
            # 权限回收语句
            result = self._parse_revoke()
        elif self.current_token.type == TokenType.INSERT:
            # 插入语句
            result = self._parse_insert()
        elif self.current_token.type == TokenType.SELECT:
            # 查询语句
            result = self._parse_select()
        elif self.current_token.type == TokenType.UPDATE:  
            # 更新语句
            result = self._parse_update()
        elif self.current_token.type == TokenType.DELETE:  
            # 删除语句
            result = self._parse_delete()
        elif self.current_token.type == TokenType.BEGIN:
            # BEGIN事务语句
            result = self._parse_begin()
        elif self.current_token.type == TokenType.START:
            # START TRANSACTION事务语句，区别于BEGIN
            result = self._parse_start_transaction()
        elif self.current_token.type == TokenType.COMMIT:
            # 提交事务
            result = self._parse_commit()
        elif self.current_token.type == TokenType.ROLLBACK:
            # 回滚事务
            result = self._parse_rollback()
        elif self.current_token.type == TokenType.SET:
            # SET相关配置
            result = self._parse_set()
        elif self.current_token.type == TokenType.TRUNCATE:
            # TRUNCATE TABLE语句
            result = self._parse_truncate()
        elif self.current_token.type == TokenType.ALTER:
            # ALTER TABLE语句
            result = self._parse_alter_table()
        elif self.current_token.type == TokenType.SHOW:
            # SHOW相关配置
            result = self._parse_show()
            # 游标
        elif self.current_token.type == TokenType.OPEN:
            result = self._parse_open_cursor()
        elif self.current_token.type == TokenType.FETCH:
            result = self._parse_fetch_cursor()
        elif self.current_token.type == TokenType.CLOSE:
            result = self._parse_close_cursor()
        else:
            # 兜底分支：遇到未知或不支持的语句类型，直接报错
            raise SyntaxError(f"不支持的语句类型: {self.current_token.value}")
        # 统一要求所有SQL语句必须以英文分号结尾
        self._expect(TokenType.SEMICOLON)
        return result

    def _advance(self):
        """移动到下一个token"""
        if self.position < len(self.tokens) - 1:
            self.position += 1
            self.current_token = self.tokens[self.position]

    def _expect(self, expected_type: TokenType) -> Token:
        """期望特定类型的token
        若当前token类型不符则抛出带行列号的语法错误。
        """
        if not self.current_token or self.current_token.type != expected_type:
            line = self.current_token.line if self.current_token else -1
            column = self.current_token.column if self.current_token else -1
            actual = self.current_token.value if self.current_token else 'EOF'
            raise SyntaxError(str([line, column, f"期望{expected_type.value}, 实际{actual}"]))
        token = self.current_token
        self._advance()
        return token

    def _peek_token_type(self, offset):
        # 向前窥视offset个token，返回其类型
        pos = self.position + offset
        if 0 <= pos < len(self.tokens):
            return self.tokens[pos].type
        return None


    def _parse_column_definition(self) -> dict:
        """解析列定义，支持DEFAULT、CHECK、FOREIGN KEY
        返回列名、类型、长度、约束、默认值、检查、外键等信息。
        """
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
           raise SyntaxError(str([self.current_token.line, self.current_token.column, f"期望数据类型，但得到 {self.current_token.value}"]))

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
                    raise SyntaxError(str([self.current_token.line, self.current_token.column, f"不支持的DEFAULT值: {self.current_token.value}"]))
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
        """解析 INSERT 语句
        支持多行插入、可选列名。
        """
        start_line, start_column = self.current_token.line, self.current_token.column
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
                    raise SyntaxError(str([self.current_token.line, self.current_token.column, "列名之间需要逗号分隔"]))
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
                    raise SyntaxError(str([self.current_token.line, self.current_token.column, "值之间需要逗号分隔"]))

            self._expect(TokenType.RIGHT_PAREN)
            values.append(row_values)

            # 检查是否还有更多值行
            if self.current_token.type == TokenType.COMMA:
                self._advance()
            else:
                break

        return InsertStatement(table_name, columns, values, start_line, start_column)

    def _parse_truncate(self) -> TruncateTableStatement:
        """解析TRUNCATE TABLE语句"""
        self._expect(TokenType.TRUNCATE)
        self._expect(TokenType.TABLE)
        table_name = self._expect(TokenType.IDENTIFIER).value
        return TruncateTableStatement(table_name)

    def _parse_select(self) -> SelectStatement:
        """解析 SELECT 语句
        支持聚合、JOIN、WHERE、GROUP BY、ORDER BY等子句。
        """
        self._expect(TokenType.SELECT)

        # 解析选择列表（SELECT后面跟的字段/表达式/聚合函数等）
        columns = []
        while True:
            # 修复：优先判断SELECT *，防止*被_parse_expression误判
            if self.current_token.type == TokenType.STAR:
                columns.append("*")
                self._advance()
            # 如果是聚合函数（如COUNT/SUM等），则特殊处理
            elif self.current_token.type in (
                TokenType.COUNT,
                TokenType.SUM,
                TokenType.AVG,
                TokenType.MIN,
                TokenType.MAX,
            ):
                func_type = self.current_token.type
                line = self.current_token.line
                column = self.current_token.column
                self._advance()
                self._expect(TokenType.LEFT_PAREN)
                # 支持COUNT(*)
                if self.current_token.type == TokenType.STAR:
                    arg = "*"
                    self._advance()
                else:
                    # 解析聚合函数参数，可以是表达式
                    arg = self._parse_expression()
                self._expect(TokenType.RIGHT_PAREN)
                # 构造聚合函数AST节点
                columns.append(AggregateFunction(func_type.name, arg, line, column))
            else:
                # 普通列名或带表前缀的列
                column_name = self._expect(TokenType.IDENTIFIER).value
                # 记录当前token的位置信息
                line = self.tokens[self.position - 1].line if self.position > 0 else None
                column = self.tokens[self.position - 1].column if self.position > 0 else None
                if self.current_token and self.current_token.type == TokenType.DOT:
                    self._advance()
                    table_name = column_name
                    column_name = self._expect(TokenType.IDENTIFIER).value
                    columns.append(ColumnRef(column_name, table_name, line, column))
                else:
                    columns.append(ColumnRef(column_name, None, line, column))

            # 逗号分隔多个字段，遇到逗号则继续循环，否则跳出
            if self.current_token.type == TokenType.COMMA:
                self._advance()
            else:
                break

        # 解析FROM子句，获取主表名和可选别名
        self._expect(TokenType.FROM)
        from_table_name = self._expect(TokenType.IDENTIFIER).value
        from_table_alias = None
        if self.current_token and self.current_token.type == TokenType.IDENTIFIER:
            from_table_alias = self._expect(TokenType.IDENTIFIER).value
        # 修复：只传表名字符串，不传元组，别名单独处理
        left = from_table_name
        # 支持多表JOIN，循环处理所有JOIN子句
        while self.current_token and self.current_token.type in (
            TokenType.JOIN,
            TokenType.INNER,
            TokenType.LEFT,
            TokenType.RIGHT,
        ):
            # 判断JOIN类型
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
                # 默认JOIN类型为INNER
                join_type = "INNER"
                self._expect(TokenType.JOIN)

            # 解析右表名和可选别名
            right_table_name = self._expect(TokenType.IDENTIFIER).value
            right_table_alias = None
            if self.current_token and self.current_token.type == TokenType.IDENTIFIER:
                right_table_alias = self._expect(TokenType.IDENTIFIER).value
            # 解析ON条件表达式
            self._expect(TokenType.ON)
            on_expr = self._parse_where_expression()
            # 构造JOIN AST节点，left链式连接
            left = JoinClause(left, right_table_name, join_type, on_expr)
        # left为最终的from_table（str或JoinClause）

        # 解析可选的WHERE子句
        where_clause = None
        if self.current_token and self.current_token.type == TokenType.WHERE:
            self._advance()
            where_clause = self._parse_where_expression()

        # 解析可选的GROUP BY子句，支持多列
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
                    # GROUP BY分支：遇到非标识符，抛出语法错误
                    raise SyntaxError(str([self.current_token.line, self.current_token.column, "GROUP BY 期望列名"]))
                if self.current_token and self.current_token.type == TokenType.COMMA:
                    self._advance()
                else:
                    break

        # 解析可选的ORDER BY子句，支持多列和排序方向
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
                    # ORDER BY分支：遇到非标识符，抛出语法错误
                    raise SyntaxError(str([self.current_token.line, self.current_token.column, "ORDER BY 期望列名"]))
                # 解析排序方向，默认为ASC
                direction = "ASC"
                if self.current_token and self.current_token.type in (TokenType.ASC, TokenType.DESC):
                    direction = self.current_token.value.upper()
                    self._advance()
                order_by.append(OrderItem(expr, direction))
                if self.current_token and self.current_token.type == TokenType.COMMA:
                    self._advance()
                else:
                    break

        # 构造SELECT AST节点，包含所有子句
        return SelectStatement(columns, left, where_clause, group_by, order_by)

    def _parse_where_expression(self) -> Expression:
        """解析 WHERE 表达式（入口）"""
        return self._parse_or_expression()

    def _parse_or_expression(self) -> Expression:
        """解析 OR 表达式，左递归"""
        left = self._parse_and_expression()

        while self.current_token and self.current_token.type == TokenType.OR:
            operator = self.current_token.value
            self._advance()
            right = self._parse_and_expression()
            left = LogicalOp(left, operator, right)

        return left

    def _parse_and_expression(self) -> Expression:
        """解析 AND 表达式，左递归"""
        left = self._parse_comparison_expression()

        while self.current_token and self.current_token.type == TokenType.AND:
            operator = self.current_token.value
            self._advance()
            right = self._parse_comparison_expression()
            left = LogicalOp(left, operator, right)

        return left

    def _parse_comparison_expression(self) -> Expression:
        """解析比较表达式，支持=、!=、<、<=、>、>="""
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
        """解析基本表达式，支持列、字面量、NULL、布尔、简单二元算术运算（+ - * /）"""
        left = self._parse_primary()
        while self.current_token and self.current_token.type in (TokenType.STAR, TokenType.PLUS, TokenType.MINUS, TokenType.SLASH):
            op_token = self.current_token
            self._advance()
            right = self._parse_primary()
            left = BinaryOp(left, op_token.value, right)
        return left

    def _parse_primary(self) -> Expression:
        if self.current_token.type == TokenType.IDENTIFIER:
            column_name = self.current_token.value
            line = self.current_token.line
            column = self.current_token.column
            self._advance()
            if self.current_token and self.current_token.type == TokenType.DOT:
                self._advance()
                table_name = column_name
                column_name = self._expect(TokenType.IDENTIFIER).value
                return ColumnRef(column_name, table_name, line, column)
            else:
                return ColumnRef(column_name, None, line, column)
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
        elif self.current_token.type == TokenType.TRUE:
            self._advance()
            return Literal(True, "BOOLEAN")
        elif self.current_token.type == TokenType.FALSE:
            self._advance()
            return Literal(False, "BOOLEAN")
        else:
            raise SyntaxError(str([self.current_token.line, self.current_token.column, f"不期望的token: {self.current_token.value}"]))


    def _parse_if_exists_flags(self, support_not=True):
        """辅助解析 IF [NOT] EXISTS，返回 (if_exists, if_not_exists)
        用于CREATE/DROP等语句的可选修饰。
        """
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

    def _parse_create_table(self, start_line, start_column):
        # 解析 CREATE TABLE 语句，支持 IF NOT EXISTS
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
                raise SyntaxError(str([self.current_token.line, self.current_token.column, "列定义之间需要逗号分隔"]))
        self._expect(TokenType.RIGHT_PAREN)
        return CreateTableStatement(table_name, columns, if_not_exists=if_not_exists, line=start_line, column=start_column)

    def _parse_drop_table(self, start_line, start_column):
        # 解析 DROP TABLE 语句，支持 IF EXISTS
        self._expect(TokenType.TABLE)
        # 在 DROP TABLE 之后，在表名之前，解析 IF EXISTS
        if_exists, _ = self._parse_if_exists_flags(support_not=False)
        table_name = self._expect(TokenType.IDENTIFIER).value
        return DropTableStatement(table_name, if_exists=if_exists, line=start_line, column=start_column)

    def _parse_update(self) -> UpdateStatement:
        """解析UPDATE语句: UPDATE table SET col1=val1, col2=val2 WHERE condition
        支持多列赋值和可选WHERE。
        """
        start_line, start_column = self.current_token.line, self.current_token.column
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

        return UpdateStatement(table_name, set_clauses, where_clause, start_line, start_column)

    def _parse_delete(self) -> DeleteStatement:
        """解析DELETE语句: DELETE FROM table WHERE condition
        支持可选WHERE。
        """
        start_line, start_column = self.current_token.line, self.current_token.column
        self._expect(TokenType.DELETE)
        self._expect(TokenType.FROM)

        # 获取表名
        table_name = self._expect(TokenType.IDENTIFIER).value

        # 可选的WHERE子句
        where_clause = None
        if self.current_token and self.current_token.type == TokenType.WHERE:
            self._expect(TokenType.WHERE)
            where_clause = self._parse_where_expression()  # 使用现有的WHERE解析方法

        return DeleteStatement(table_name, where_clause, start_line, start_column)


    def _parse_create_index(self, is_unique: bool = False):
        # 解析 CREATE [UNIQUE] INDEX 语句，支持 IF NOT EXISTS
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
        # 解析 DROP INDEX 语句，支持 IF EXISTS
        self._expect(TokenType.INDEX)
        # 在 DROP INDEX 之后，在表名之前，解析 IF EXISTS
        if_exists, _ = self._parse_if_exists_flags(support_not=False)
        index_name = self._expect(TokenType.IDENTIFIER).value
        return DropIndexStatement(index_name, if_exists=if_exists)


    def _parse_create_view(self):
        # 解析 CREATE VIEW 语句，支持 IF NOT EXISTS
        self._expect(TokenType.CREATE)
        self._expect(TokenType.VIEW)

        _, if_not_exists = self._parse_if_exists_flags(support_not=True)

        view_name = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.AS)

        # 从当前位置开始，收集tokens直到分号或EOF
        start_pos = self.position
        while self.current_token and self.current_token.type != TokenType.SEMICOLON and self.current_token.type != TokenType.EOF:
            self._advance()

        end_pos = self.position
        definition_tokens = self.tokens[start_pos:end_pos]

        parts = []
        for token in definition_tokens:
            if token.type == TokenType.STRING:
                parts.append("'" + token.value.replace("'", "''") + "'")
            else:
                parts.append(token.value)

        view_definition = " ".join(parts).strip()
        if not view_definition.endswith(';'):
            view_definition += ';'
        return CreateViewStatement(view_name, view_definition, if_not_exists=if_not_exists)

    def _parse_drop_view(self):
        # 解析 DROP VIEW 语句，支持 IF EXISTS
        self._expect(TokenType.DROP)
        self._expect(TokenType.VIEW)
        # 在 DROP VIEW 之后，在表名之前，解析 IF EXISTS
        if_exists, _ = self._parse_if_exists_flags(support_not=False)
        view_name = self._expect(TokenType.IDENTIFIER).value
        return DropViewStatement(view_name, if_exists=if_exists)


    def _parse_create_user(self):
        # 解析 CREATE USER 语句，支持 IF NOT EXISTS
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
        # 解析 DROP USER 语句，支持 IF EXISTS
        self._expect(TokenType.DROP)
        self._expect(TokenType.USER)
        # 在 DROP USER 之后，在表名之前，解析 IF EXISTS
        if_exists, _ = self._parse_if_exists_flags(support_not=False)
        username = self._expect(TokenType.IDENTIFIER).value
        return DropUserStatement(username, if_exists=if_exists)

    # 在 _parse_create_statement/_parse_drop_statement 里分发到上述方法
    def _parse_create_statement(self):
        # CREATE分发：TABLE/INDEX
        start_line, start_column = self.current_token.line, self.current_token.column
        self._expect(TokenType.CREATE)
        if self.current_token.type == TokenType.UNIQUE:
            self._advance()
            return self._parse_create_index(is_unique=True)
        elif self.current_token.type == TokenType.INDEX:
            return self._parse_create_index(is_unique=False)
        elif self.current_token.type == TokenType.TABLE:
            return self._parse_create_table(start_line, start_column)
        else:
            raise SyntaxError(f"期望TABLE或INDEX，但得到{self.current_token.value}")

    def _parse_drop_statement(self):
        # DROP分发：TABLE/INDEX
        start_line, start_column = self.current_token.line, self.current_token.column
        self._expect(TokenType.DROP)
        if self.current_token.type == TokenType.INDEX:
            return self._parse_drop_index()
        elif self.current_token.type == TokenType.TABLE:
            return self._parse_drop_table(start_line, start_column)
        else:
            raise SyntaxError(f"DROP语句支持INDEX或TABLE，但得到 {self.current_token.value}")

    def _parse_grant(self) -> GrantStatement:
        """解析 GRANT 语句
        支持ALL/SELECT/INSERT等权限关键字。
        """
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
        """解析 REVOKE 语句
        支持ALL/SELECT/INSERT等权限关键字。
        """
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
        # 解析 BEGIN [TRANSACTION] 事务开始
        self._expect(TokenType.BEGIN)
        if self.current_token and self.current_token.type == TokenType.TRANSACTION:
            self._advance()
        return BeginTransaction()

    def _parse_start_transaction(self) -> Statement:
        # 解析 START TRANSACTION 事务开始
        self._expect(TokenType.START)
        self._expect(TokenType.TRANSACTION)
        return BeginTransaction()

    def _parse_commit(self) -> Statement:
        # 解析 COMMIT 事务提交
        self._expect(TokenType.COMMIT)
        return CommitTransaction()

    def _parse_rollback(self) -> Statement:
        # 解析 ROLLBACK 事务回滚
        self._expect(TokenType.ROLLBACK)
        return RollbackTransaction()

    def _parse_set(self) -> Statement:
        """解析 SET 语句
        支持 SET AUTOCOMMIT=0|1 和 SET SESSION TRANSACTION ISOLATION LEVEL ...
        """
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
                # AUTOCOMMIT分支：只允许0或1，其他值报错
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
                else:
                    # READ分支：未知的READ隔离级别
                    raise SyntaxError("未知的隔离级别: READ ...")
            elif self.current_token.type == TokenType.REPEATABLE:
                self._advance()
                self._expect(TokenType.READ)
                return SetIsolationLevel("REPEATABLE READ")
            elif self.current_token.type == TokenType.SERIALIZABLE:
                self._advance()
                return SetIsolationLevel("SERIALIZABLE")
            else:
                # SESSION分支：未知的隔离级别
                raise SyntaxError("未知的隔离级别")
        else:
            # SET分支：仅支持AUTOCOMMIT和SESSION
            raise SyntaxError("仅支持: SET AUTOCOMMIT 或 SET SESSION TRANSACTION ISOLATION LEVEL ...")

    def _parse_create_trigger(self) -> CreateTriggerStatement:
        """解析 CREATE TRIGGER 语句
        支持 BEFORE/AFTER, INSERT/UPDATE/DELETE, FOR EACH ROW, 触发器体。
        """
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
            # 对于字符串类型的token，需要重新添加引号并转义内部引号
            if self.current_token.type == TokenType.STRING:
                # 转义字符串中的单引号
                escaped_value = self.current_token.value.replace("'", "''")
                statement_tokens.append(f"'{escaped_value}'")
            else:
                statement_tokens.append(self.current_token.value)
            self._advance()

        if not statement_tokens:
            raise SyntaxError("触发器体不能为空")

        statement = " ".join(statement_tokens)
        if not statement.endswith(';'):
            statement += ';'
        return CreateTriggerStatement(trigger_name, timing, event, table_name, statement)

    def _parse_drop_trigger(self) -> DropTriggerStatement:
        # 解析 DROP TRIGGER 语句，支持 IF EXISTS
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
        # 解析 ALTER TABLE 语句，支持 ADD COLUMN/DROP COLUMN
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

    def _parse_show(self) -> ShowStatement:
        # 解析 SHOW AUTOCOMMIT/SHOW ISOLATION LEVEL
        self._expect(TokenType.SHOW)

        if self.current_token.type == TokenType.AUTOCOMMIT:
            self._advance()
            return ShowStatement("AUTOCOMMIT")
        elif self.current_token.type == TokenType.ISOLATION:
            self._advance()
            self._expect(TokenType.LEVEL)
            return ShowStatement("ISOLATION_LEVEL")
        else:
            raise SyntaxError("仅支持: SHOW AUTOCOMMIT 或 SHOW ISOLATION LEVEL")

    def _parse_open_cursor(self):
        self._expect(TokenType.OPEN)
        self._expect(TokenType.CURSOR)
        cursor_name = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.FOR)
        # 允许FOR后嵌套SELECT语句
        select_stmt = self._parse_select()
        return OpenCursorStatement(cursor_name, select_stmt)

    def _parse_fetch_cursor(self):
        self._expect(TokenType.FETCH)
        count = int(self._expect(TokenType.NUMBER).value)
        self._expect(TokenType.FROM)
        cursor_name = self._expect(TokenType.IDENTIFIER).value
        return FetchCursorStatement(count, cursor_name)

    def _parse_close_cursor(self):
        self._expect(TokenType.CLOSE)
        self._expect(TokenType.CURSOR)
        cursor_name = self._expect(TokenType.IDENTIFIER).value
        return CloseCursorStatement(cursor_name)
