"""
抽象语法树节点定义
"""

from abc import ABC
from typing import Any, List, Dict, Optional, Union


class ASTNode(ABC):
    """抽象语法树节点基类"""

    pass


class Expression(ASTNode):
    """表达式基类"""

    pass


class Statement(ASTNode):
    """语句基类"""

    pass


# 表达式节点
class ColumnRef(Expression):
    """列引用"""

    def __init__(self, column_name: str, table_name: str = None):
        self.column_name = column_name
        self.table_name = table_name
        # print(f"[AST DEBUG] ColumnRef created: column_name={column_name}, table_name={table_name}")

    def __repr__(self):
        if self.table_name:
            return f"{self.table_name}.{self.column_name}"
        return self.column_name


class Literal(Expression):
    """字面量"""

    def __init__(self, value: Any, data_type: str):
        self.value = value
        self.data_type = data_type  # 'INTEGER', 'STRING', 'FLOAT', 'BOOLEAN', 'NULL'
        # # print(f"[AST DEBUG] Literal created: value={value}, data_type={data_type}")

    def __repr__(self):
        return f"{self.value}({self.data_type})"


class BinaryOp(Expression):
    """二元操作符"""

    def __init__(self, left: Expression, operator: str, right: Expression):
        self.left = left
        self.operator = operator
        self.right = right
        # # print(f"[AST DEBUG] BinaryOp created: left={left}, operator={operator}, right={right}")

    def __repr__(self):
        return f"({self.left} {self.operator} {self.right})"


class LogicalOp(Expression):
    """逻辑操作符 (AND, OR)"""

    def __init__(self, left: Expression, operator: str, right: Expression):
        self.left = left
        self.operator = operator  # 'AND', 'OR'
        self.right = right

    def __repr__(self):
        return f"({self.left} {self.operator} {self.right})"


class AggregateFunction(Expression):
    """聚合函数表达式，如COUNT(col), SUM(col)等"""

    def __init__(self, func_name: str, arg: Any):
        self.func_name = func_name.upper()
        self.arg = arg  # 可以是ColumnRef、'*'等

    def __repr__(self):
        return f"{self.func_name}({self.arg})"


# 语句节点
class CreateTableStatement(Statement):
    """CREATE TABLE 语句"""

    def __init__(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        table_constraints: List[dict] = None,
    ):
        self.table_name = table_name
        self.columns = (
            columns  # [{'name': str, 'type': str, 'length': int, 'constraints': [str]}]
        )
        self.table_constraints = table_constraints or []

    def __repr__(self):
        return (
            f"CREATE TABLE {self.table_name} ({self.columns}, {self.table_constraints})"
        )


class InsertStatement(Statement):
    """INSERT 语句"""

    def __init__(
        self, table_name: str, columns: List[str], values: List[List[Expression]]
    ):
        self.table_name = table_name
        self.columns = columns  # 可选列名列表
        self.values = values  # 值列表的列表（支持多行插入）

    def __repr__(self):
        return f"INSERT INTO {self.table_name} ({self.columns}) VALUES {self.values}"


class OrderItem(ASTNode):
    def __init__(self, expr: Union[ColumnRef, str], direction: str = "ASC"):
        self.expr = expr
        self.direction = direction.upper()

    def __repr__(self):
        return f"{self.expr} {self.direction}"


class SelectStatement(Statement):
    """SELECT 语句"""

    def __init__(
        self,
        columns: List[Union[ColumnRef, str, AggregateFunction]],
        from_table: Union[str, "JoinClause"],  # 修改为支持JoinClause
        where_clause: Optional[Expression] = None,
        group_by: Optional[List[Union[ColumnRef, str]]] = None,
        order_by: Optional[List[OrderItem]] = None,
    ):
        self.columns = columns  # 选择的列，'*' 表示所有列
        self.from_table = from_table  # 表名或JoinClause
        self.where_clause = where_clause  # WHERE条件
        self.group_by = group_by or []
        self.order_by = order_by or []
        # print(f"[AST DEBUG] SelectStatement created: columns={columns}, from_table={from_table}, where_clause={where_clause}")

    def __repr__(self):
        cols = ", ".join(str(col) for col in self.columns)
        result = f"SELECT {cols} FROM {self.from_table}"
        if self.where_clause:
            result += f" WHERE {self.where_clause}"
        if self.group_by:
            result += " GROUP BY " + ", ".join(str(g) for g in self.group_by)
        if self.order_by:
            result += " ORDER BY " + ", ".join(str(o) for o in self.order_by)
        return result


# sql/ast_nodes.py 中添加
class CreateIndexNode(ASTNode):
    """创建索引AST节点"""

    def __init__(
        self,
        index_name: str,
        table_name: str,
        column_name: str,
        is_unique: bool = False,
    ):
        self.index_name = index_name
        self.table_name = table_name
        self.column_name = column_name
        self.is_unique = is_unique


class DropIndexNode(ASTNode):
    """删除索引AST节点"""

    def __init__(self, index_name: str):
        self.index_name = index_name


class CreateIndexStatement(Statement):
    """CREATE INDEX语句"""

    def __init__(
        self,
        index_name: str,
        table_name: str,
        column_name: str,
        is_unique: bool = False,
    ):
        self.index_name = index_name
        self.table_name = table_name
        self.column_name = column_name
        self.is_unique = is_unique


class DropIndexStatement(Statement):
    """DROP INDEX语句"""

    def __init__(self, index_name: str):
        self.index_name = index_name


class JoinClause(ASTNode):
    """
    JOIN子句AST节点
    left: 左表（表名str或JoinClause）
    right: 右表（表名str）
    join_type: 连接类型（如'INNER', 'LEFT'等）
    on: 连接条件（Expression）
    """

    def __init__(
        self, left: Union[str, "JoinClause"], right: str, join_type: str, on: Expression
    ):
        self.left = left
        self.right = right
        self.join_type = join_type  # 'INNER', 'LEFT', ...
        self.on = on

    def __repr__(self):
        return f"({self.left} {self.join_type} JOIN {self.right} ON {self.on})"


class UpdateStatement(Statement):
    """UPDATE 语句"""

    def __init__(
        self,
        table_name: str,
        set_clauses: List[Dict[str, Expression]],
        where_clause: Optional[Expression] = None,
    ):
        self.table_name = table_name
        self.set_clauses = set_clauses  # [{'column': str, 'value': Expression}]
        self.where_clause = where_clause

    def __repr__(self):
        set_parts = [
            f"{clause['column']} = {clause['value']}" for clause in self.set_clauses
        ]
        result = f"UPDATE {self.table_name} SET {', '.join(set_parts)}"
        if self.where_clause:
            result += f" WHERE {self.where_clause}"
        return result


class DeleteStatement(Statement):
    """DELETE 语句"""

    def __init__(self, table_name: str, where_clause: Optional[Expression] = None):
        self.table_name = table_name
        self.where_clause = where_clause

    def __repr__(self):
        result = f"DELETE FROM {self.table_name}"
        if self.where_clause:
            result += f" WHERE {self.where_clause}"
        return result


class DropTableStatement(Statement):
    """DROP TABLE 语句"""

    def __init__(self, table_name: str):
        self.table_name = table_name

    def __repr__(self):
        return f"DROP TABLE {self.table_name}"


class TruncateTableStatement(Statement):
    """TRUNCATE TABLE 语句"""

    def __init__(self, table_name: str):
        self.table_name = table_name

    def __repr__(self):
        return f"TRUNCATE TABLE {self.table_name}"


class CreateViewStatement(Statement):
    def __init__(self, view_name: str, view_definition: str):
        self.view_name = view_name
        self.view_definition = view_definition
        # print(f"[AST DEBUG] CreateViewStatement created: view_name={view_name}, definition=\"{view_definition}\"")

class DropViewStatement(Statement):
    def __init__(self, view_name: str):
        self.view_name = view_name
        # print(f"[AST DEBUG] DropViewStatement created: view_name={view_name}")
