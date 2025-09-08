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

    def __repr__(self):
        if self.table_name:
            return f"{self.table_name}.{self.column_name}"
        return self.column_name


class Literal(Expression):
    """字面量"""

    def __init__(self, value: Any, data_type: str):
        self.value = value
        self.data_type = data_type  # 'INTEGER', 'STRING', 'FLOAT', 'BOOLEAN', 'NULL'

    def __repr__(self):
        return f"{self.value}({self.data_type})"


class BinaryOp(Expression):
    """二元操作符"""

    def __init__(self, left: Expression, operator: str, right: Expression):
        self.left = left
        self.operator = operator
        self.right = right

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


# 语句节点
class CreateTableStatement(Statement):
    """CREATE TABLE 语句"""

    def __init__(self, table_name: str, columns: List[Dict[str, Any]]):
        self.table_name = table_name
        self.columns = (
            columns  # [{'name': str, 'type': str, 'length': int, 'constraints': [str]}]
        )

    def __repr__(self):
        return f"CREATE TABLE {self.table_name} ({self.columns})"


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


class SelectStatement(Statement):
    """SELECT 语句"""

    def __init__(
        self,
        columns: List[Union[ColumnRef, str]],
        from_table: str,
        where_clause: Optional[Expression] = None,
    ):
        self.columns = columns  # 选择的列，'*' 表示所有列
        self.from_table = from_table  # 表名
        self.where_clause = where_clause  # WHERE条件

    def __repr__(self):
        cols = ", ".join(str(col) for col in self.columns)
        result = f"SELECT {cols} FROM {self.from_table}"
        if self.where_clause:
            result += f" WHERE {self.where_clause}"
        return result
