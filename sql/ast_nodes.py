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
        if_exists: bool = False,
        if_not_exists: bool = False,
    ):
        self.table_name = table_name
        self.columns = columns
        self.table_constraints = table_constraints or []
        self.if_exists = if_exists
        self.if_not_exists = if_not_exists

    def __repr__(self):
        flag = " IF NOT EXISTS" if self.if_not_exists else (" IF EXISTS" if self.if_exists else "")
        return f"CREATE TABLE{flag} {self.table_name} ({self.columns}, {self.table_constraints})"


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
    def __init__(self, table_name: str, if_exists: bool = False, if_not_exists: bool = False):
        self.table_name = table_name
        self.if_exists = if_exists
        self.if_not_exists = if_not_exists

    def __repr__(self):
        flag = " IF EXISTS" if self.if_exists else (" IF NOT EXISTS" if self.if_not_exists else "")
        return f"DROP TABLE{flag} {self.table_name}"


class TruncateTableStatement(Statement):
    """TRUNCATE TABLE 语句"""

    def __init__(self, table_name: str):
        self.table_name = table_name

    def __repr__(self):
        return f"TRUNCATE TABLE {self.table_name}"


# 索引相关语句
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
        if_not_exists: bool = False,
    ):
        self.index_name = index_name
        self.table_name = table_name
        self.column_name = column_name
        self.is_unique = is_unique
        self.if_not_exists = if_not_exists


class DropIndexStatement(Statement):
    """DROP INDEX语句"""
    def __init__(self, index_name: str, if_exists: bool = False):
        self.index_name = index_name
        self.if_exists = if_exists


# JOIN相关
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


# 视图相关语句
class CreateViewStatement(Statement):
    def __init__(self, view_name: str, view_definition: str, if_not_exists: bool = False):
        self.view_name = view_name
        self.view_definition = view_definition
        self.if_not_exists = if_not_exists
        # print(f"[AST DEBUG] CreateViewStatement created: view_name={view_name}, definition=\"{view_definition}\"")


class DropViewStatement(Statement):
    def __init__(self, view_name: str, if_exists: bool = False):
        self.view_name = view_name
        self.if_exists = if_exists
        # print(f"[AST DEBUG] DropViewStatement created: view_name={view_name}")


# 用户管理语句
class CreateUserStatement(Statement):
    """CREATE USER 语句"""
    def __init__(self, username: str, password: str, if_not_exists: bool = False):
        self.username = username
        self.password = password
        self.if_not_exists = if_not_exists

    def __repr__(self):
        return f"CREATE USER {self.username} IDENTIFIED BY '***'"


class DropUserStatement(Statement):
    """DROP USER 语句"""
    def __init__(self, username: str, if_exists: bool = False):
        self.username = username
        self.if_exists = if_exists

    def __repr__(self):
        return f"DROP USER {self.username}"


# 权限管理语句
class GrantStatement(Statement):
    """GRANT 语句"""

    def __init__(self, privilege: str, table_name: str, username: str):
        self.privilege = privilege  # SELECT, INSERT, UPDATE, DELETE, ALL
        self.table_name = table_name
        self.username = username

    def __repr__(self):
        return f"GRANT {self.privilege} ON {self.table_name} TO {self.username}"


class RevokeStatement(Statement):
    """REVOKE 语句"""

    def __init__(self, privilege: str, table_name: str, username: str):
        self.privilege = privilege
        self.table_name = table_name
        self.username = username

    def __repr__(self):
        return f"REVOKE {self.privilege} ON {self.table_name} FROM {self.username}"


# 事务相关语句
class BeginTransaction(Statement):
    """BEGIN 或 START TRANSACTION"""

    def __repr__(self):
        return "BEGIN"


class CommitTransaction(Statement):
    """COMMIT"""

    def __repr__(self):
        return "COMMIT"


class RollbackTransaction(Statement):
    """ROLLBACK（当前不实现实际回滚）"""

    def __repr__(self):
        return "ROLLBACK"


class SetAutocommit(Statement):
    """SET AUTOCOMMIT = 0|1"""

    def __init__(self, enabled: bool):
        self.enabled = enabled

    def __repr__(self):
        return f"SET AUTOCOMMIT = {1 if self.enabled else 0}"


class SetIsolationLevel(Statement):
    """SET SESSION TRANSACTION ISOLATION LEVEL ..."""

    def __init__(self, level: str):
        # level ∈ {READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE}
        self.level = level

    def __repr__(self):
        return f"SET SESSION TRANSACTION ISOLATION LEVEL {self.level}"


# 触发器相关语句
class CreateTriggerStatement(Statement):
    """CREATE TRIGGER 语句"""

    def __init__(
        self,
        trigger_name: str,
        timing: str,  # BEFORE 或 AFTER
        event: str,   # INSERT, UPDATE, DELETE
        table_name: str,
        statement: str  # 触发器体（SQL语句）
    ):
        self.trigger_name = trigger_name
        self.timing = timing.upper()
        self.event = event.upper()
        self.table_name = table_name
        self.statement = statement

    def __repr__(self):
        return f"CREATE TRIGGER {self.trigger_name} {self.timing} {self.event} ON {self.table_name} FOR EACH ROW {self.statement}"


class DropTriggerStatement(Statement):
    """DROP TRIGGER 语句"""

    def __init__(self, trigger_name: str, if_exists: bool = False):
        self.trigger_name = trigger_name
        self.if_exists = if_exists

    def __repr__(self):
        flag = " IF EXISTS" if self.if_exists else ""
        return f"DROP TRIGGER{flag} {self.trigger_name}"


# ALTER TABLE 相关
class AlterTableStatement(Statement):
    """ALTER TABLE 语句"""
    def __init__(self, table_name: str, action: str, column_def: dict = None, column_name: str = None):
        self.table_name = table_name
        self.action = action  # 'ADD' 或 'DROP'
        self.column_def = column_def  # dict, 仅ADD时有
        self.column_name = column_name  # str, 仅DROP时有
    def __repr__(self):
        if self.action == 'ADD':
            return f"ALTER TABLE {self.table_name} ADD COLUMN {self.column_def}"
        elif self.action == 'DROP':
            return f"ALTER TABLE {self.table_name} DROP COLUMN {self.column_name}"
        else:
            return f"ALTER TABLE {self.table_name} <UNKNOWN ACTION>"


class ShowStatement(Statement):
    """SHOW语句"""

    def __init__(self, show_type: str):
        self.show_type = show_type  # 'AUTOCOMMIT', 'ISOLATION_LEVEL', etc.

    def __repr__(self):
        return f"SHOW {self.show_type}"