"""
诊断与纠错引擎：在语义分析失败后，尝试进行轻量自动纠错或给出建议
"""

from typing import List, Tuple, Optional, Dict, Union
from catalog import SystemCatalog
from .ast_nodes import (
    Statement,
    SelectStatement,
    InsertStatement,
    UpdateStatement,
    DeleteStatement,
    JoinClause,
    ColumnRef,
    OrderItem,
)


def _levenshtein(a: str, b: str) -> int:
    a, b = a or "", b or ""
    la, lb = len(a), len(b)
    if la == 0:
        return lb
    if lb == 0:
        return la
    dp = list(range(lb + 1))
    for i in range(1, la + 1):
        prev = dp[0]
        dp[0] = i
        for j in range(1, lb + 1):
            cur = dp[j]
            cost = 0 if a[i - 1].lower() == b[j - 1].lower() else 1
            dp[j] = min(dp[j] + 1, dp[j - 1] + 1, prev + cost)
            prev = cur
    return dp[-1]


def _closest(name: str, candidates: List[str]) -> Optional[str]:
    if not candidates:
        return None
    scored = [(c, _levenshtein(name, c)) for c in candidates]
    scored.sort(key=lambda x: x[1])
    best, dist = scored[0]
    # 经验阈值：长度<=4 允许距离1；<=8 允许2；否则3
    limit = 1 if len(name) <= 4 else (2 if len(name) <= 8 else 3)
    return best if dist <= limit else None


class CorrectionResult:
    def __init__(self, ast: Optional[Statement], hints: List[str], changed: bool):
        self.ast = ast
        self.hints = hints
        self.changed = changed


class DiagnosticEngine:
    def __init__(self, catalog: SystemCatalog, auto_correct: bool = True):
        self.catalog = catalog
        self.auto = auto_correct

    def try_correct(self, stmt: Statement, error_message: str) -> CorrectionResult:
        hints: List[str] = []
        changed = False

        # 在 try_correct 函数开头，统一用 getattr(stmt, 'if_exists', False) 和 getattr(stmt, 'if_not_exists', False) 判断，
        # 只要 error_message 包含“已存在”且 if_not_exists=True，或包含“不存在”且 if_exists=True，都直接返回 CorrectionResult(None, [友好提示], True)。
        if hasattr(stmt, 'if_exists') and stmt.if_exists and '不存在' in error_message:
            return CorrectionResult(None, [f"已忽略：{error_message} (因 IF EXISTS)"], True)
        if hasattr(stmt, 'if_not_exists') and stmt.if_not_exists and '已存在' in error_message:
            return CorrectionResult(None, [f"已忽略：{error_message} (因 IF NOT EXISTS)"], True)

        # 表名纠错：CREATE/INSERT/UPDATE/DELETE/SELECT FROM / JOIN
        def correct_table_name(name: str) -> Optional[str]:
            names = self.catalog.list_tables() or []
            # 大小写匹配
            for n in names:
                if n.lower() == name.lower():
                    return n
            # 近似匹配
            return _closest(name, names)

        def rewrite_from(node: Union[str, JoinClause]) -> Union[str, JoinClause]:
            nonlocal changed
            if isinstance(node, str):
                fixed = correct_table_name(node)
                if fixed and fixed != node:
                    changed = True
                    return fixed
                return node
            elif isinstance(node, JoinClause):
                node.left = rewrite_from(node.left)
                # right 是表名字符串
                if isinstance(node.right, str):
                    fixed = correct_table_name(node.right)
                    if fixed and fixed != node.right:
                        changed = True
                        node.right = fixed
                return node
            return node

        def correct_column_name(table: Optional[str], name: str) -> Optional[str]:
            # 若给定表，精确在该表内纠正；否则仅在单表场景尝试
            if table:
                schema = self.catalog.get_table_schema(table)
                if not schema:
                    return None
                cols = [c.name for c in schema.columns]
                # 大小写
                for c in cols:
                    if c.lower() == name.lower():
                        return c
                return _closest(name, cols)
            return None

        # 仅在 SELECT 的单表场景，对裸列尝试纠错；多表容易引入歧义，仅给建议
        def select_single_table(from_table: Union[str, JoinClause]) -> Optional[str]:
            if isinstance(from_table, str):
                return from_table
            return None

        if isinstance(stmt, SelectStatement):
            # 先纠正 FROM/JOINS 表名
            stmt.from_table = rewrite_from(stmt.from_table)

            single_table = select_single_table(stmt.from_table)
            if single_table:
                # 列表、WHERE、GROUP BY、ORDER BY 的列名纠错（大小写/近似）
                def fix_colref(col: ColumnRef) -> ColumnRef:
                    nonlocal changed
                    # 若有表前缀，按表纠正；无前缀则按单表纠正
                    table = col.table_name or single_table
                    fixed = correct_column_name(table, col.column_name)
                    if fixed and fixed != col.column_name:
                        changed = True
                        return ColumnRef(fixed, col.table_name)
                    return col

                for i, c in enumerate(stmt.columns):
                    if isinstance(c, ColumnRef):
                        stmt.columns[i] = fix_colref(c)

                if stmt.where_clause:
                    def walk(expr):
                        if isinstance(expr, ColumnRef):
                            return fix_colref(expr)
                        if hasattr(expr, 'left') and hasattr(expr, 'right'):
                            expr.left = walk(expr.left)
                            expr.right = walk(expr.right)
                            return expr
                        if hasattr(expr, 'arg') and isinstance(expr.arg, ColumnRef):
                            expr.arg = fix_colref(expr.arg)
                            return expr
                        return expr
                    stmt.where_clause = walk(stmt.where_clause)

                if getattr(stmt, 'group_by', None):
                    for i, g in enumerate(stmt.group_by):
                        if isinstance(g, ColumnRef):
                            stmt.group_by[i] = fix_colref(g)

                if getattr(stmt, 'order_by', None):
                    for i, o in enumerate(stmt.order_by):
                        if isinstance(o.expr, ColumnRef):
                            stmt.order_by[i].expr = fix_colref(o.expr)

                # 纠错：ORDER BY 列不在选择列表中 -> 自动添加到投影（可选）
                if self.auto and getattr(stmt, 'order_by', None):
                    projected_names = set()
                    for c in stmt.columns:
                        if isinstance(c, ColumnRef):
                            projected_names.add(c.column_name)
                    to_add: List[ColumnRef] = []
                    for o in stmt.order_by:
                        if isinstance(o.expr, ColumnRef):
                            if o.expr.column_name not in projected_names:
                                to_add.append(ColumnRef(o.expr.column_name, None))
                    if to_add:
                        hints.append("为满足 ORDER BY 规则，已将排序列补充到选择列表中")
                        stmt.columns.extend(to_add)
                        changed = True

        elif isinstance(stmt, (InsertStatement, UpdateStatement, DeleteStatement)):
            # 纠正 DML 的表名
            original = None
            if isinstance(stmt, InsertStatement):
                original = stmt.table_name
            elif isinstance(stmt, UpdateStatement):
                original = stmt.table_name
            elif isinstance(stmt, DeleteStatement):
                original = stmt.table_name
            fixed = correct_table_name(original)
            if fixed and fixed != original:
                hints.append(f"表名 '{original}' 修正为 '{fixed}'")
                stmt.table_name = fixed
                changed = True

        return CorrectionResult(stmt if changed else None, hints, changed) 