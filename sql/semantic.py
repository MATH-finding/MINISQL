"""
语义分析器：在解析与执行之间进行名称解析、分组/聚合规则校验、列消解与 * 展开
"""

from typing import Dict, List, Tuple, Optional, Union
from catalog import SystemCatalog
from .ast_nodes import (
    Statement,
    SelectStatement,
    InsertStatement,
    UpdateStatement,
    DeleteStatement,
    JoinClause,
    ColumnRef,
    AggregateFunction,
    OrderItem,
)


class SemanticError(Exception):
    pass


class AnalyzedResult:
    def __init__(self, ast: Statement, output_schema: Optional[List[Dict]] = None, metadata: Optional[Dict] = None):
        self.ast = ast
        self.output_schema = output_schema or []
        self.metadata = metadata or {}


class SemanticAnalyzer:
    """语义分析器：不处理注释，注释已在词法分析阶段去除。仅英文分号;被识别为语句结束符，中文分号；不被识别。"""
    def __init__(self, catalog: SystemCatalog):
        self.catalog = catalog

    def analyze(self, stmt: Statement) -> AnalyzedResult:
        if isinstance(stmt, SelectStatement):
            return self._analyze_select(stmt)
        elif isinstance(stmt, InsertStatement):
            return self._analyze_insert(stmt)
        elif isinstance(stmt, UpdateStatement):
            return self._analyze_update(stmt)
        elif isinstance(stmt, DeleteStatement):
            return self._analyze_delete(stmt)
        else:
            # 其他语句暂不做语义分析
            return AnalyzedResult(stmt)

    # =============== SELECT ===============
    def _collect_visible_columns(self, from_table: Union[str, JoinClause]) -> Tuple[Dict[str, List[Tuple[str, str]]], Dict[str, Dict]]:
        """
        返回:
        - visible: 裸列名 -> 列表[(table_name, column_name), ...] 用于歧义检测
        - table_schemas: table_name -> { column_name -> column_info(dict) }
        """
        visible: Dict[str, List[Tuple[str, str]]] = {}
        table_schemas: Dict[str, Dict[str, Dict]] = {}

        def add_table(table_name: str):
            # 允许视图：由执行器在执行阶段展开，这里不抛错
            if hasattr(self.catalog, 'views') and table_name in getattr(self.catalog, 'views', {}):
                table_schemas[table_name] = {}
                return
            schema = self.catalog.get_table_schema(table_name)
            if not schema:
                raise SemanticError(f"表 {table_name} 不存在")
            col_map: Dict[str, Dict] = {}
            for col in schema.columns:
                col_map[col.name] = {"name": col.name, "type": getattr(col.data_type, "name", str(col.data_type))}
                visible.setdefault(col.name, []).append((table_name, col.name))
            table_schemas[table_name] = col_map

        def dfs(node: Union[str, JoinClause]):
            if isinstance(node, str):
                add_table(node)
            elif isinstance(node, JoinClause):
                dfs(node.left)
                dfs(node.right)
            else:
                raise SemanticError("未知的from_table类型")

        dfs(from_table)
        return visible, table_schemas

    def _resolve_column(self, col: ColumnRef, visible: Dict[str, List[Tuple[str, str]]]) -> ColumnRef:
        if col.table_name:
            return col
        name = col.column_name
        candidates = visible.get(name, [])
        if len(candidates) == 0:
            raise SemanticError(f"列 {name} 不存在")
        if len(candidates) > 1:
            raise SemanticError(f"列 {name} 不明确，请加表前缀")
        table_name, column_name = candidates[0]
        return ColumnRef(column_name, table_name)

    def _expand_star(self, from_table: Union[str, JoinClause], table_schemas: Dict[str, Dict[str, Dict]]) -> List[ColumnRef]:
        result: List[ColumnRef] = []
        def dfs(node: Union[str, JoinClause]):
            if isinstance(node, str):
                # 对视图此处无法展开，由执行器负责
                if node in table_schemas and table_schemas[node]:
                    for col_name in table_schemas[node].keys():
                        result.append(ColumnRef(col_name, node))
            else:
                dfs(node.left)
                dfs(node.right)
        dfs(from_table)
        return result

    def _select_output_schema(self, columns: List[Union[ColumnRef, str, AggregateFunction]]) -> List[Dict]:
        out = []
        for c in columns:
            if isinstance(c, ColumnRef):
                out.append({"name": c.column_name, "type": None, "is_aggregated": False})
            elif isinstance(c, AggregateFunction):
                name = c.func_name.upper()
                # 简化类型推断
                agg_type = "INTEGER" if name == "COUNT" else "FLOAT"
                out.append({"name": name, "type": agg_type, "is_aggregated": True})
            elif isinstance(c, str):
                # 不应出现裸字符串列名（* 已展开），保底
                out.append({"name": c, "type": None, "is_aggregated": False})
        return out

    def _analyze_select(self, stmt: SelectStatement) -> AnalyzedResult:
        # 收集可见列
        visible, table_schemas = self._collect_visible_columns(stmt.from_table)

        # 列消解：展开 *，补齐 ColumnRef.table_name
        resolved_columns: List[Union[ColumnRef, AggregateFunction]] = []
        for col in stmt.columns:
            if isinstance(col, str) and col == "*":
                resolved_columns.extend(self._expand_star(stmt.from_table, table_schemas))
            elif isinstance(col, ColumnRef):
                resolved_columns.append(self._resolve_column(col, visible))
            else:
                # AggregateFunction 或其他
                resolved_columns.append(col)

        # WHERE 中的列消解
        def resolve_expr(expr):
            if isinstance(expr, ColumnRef):
                return self._resolve_column(expr, visible)
            elif hasattr(expr, 'left') and hasattr(expr, 'right'):
                expr.left = resolve_expr(expr.left)
                expr.right = resolve_expr(expr.right)
                return expr
            elif hasattr(expr, 'arg'):
                # 聚合参数
                if isinstance(expr.arg, ColumnRef):
                    expr.arg = self._resolve_column(expr.arg, visible)
                return expr
            else:
                return expr
        if stmt.where_clause:
            stmt.where_clause = resolve_expr(stmt.where_clause)

        # GROUP BY 列消解
        resolved_group_by: List[ColumnRef] = []
        for g in getattr(stmt, 'group_by', []) or []:
            if isinstance(g, ColumnRef):
                resolved_group_by.append(self._resolve_column(g, visible))
            else:
                raise SemanticError("GROUP BY 只支持列名")

        # 聚合与 GROUP BY 规则校验
        has_agg = any(isinstance(c, AggregateFunction) for c in resolved_columns)
        group_present = len(resolved_group_by) > 0
        if has_agg or group_present:
            group_names = {g.column_name for g in resolved_group_by}
            for c in resolved_columns:
                if isinstance(c, ColumnRef) and c.column_name not in group_names:
                    raise SemanticError("GROUP BY 查询中，非聚合列必须包含在分组键中")

        # ORDER BY 消解到输出列名（优先）或可见列
        resolved_order_by: List[OrderItem] = []
        output_names_after = []
        for c in resolved_columns:
            if isinstance(c, ColumnRef):
                output_names_after.append(c.column_name)
            elif isinstance(c, AggregateFunction):
                output_names_after.append(c.func_name.upper())
        for item in getattr(stmt, 'order_by', []) or []:
            expr = item.expr
            if isinstance(expr, ColumnRef):
                resolved = self._resolve_column(expr, visible)
                key_name = resolved.column_name
                if key_name not in output_names_after:
                    raise SemanticError(f"ORDER BY 列 {key_name} 必须出现在选择列表中")
                resolved_order_by.append(OrderItem(ColumnRef(key_name), item.direction))
            else:
                raise SemanticError("ORDER BY 只支持列名")

        # 写回 AST
        stmt.columns = resolved_columns
        stmt.group_by = resolved_group_by
        stmt.order_by = resolved_order_by

        output_schema = self._select_output_schema(resolved_columns)
        metadata = {
            "has_agg": has_agg,
            "group_keys": [g.column_name for g in resolved_group_by],
            "order_keys": [(i.expr.column_name if isinstance(i.expr, ColumnRef) else str(i.expr), i.direction) for i in resolved_order_by],
        }
        return AnalyzedResult(stmt, output_schema, metadata)

    # =============== INSERT / UPDATE / DELETE（轻量校验） ===============
    def _analyze_insert(self, stmt: InsertStatement) -> AnalyzedResult:
        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise SemanticError(f"表 {stmt.table_name} 不存在")
        # 列存在性
        if stmt.columns:
            table_cols = {c.name for c in schema.columns}
            for name in stmt.columns:
                if name not in table_cols:
                    raise SemanticError(f"列 {name} 不存在于表 {stmt.table_name}")
        return AnalyzedResult(stmt)

    def _analyze_update(self, stmt: UpdateStatement) -> AnalyzedResult:
        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise SemanticError(f"表 {stmt.table_name} 不存在")
        table_cols = {c.name for c in schema.columns}
        for s in stmt.set_clauses:
            if s["column"] not in table_cols:
                raise SemanticError(f"列 {s['column']} 不存在于表 {stmt.table_name}")
        return AnalyzedResult(stmt)

    def _analyze_delete(self, stmt: DeleteStatement) -> AnalyzedResult:
        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise SemanticError(f"表 {stmt.table_name} 不存在")
        return AnalyzedResult(stmt) 