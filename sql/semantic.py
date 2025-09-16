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
    CreateTableStatement,
    DropTableStatement,
    CreateTriggerStatement,
    DropTriggerStatement,
    AlterTableStatement,
)


class SemanticError(Exception):
    def __init__(self, reason, line=None, column=None):
        error_type = "SemanticError"
        position = f"行{line},列{column}" if line is not None and column is not None else "未知位置"
        self.error_list = [error_type, position, reason]
        super().__init__(self.error_list)
    def __str__(self):
        return str(self.error_list)


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
        elif isinstance(stmt, CreateTableStatement):
            return self._analyze_create_table(stmt)
        elif isinstance(stmt, DropTableStatement):
            return self._analyze_drop_table(stmt)
        elif isinstance(stmt, CreateTriggerStatement):
            return self._analyze_create_trigger(stmt)
        elif isinstance(stmt, DropTriggerStatement):
            return self._analyze_drop_trigger(stmt)
        elif isinstance(stmt, AlterTableStatement):
            return self._analyze_alter_table(stmt)
        else:
            # 其他语句暂不做语义分析
            return AnalyzedResult(stmt)

    # =============== SELECT ===============
    def _collect_visible_columns(self, from_table: Union[str, JoinClause]) -> Tuple[
        Dict[str, List[Tuple[str, str]]], Dict[str, Dict]]:
        """
        返回:
        - visible: 裸列名 -> 列表[(table_name, column_name), ...] 用于歧义检测
        - table_schemas: table_name -> { column_name -> column_info(dict) }
        """
        visible: Dict[str, List[Tuple[str, str]]] = {}
        table_schemas: Dict[str, Dict[str, Dict]] = {}

        def add_table(table_name: str):
            # **修复：改进视图支持**
            if hasattr(self.catalog, 'views') and table_name in getattr(self.catalog, 'views', {}):
                try:
                    view_def = self.catalog.get_view_definition(table_name)
                    # 如果视图定义包含基表信息，使用基表的列
                    if hasattr(view_def, 'base_table'):
                        base_schema = self.catalog.get_table_schema(view_def.base_table)
                        if base_schema:
                            col_map: Dict[str, Dict] = {}
                            for col in base_schema.columns:
                                col_map[col.name] = {"name": col.name,
                                                     "type": getattr(col.data_type, "name", str(col.data_type))}
                                visible.setdefault(col.name, []).append((table_name, col.name))
                            table_schemas[table_name] = col_map
                            return
                    elif hasattr(view_def, 'select_stmt'):
                        # 从SELECT语句推断列
                        select_stmt = view_def.select_stmt
                        if hasattr(select_stmt, 'from_table') and isinstance(select_stmt.from_table, str):
                            base_schema = self.catalog.get_table_schema(select_stmt.from_table)
                            if base_schema:
                                col_map: Dict[str, Dict] = {}
                                for col in base_schema.columns:
                                    col_map[col.name] = {"name": col.name,
                                                         "type": getattr(col.data_type, "name", str(col.data_type))}
                                    visible.setdefault(col.name, []).append((table_name, col.name))
                                table_schemas[table_name] = col_map
                                return
                except:
                    pass
                # 如果无法解析视图，设为空但不报错
                table_schemas[table_name] = {}
                return

            schema = self.catalog.get_table_schema(table_name)
            if not schema:
                raise SemanticError(f"表 {table_name} 不存在", None, None)
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
                raise SemanticError("未知的from_table类型", None, None)

        dfs(from_table)
        return visible, table_schemas

    def _resolve_column(self, col: ColumnRef, visible: Dict[str, List[Tuple[str, str]]]) -> ColumnRef:
        if col.table_name:
            return col
        name = col.column_name
        candidates = visible.get(name, [])
        if len(candidates) == 0:
            raise SemanticError(f"列 {name} 不存在", getattr(col, 'line', None), getattr(col, 'column', None))
        if len(candidates) > 1:
            raise SemanticError(f"列 {name} 不明确，请加表前缀", getattr(col, 'line', None), getattr(col, 'column', None))
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
                raise SemanticError("GROUP BY 只支持列名", None, None)

        # 聚合与 GROUP BY 规则校验
        has_agg = any(isinstance(c, AggregateFunction) for c in resolved_columns)
        group_present = len(resolved_group_by) > 0
        if has_agg or group_present:
            group_names = {g.column_name for g in resolved_group_by}
            for c in resolved_columns:
                if isinstance(c, ColumnRef) and c.column_name not in group_names:
                    raise SemanticError("非聚合列必须包含在分组键中", getattr(c, 'line', None), getattr(c, 'column', None))

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
                    raise SemanticError(f"ORDER BY 列 {key_name} 必须出现在选择列表中", getattr(expr, 'line', None), getattr(expr, 'column', None))
                resolved_order_by.append(OrderItem(ColumnRef(key_name), item.direction))
            else:
                raise SemanticError("ORDER BY 只支持列名", None, None)

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
            raise SemanticError(f"表 {stmt.table_name} 不存在", None, None)
        # 列存在性
        if stmt.columns:
            table_cols = {c.name for c in schema.columns}
            for name in stmt.columns:
                if name not in table_cols:
                    raise SemanticError(f"列 {name} 不存在于表 {stmt.table_name}", None, None)
        # NOT NULL约束检查
        if stmt.columns and hasattr(stmt, 'values') and stmt.values:
            col_defs = [next(c for c in schema.columns if c.name == name) for name in stmt.columns]
            for row in stmt.values:
                # 列数检查
                if len(row) != len(col_defs):
                    raise SemanticError(f"插入的值数({len(row)})与列数({len(col_defs)})不一致", None, None)
                for col_def, value in zip(col_defs, row):
                    # NOT NULL检查（已实现）
                    if not col_def.nullable and value is None:
                        raise SemanticError(f"列 {col_def.name} 不允许为NULL", None, None)
                    # 类型一致性检查
                    if value is not None:
                        if hasattr(value, 'value'):
                            value = value.value
                        expected_type = col_def.data_type.name
                        if expected_type == "INTEGER" and not isinstance(value, int):
                            raise SemanticError(f"列 {col_def.name} 期望INTEGER类型，实际为{type(value).__name__}", None, None)
                        if expected_type == "FLOAT" and not (isinstance(value, float) or isinstance(value, int)):
                            raise SemanticError(f"列 {col_def.name} 期望FLOAT类型，实际为{type(value).__name__}", None, None)
                        if expected_type == "BOOLEAN" and not isinstance(value, bool):
                            raise SemanticError(f"列 {col_def.name} 期望BOOLEAN类型，实际为{type(value).__name__}", None, None)
                        if expected_type == "VARCHAR" and not isinstance(value, str):
                            raise SemanticError(f"列 {col_def.name} 期望VARCHAR类型，实际为{type(value).__name__}", None, None)
                        if expected_type == "CHAR" and not isinstance(value, str):
                            raise SemanticError(f"列 {col_def.name} 期望CHAR类型，实际为{type(value).__name__}", None, None)
                        if expected_type == "BIGINT" and not isinstance(value, int):
                            raise SemanticError(f"列 {col_def.name} 期望BIGINT类型，实际为{type(value).__name__}", None, None)
                        if expected_type == "TINYINT" and not isinstance(value, int):
                            raise SemanticError(f"列 {col_def.name} 期望TINYINT类型，实际为{type(value).__name__}", None, None)
                        if expected_type == "TEXT" and not isinstance(value, str):
                            raise SemanticError(f"列 {col_def.name} 期望TEXT类型，实际为{type(value).__name__}", None, None)
                        # 其它类型可按需扩展
        return AnalyzedResult(stmt)

    def _analyze_update(self, stmt: UpdateStatement) -> AnalyzedResult:
        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise SemanticError(f"表 {stmt.table_name} 不存在", None, None)
        table_cols = {c.name for c in schema.columns}
        for s in stmt.set_clauses:
            if s["column"] not in table_cols:
                raise SemanticError(f"列 {s['column']} 不存在于表 {stmt.table_name}", None, None)
        # 类型一致性检查 for UPDATE
        for s in stmt.set_clauses:
            col_def = next((c for c in schema.columns if c.name == s["column"]), None)
            if col_def is not None:
                value = s["value"]
                if hasattr(value, 'value'):
                    value = value.value
                if value is not None:
                    expected_type = col_def.data_type.name
                    if expected_type == "INTEGER" and not isinstance(value, int):
                        raise SemanticError(f"列 {col_def.name} 期望INTEGER类型，实际为{type(value).__name__}", None, None)
                    if expected_type == "FLOAT" and not (isinstance(value, float) or isinstance(value, int)):
                        raise SemanticError(f"列 {col_def.name} 期望FLOAT类型，实际为{type(value).__name__}", None, None)
                    if expected_type == "BOOLEAN" and not isinstance(value, bool):
                        raise SemanticError(f"列 {col_def.name} 期望BOOLEAN类型，实际为{type(value).__name__}", None, None)
                    if expected_type == "VARCHAR" and not isinstance(value, str):
                        raise SemanticError(f"列 {col_def.name} 期望VARCHAR类型，实际为{type(value).__name__}", None, None)
                    if expected_type == "CHAR" and not isinstance(value, str):
                        raise SemanticError(f"列 {col_def.name} 期望CHAR类型，实际为{type(value).__name__}", None, None)
                    if expected_type == "BIGINT" and not isinstance(value, int):
                        raise SemanticError(f"列 {col_def.name} 期望BIGINT类型，实际为{type(value).__name__}", None, None)
                    if expected_type == "TINYINT" and not isinstance(value, int):
                        raise SemanticError(f"列 {col_def.name} 期望TINYINT类型，实际为{type(value).__name__}", None, None)
                    if expected_type == "TEXT" and not isinstance(value, str):
                        raise SemanticError(f"列 {col_def.name} 期望TEXT类型，实际为{type(value).__name__}", None, None)
                    # 其它类型可按需扩展
        # WHERE子句列名检查
        def check_where_expr(expr):
            if isinstance(expr, ColumnRef):
                if expr.column_name not in table_cols:
                    raise SemanticError(f"WHERE子句列 {expr.column_name} 不存在于表 {stmt.table_name}", None, None)
            elif hasattr(expr, 'left') and hasattr(expr, 'right'):
                check_where_expr(expr.left)
                check_where_expr(expr.right)
            elif hasattr(expr, 'arg'):
                if isinstance(expr.arg, ColumnRef):
                    if expr.arg.column_name not in table_cols:
                        raise SemanticError(f"WHERE子句列 {expr.arg.column_name} 不存在于表 {stmt.table_name}", None, None)
            # 其它类型略
        if getattr(stmt, 'where_clause', None):
            check_where_expr(stmt.where_clause)
        return AnalyzedResult(stmt)

    def _analyze_delete(self, stmt: DeleteStatement) -> AnalyzedResult:
        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise SemanticError(f"表 {stmt.table_name} 不存在", None, None)
        table_cols = {c.name for c in schema.columns}
        # WHERE子句列名检查
        def check_where_expr(expr):
            if isinstance(expr, ColumnRef):
                if expr.column_name not in table_cols:
                    raise SemanticError(f"WHERE子句列 {expr.column_name} 不存在于表 {stmt.table_name}", None, None)
            elif hasattr(expr, 'left') and hasattr(expr, 'right'):
                check_where_expr(expr.left)
                check_where_expr(expr.right)
            elif hasattr(expr, 'arg'):
                if isinstance(expr.arg, ColumnRef):
                    if expr.arg.column_name not in table_cols:
                        raise SemanticError(f"WHERE子句列 {expr.arg.column_name} 不存在于表 {stmt.table_name}", None, None)
            # 其它类型略
        if getattr(stmt, 'where_clause', None):
            check_where_expr(stmt.where_clause)
        return AnalyzedResult(stmt)

    def _analyze_create_table(self, stmt: CreateTableStatement) -> AnalyzedResult:
        # CREATE TABLE
        schema = self.catalog.get_table_schema(stmt.table_name)
        if schema and getattr(stmt, 'if_not_exists', False):
            # 已存在且 IF NOT EXISTS，直接通过
            return AnalyzedResult(stmt)
        if schema:
            raise SemanticError(f"表 {stmt.table_name} 已存在", None, None)
        # 重复列名检查
        col_names = [col['name'] if isinstance(col, dict) else getattr(col, 'name', None) for col in stmt.columns]
        if len(col_names) != len(set(col_names)):
            raise SemanticError(f"表 {stmt.table_name} 存在重复列名", None, None)
        # 多主键检查
        pk_count = 0
        for col in stmt.columns:
            if isinstance(col, dict):
                constraints = col.get('constraints', [])
            else:
                constraints = getattr(col, 'constraints', [])
            if 'PRIMARY KEY' in constraints:
                pk_count += 1
        if pk_count > 1:
            raise SemanticError(f"表 {stmt.table_name} 存在多个主键", None, None)
        return AnalyzedResult(stmt)

    def _analyze_drop_table(self, stmt: DropTableStatement) -> AnalyzedResult:
        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema and getattr(stmt, 'if_exists', False):
            # 不存在且 IF EXISTS，直接通过
            return AnalyzedResult(stmt)
        if not schema:
            raise SemanticError(f"表 {stmt.table_name} 不存在", None, None)
        return AnalyzedResult(stmt)

    # 你可以按需为索引、视图、用户等类似扩展 
    
    def _analyze_create_trigger(self, stmt: CreateTriggerStatement) -> AnalyzedResult:
        """分析CREATE TRIGGER语句"""
        # 检查表是否存在
        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise SemanticError(f"表 {stmt.table_name} 不存在", None, None)
        
        # 检查事件是否合法
        if stmt.event not in ('INSERT', 'UPDATE', 'DELETE'):
            raise SemanticError(f"不支持的事件类型: {stmt.event}", None, None)
            
        # 检查时机是否合法
        if stmt.timing not in ('BEFORE', 'AFTER'):
            raise SemanticError(f"不支持的触发时机: {stmt.timing}", None, None)
        
        # 触发器体的语义分析在执行阶段进行（因为可能引用当前表）
        return AnalyzedResult(stmt)

    def _analyze_drop_trigger(self, stmt: DropTriggerStatement) -> AnalyzedResult:
        """分析DROP TRIGGER语句"""
        # 基本校验，实际的触发器存在性检查在执行阶段进行
        return AnalyzedResult(stmt)

    def _analyze_alter_table(self, stmt: 'AlterTableStatement') -> AnalyzedResult:
        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise SemanticError(f"表 {stmt.table_name} 不存在", None, None)
        if stmt.action == 'ADD':
            col_name = stmt.column_def['name']
            if any(c.name == col_name for c in schema.columns):
                raise SemanticError(f"列 {col_name} 已存在于表 {stmt.table_name}", None, None)
        elif stmt.action == 'DROP':
            col_name = stmt.column_name
            if not any(c.name == col_name for c in schema.columns):
                raise SemanticError(f"列 {col_name} 不存在于表 {stmt.table_name}", None, None)
        else:
            raise SemanticError(f"ALTER TABLE 不支持的操作: {stmt.action}", None, None)
        return AnalyzedResult(stmt)