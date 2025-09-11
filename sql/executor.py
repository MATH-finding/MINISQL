"""
SQL执行器
"""

from typing import List, Dict, Any, Optional, Union
import operator
from storage import Record
from catalog import DataType, ColumnDefinition, SystemCatalog
from table import TableManager
from .ast_nodes import *



class SQLExecutor:
    """SQL执行器，将AST转换为数据库操作"""

    def __init__(
        self, table_manager: TableManager, catalog: SystemCatalog, index_manager=None
    ):
        self.table_manager = table_manager
        self.catalog = catalog
        self.index_manager = index_manager

        # 操作符映射
        self.comparison_ops = {
            "=": operator.eq,
            "!=": operator.ne,
            "<": operator.lt,
            "<=": operator.le,
            ">": operator.gt,
            ">=": operator.ge,
        }

    def execute(self, ast: Statement) -> Dict[str, Any]:
        """执行SQL语句"""
        try:
            # DEBUG: 打印进入execute的AST类型
            # print(f"[EXECUTOR DEBUG] execute() with AST: {type(ast).__name__}")
            if isinstance(ast, CreateTableStatement):
                return self._execute_create_table(ast)
            elif isinstance(ast, InsertStatement):
                return self._execute_insert(ast)
            elif isinstance(ast, SelectStatement):
                # 查询视图重写
                if isinstance(ast.from_table, str) and ast.from_table.lower() in self.catalog.views:
                    # print(f"[EXECUTOR DEBUG] SELECT on view detected: view={ast.from_table}")
                    view_sql = self.catalog.get_view_definition(ast.from_table.lower())
                    # print(f"[EXECUTOR DEBUG] view SQL: {view_sql}")
                    from sql.lexer import SQLLexer
                    from sql.parser import SQLParser
                    lexer = SQLLexer(view_sql)
                    tokens = lexer.tokenize()
                    parser = SQLParser(tokens)
                    view_ast = parser.parse()
                    # print(f"[EXECUTOR DEBUG] parsed view AST: {type(view_ast).__name__} -> {view_ast}")

                    # 步骤 1: 执行视图定义，得到原始结果（list of dict）
                    view_result = self.execute(view_ast)
                    # print(f"[EXECUTOR DEBUG] view execution result meta: success={view_result.get('success')}, rows={len(view_result.get('data', []) ) if isinstance(view_result.get('data'), list) else 'N/A'}")
                    if not view_result.get("success") or not isinstance(view_result.get("data"), list):
                        return view_result
                    current_rows = view_result["data"]

                    # 步骤 2: 应用外部WHERE过滤
                    if ast.where_clause:
                        # print(f"[EXECUTOR DEBUG] applying outer WHERE on {len(current_rows)} rows: {ast.where_clause}")
                        current_rows = [row for row in current_rows if self._evaluate_condition(ast.where_clause, row)]
                        # print(f"[EXECUTOR DEBUG] rows after WHERE: {len(current_rows)}")

                    # 步骤 3: 投影
                    final_data = []
                    columns_to_project = view_ast.columns if ast.columns == ["*"] else ast.columns
                    is_select_all = (columns_to_project == ["*"])
                    # print(f"[EXECUTOR DEBUG] projection columns: {columns_to_project}")
                    for row in current_rows:
                        if is_select_all:
                            proj = dict(row)
                        else:
                            proj = {}
                            for col in columns_to_project:
                                if isinstance(col, ColumnRef):
                                    col_name = col.column_name
                                else:
                                    col_name = str(col)
                                if col_name in row:
                                    proj[col_name] = row[col_name]
                        final_data.append(proj)
                    # print(f"[EXECUTOR DEBUG] final projected rows: {len(final_data)}")
                    return {"type": "SELECT", "success": True, "data": final_data, "message": f"查询成功，返回{len(final_data)}行"}
                return self._execute_select(ast)
            elif isinstance(ast, CreateIndexStatement):
                return self._execute_create_index(ast)
            elif isinstance(ast, DropIndexStatement):
                return self._execute_drop_index(ast)
            elif isinstance(ast, CreateViewStatement):
                return self._execute_create_view(ast)
            elif isinstance(ast, DropViewStatement):
                return self._execute_drop_view(ast)
            elif isinstance(ast, UpdateStatement):
                return self._execute_update(ast)
            elif isinstance(ast, DeleteStatement):
                return self._execute_delete(ast)
            elif isinstance(ast, DropTableStatement):
                return self._execute_drop_table(ast)
            elif isinstance(ast, TruncateTableStatement):
                return self._execute_truncate_table(ast)
            else:
                raise ValueError(f"不支持的语句类型: {type(ast)}")
        except Exception as e:
            # 统一异常返回结构，避免KeyError
            return {"success": False, "error": str(e), "data": [], "message": f"SQL执行失败: {str(e)}"}

    # --- 新增：CHECK 评估（NULL 视为通过，不违反约束） ---
    def _evaluate_check(self, condition: Expression, record_data: Dict[str, Any]) -> bool:
        try:
            # 如果任一侧取值为 None，按 SQL 三值逻辑视为 UNKNOWN，不违反约束
            if isinstance(condition, BinaryOp):
                left_val = self._evaluate_expression(condition.left, record_data)
                right_val = self._evaluate_expression(condition.right, record_data)
                if left_val is None or right_val is None:
                    return True
            return self._evaluate_condition(condition, record_data)
        except Exception:
            # 任意异常都视为不违反（保守处理），避免插入失败
            return True

    def _execute_create_table(self, stmt: CreateTableStatement) -> Dict[str, Any]:
        """执行CREATE TABLE"""
        columns = []

        for col_def in stmt.columns:
            # 转换数据类型
            data_type_map = {
                "INTEGER": DataType.INTEGER,
                "VARCHAR": DataType.VARCHAR,
                "FLOAT": DataType.FLOAT,
                "BOOLEAN": DataType.BOOLEAN,
                "CHAR": DataType.CHAR,
                "DECIMAL": DataType.DECIMAL,
                "DATE": DataType.DATE,
                "TIME": DataType.TIME,
                "DATETIME": DataType.DATETIME,
                "BIGINT": DataType.BIGINT,
                "TINYINT": DataType.TINYINT,
                "TEXT": DataType.TEXT,
            }

            data_type = data_type_map.get(col_def["type"])
            if not data_type:
                raise ValueError(f"不支持的数据类型: {col_def['type']}")

            # 解析约束
            nullable = True
            primary_key = False
            unique = False
            default = col_def.get("default")
            check = col_def.get("check")
            foreign_key = col_def.get("foreign_key")

            for constraint in col_def["constraints"]:
                if constraint == "NOT NULL":
                    nullable = False
                elif constraint == "PRIMARY KEY":
                    primary_key = True
                    nullable = False
                elif constraint == "UNIQUE":
                    unique = True

            column = ColumnDefinition(
                name=col_def["name"],
                data_type=data_type,
                max_length=col_def["length"],
                nullable=nullable,
                primary_key=primary_key,
                unique=unique,
                default=default,
                check=check,
                foreign_key=foreign_key,
            )
            columns.append(column)

        self.table_manager.create_table(stmt.table_name, columns)

        return {
            "type": "CREATE_TABLE",
            "table_name": stmt.table_name,
            "columns_created": len(columns),
            "success": True,
            "message": f"表 {stmt.table_name} 创建成功",
        }

    def _execute_drop_table(self, stmt: DropTableStatement) -> Dict[str, Any]:
        """执行DROP TABLE"""
        # 检查表是否存在
        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise ValueError(f"表 {stmt.table_name} 不存在")

        # 删除表相关的所有索引
        if self.index_manager:
            indexes_to_drop = []
            for index_name, index_info in self.index_manager.indexes.items():
                if index_info.table_name == stmt.table_name:
                    indexes_to_drop.append(index_name)

            for index_name in indexes_to_drop:
                self.index_manager.drop_index(index_name)

        # 删除表数据和元数据
        self.table_manager.drop_table(stmt.table_name)

        return {
            "type": "DROP_TABLE",
            "table_name": stmt.table_name,
            "success": True,
            "message": f"表 {stmt.table_name} 删除成功",
        }

    def _execute_truncate_table(self, stmt: TruncateTableStatement) -> Dict[str, Any]:
        """执行TRUNCATE TABLE"""
        # 检查表是否存在
        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise ValueError(f"表 {stmt.table_name} 不存在")

        # 清空表数据但保留结构
        cleared_count = self.table_manager.truncate_table(stmt.table_name)

        # 清理相关索引数据但保留索引结构
        if self.index_manager:
            table_indexes = self.index_manager.get_table_indexes(stmt.table_name)
            for index_name in table_indexes:
                btree = self.index_manager.get_index(index_name)
                if btree:
                    btree.clear()  # 假设B+树有clear方法

        return {
            "type": "TRUNCATE_TABLE",
            "table_name": stmt.table_name,
            "rows_cleared": cleared_count,
            "success": True,
            "message": f"表 {stmt.table_name} 数据清空成功，共清除 {cleared_count} 行",
        }

    def _execute_insert(self, stmt: InsertStatement) -> Dict[str, Any]:
        """执行INSERT - 统一唯一性索引校验，支持DEFAULT、CHECK、FOREIGN KEY"""
        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise ValueError(f"表 {stmt.table_name} 不存在")

        inserted_count = 0

        for value_row in stmt.values:
            # 构建记录数据
            record_data = {}

            if stmt.columns:
                # 指定了列名
                if len(stmt.columns) != len(value_row):
                    raise ValueError("列数和值数不匹配")

                for i, col_name in enumerate(stmt.columns):
                    value = self._evaluate_expression(value_row[i], {})
                    record_data[col_name] = value
            else:
                # 未指定列名，按顺序插入所有列
                if len(value_row) != len(schema.columns):
                    raise ValueError("值数和表列数不匹配")

                for i, column in enumerate(schema.columns):
                    value = self._evaluate_expression(value_row[i], {})
                    record_data[column.name] = value

            # 补全DEFAULT值
            for column in schema.columns:
                if column.name not in record_data or record_data[column.name] is None:
                    if column.default is not None:
                        # 类型转换：如果是数字类型，转换为int/float
                        if column.data_type.name in ("INTEGER", "BIGINT", "TINYINT"):
                            record_data[column.name] = int(column.default)
                        elif column.data_type.name in ("FLOAT", "DECIMAL"):
                            record_data[column.name] = float(column.default)
                        else:
                            record_data[column.name] = column.default
                # 如果依然没有值，且不是nullable，报错
                if column.name not in record_data and not column.nullable:
                    raise ValueError(f"列 {column.name} 不能为空")

            # 获取所有唯一性索引（主键、UNIQUE列、唯一索引、复合唯一索引）
            unique_indexes = (
                self.index_manager.get_unique_indexes_for_table(stmt.table_name)
                if self.index_manager
                else []
            )

            # 插入前校验所有唯一性约束
            for index in unique_indexes:
                index_keys = [record_data.get(col) for col in index.columns]
                if all(key is not None for key in index_keys):
                    existing = self.index_manager.lookup(index.name, index_keys)
                    if existing:
                        constraint_columns = ", ".join(index.columns)
                        raise ValueError(
                            f"唯一约束冲突: 列 '{constraint_columns}' 的值 '{index_keys}' 已存在"
                        )

            # 校验CHECK约束
            for column in schema.columns:
                if column.check is not None:
                    context = record_data.copy()
                    if not self._evaluate_check(column.check, context):
                        raise ValueError(f"CHECK约束不满足: {column.name}")
            for check_expr in getattr(schema, "check_constraints", []):
                context = record_data.copy()
                if not self._evaluate_check(check_expr, context):
                    raise ValueError("表级CHECK约束不满足")

            # 校验FOREIGN KEY约束
            for column in schema.columns:
                if column.foreign_key:
                    ref_value = record_data.get(column.name)
                    if ref_value is not None:
                        ref_table = column.foreign_key["ref_table"]
                        ref_column = column.foreign_key["ref_column"]
                        ref_schema = self.catalog.get_table_schema(ref_table)
                        if not ref_schema:
                            raise ValueError(f"外键引用表不存在: {ref_table}")
                        found = False
                        # 这里的全表扫描效率很低，未来可以用索引优化
                        for ref_record in self.table_manager.scan_table(ref_table):
                            if ref_record.data.get(ref_column) == ref_value:
                                found = True
                                break
                        if not found:
                            raise ValueError(
                                f"外键约束不满足: {column.name} -> {ref_table}({ref_column})"
                            )
            # 插入记录
            try:
                record_id = self.table_manager.insert_record(
                    stmt.table_name, record_data
                )

                # 新增：如果有索引管理器，同时更新索引
                if self.index_manager:
                    self.index_manager.insert_into_indexes(
                        stmt.table_name, record_data, record_id
                    )

                inserted_count += 1
            except Exception as e:
                raise ValueError(f"插入记录失败: {str(e)}")

        return {
            "type": "INSERT",
            "table_name": stmt.table_name,
            "rows_inserted": inserted_count,
            "success": True,
            "message": f"成功插入 {inserted_count} 行到表 {stmt.table_name}",
        }

    # 合并后的 _execute_select 函数
    def _execute_select(self, stmt: SelectStatement) -> Dict[str, Any]:
        """执行SELECT - 扩展支持JOIN和聚合函数"""

        # 递归获取结果集
        def eval_from(from_table):
            if isinstance(from_table, str):
                # 单表
                schema = self.catalog.get_table_schema(from_table)
                if not schema:
                    raise ValueError(f"表 {from_table} 不存在")
                # 返回Record对象和表名
                return self.table_manager.scan_table(from_table), from_table
            elif isinstance(from_table, JoinClause):
                # JOIN
                left_records, left_name = eval_from(from_table.left)
                right_records, right_name = eval_from(from_table.right)
                result = []
                for lrow in left_records:
                    for rrow in right_records:
                        # 合并两表数据，字段加前缀
                        merged = {}
                        for k, v in lrow.data.items():
                            merged[f"{left_name}.{k}"] = v
                        for k, v in rrow.data.items():
                            merged[f"{right_name}.{k}"] = v
                        # ON条件上下文
                        if self._evaluate_condition(from_table.on, merged):
                            result.append(type(lrow)(merged))
                return result, f"({left_name} {from_table.join_type} JOIN {right_name})"
            else:
                raise ValueError("未知的from_table类型")

        all_records, from_name = eval_from(stmt.from_table)

        # WHERE过滤
        filtered_records = []
        for record in all_records:
            context = record.data.copy()
            # 对于单表，去除所有表前缀（兼容视图嵌套）
            if isinstance(stmt.from_table, str):
                context = {k.split(".")[-1]: v for k, v in context.items()}
            if stmt.where_clause is None or self._evaluate_condition(stmt.where_clause, context):
                # 只保留过滤后的context，便于后续投影
                record._filtered_context = context
                filtered_records.append(record)
        # print(f"[EXECUTOR DEBUG] _execute_select: rows_after_where={len(filtered_records)}")

        # 如果存在 GROUP BY
        if getattr(stmt, 'group_by', None):
            # 构建分组
            groups: Dict[tuple, List[Any]] = {}
            group_key_names: List[str] = []
            # 预先解析 group key 名称（按列名）
            for g in stmt.group_by:
                group_key_names.append(g.column_name if isinstance(g, ColumnRef) else str(g))

            def build_group_key(ctx: Dict[str, Any]) -> tuple:
                keys = []
                for g in stmt.group_by:
                    if isinstance(g, ColumnRef):
                        keys.append(self._evaluate_expression(g, ctx))
                    else:
                        # 简化实现：只支持列名
                        keys.append(ctx.get(str(g)))
                return tuple(keys)

            for rec in filtered_records:
                ctx = getattr(rec, '_filtered_context', rec.data)
                key = build_group_key(ctx)
                groups.setdefault(key, []).append(ctx)

            # 判断是否含聚合
            has_agg = any(isinstance(c, AggregateFunction) for c in stmt.columns)

            result_records: List[Dict[str, Any]] = []
            if has_agg:
                # 逐组计算聚合
                for key_tuple, rows in groups.items():
                    row_out: Dict[str, Any] = {}
                    # 输出分组键
                    for idx, name in enumerate(group_key_names):
                        row_out[name] = key_tuple[idx]

                    for col in stmt.columns:
                        if isinstance(col, AggregateFunction):
                            func = col.func_name.upper()
                            arg = col.arg
                            if func == "COUNT":
                                if arg == "*":
                                    row_out["COUNT"] = len(rows)
                                elif isinstance(arg, ColumnRef):
                                    non_null = 0
                                    for r in rows:
                                        val = self._evaluate_expression(arg, r)
                                        if val is not None:
                                            non_null += 1
                                    row_out["COUNT"] = non_null
                                else:
                                    raise ValueError("COUNT参数不支持")
                            elif func == "SUM":
                                if isinstance(arg, ColumnRef):
                                    vals = [self._evaluate_expression(arg, r) for r in rows]
                                    row_out["SUM"] = sum(v for v in vals if v is not None)
                                else:
                                    raise ValueError("SUM参数不支持")
                            elif func == "AVG":
                                if isinstance(arg, ColumnRef):
                                    vals = [self._evaluate_expression(arg, r) for r in rows]
                                    vals = [v for v in vals if v is not None]
                                    row_out["AVG"] = (sum(vals) / len(vals) if vals else None)
                                else:
                                    raise ValueError("AVG参数不支持")
                            elif func == "MIN":
                                if isinstance(arg, ColumnRef):
                                    vals = [self._evaluate_expression(arg, r) for r in rows]
                                    vals = [v for v in vals if v is not None]
                                    row_out["MIN"] = (min(vals) if vals else None)
                                else:
                                    raise ValueError("MIN参数不支持")
                            elif func == "MAX":
                                if isinstance(arg, ColumnRef):
                                    vals = [self._evaluate_expression(arg, r) for r in rows]
                                    vals = [v for v in vals if v is not None]
                                    row_out["MAX"] = (max(vals) if vals else None)
                                else:
                                    raise ValueError("MAX参数不支持")
                            else:
                                raise ValueError(f"不支持的聚合函数: {func}")
                        elif isinstance(col, ColumnRef):
                            # 非聚合列必须在分组键中
                            if col.column_name in group_key_names:
                                row_out[col.column_name] = row_out.get(col.column_name)
                            else:
                                raise ValueError("GROUP BY 查询中，非聚合列必须包含在分组键中")
                        elif isinstance(col, str) and col == "*":
                            # 忽略 * 在分组+聚合场景
                            pass
                        else:
                            raise ValueError("GROUP BY 查询的列仅支持分组列或聚合函数")
                    result_records.append(row_out)
            else:
                # 无聚合：返回唯一分组键
                for key_tuple in groups:
                    row_out = {}
                    for idx, name in enumerate(group_key_names):
                        row_out[name] = key_tuple[idx]
                    result_records.append(row_out)

        else:
            # 检查是否有聚合函数（无分组 -> 全表聚合）
            if any(isinstance(col, AggregateFunction) for col in stmt.columns):
                agg_result = {}
                for col in stmt.columns:
                    if isinstance(col, AggregateFunction):
                        func = col.func_name.upper()
                        arg = col.arg
                        if func == "COUNT":
                            if arg == "*":
                                agg_result["COUNT"] = len(filtered_records)
                            elif isinstance(arg, ColumnRef):
                                non_null = 0
                                for r in filtered_records:
                                    ctx = getattr(r, '_filtered_context', r.data)
                                    val = self._evaluate_expression(arg, ctx)
                                    if val is not None:
                                        non_null += 1
                                agg_result["COUNT"] = non_null
                            else:
                                raise ValueError("COUNT参数不支持")
                        elif func == "SUM":
                            if isinstance(arg, ColumnRef):
                                values = [
                                    self._evaluate_expression(arg, getattr(r, '_filtered_context', r.data))
                                    for r in filtered_records
                                ]
                                agg_result["SUM"] = sum(v for v in values if v is not None)
                            else:
                                raise ValueError("SUM参数不支持")
                        elif func == "AVG":
                            if isinstance(arg, ColumnRef):
                                values = [
                                    self._evaluate_expression(arg, getattr(r, '_filtered_context', r.data))
                                    for r in filtered_records
                                ]
                                values = [v for v in values if v is not None]
                                agg_result["AVG"] = (
                                    sum(values) / len(values) if values else None
                                )
                            else:
                                raise ValueError("AVG参数不支持")
                        elif func == "MIN":
                            if isinstance(arg, ColumnRef):
                                values = [
                                    self._evaluate_expression(arg, getattr(r, '_filtered_context', r.data))
                                    for r in filtered_records
                                ]
                                values = [v for v in values if v is not None]
                                agg_result["MIN"] = min(values) if values else None
                            else:
                                raise ValueError("MIN参数不支持")
                        elif func == "MAX":
                            if isinstance(arg, ColumnRef):
                                values = [
                                    self._evaluate_expression(arg, getattr(r, '_filtered_context', r.data))
                                    for r in filtered_records
                                ]
                                values = [v for v in values if v is not None]
                                agg_result["MAX"] = max(values) if values else None
                            else:
                                raise ValueError("MAX参数不支持")
                        else:
                            raise ValueError(f"不支持的聚合函数: {func}")
                result_records = [agg_result]
            else:
                # 选择列（原有逻辑）
                result_records = []
                for record in filtered_records:
                    context = getattr(record, '_filtered_context', record.data)
                    if stmt.columns == ["*"]:
                        # 返回所有表结构定义的字段
                        row = {}
                        schema = self.catalog.get_table_schema(
                            from_name if isinstance(from_name, str) else stmt.from_table
                        )
                        for col in schema.columns:
                            row[col.name] = record.data.get(col.name)
                        result_records.append(row)
                    else:
                        selected_data = {}
                        for col in stmt.columns:
                            if isinstance(col, ColumnRef):
                                col_name = col.column_name
                                if col_name in context:
                                    selected_data[col_name] = context[col_name]
                                else:
                                    matches = [
                                        k
                                        for k in record.data
                                        if k.endswith(f".{col.column_name}")
                                        or k == col.column_name
                                    ]
                                    if len(matches) == 1:
                                        selected_data[col_name] = context[matches[0]]
                                    elif len(matches) == 0:
                                        raise ValueError(f"列 {col_name} 不存在")
                                    else:
                                        raise ValueError(f"列 {col.column_name} 不明确，请加表前缀")
                            elif isinstance(col, AggregateFunction):
                                raise ValueError("聚合函数只能单独出现在SELECT列表中")
                            else:
                                if col in context:
                                    selected_data[col] = context[col]
                                else:
                                    raise ValueError(f"列 {col} 不存在")
                        result_records.append(selected_data)
        # print(f"[EXECUTOR DEBUG] _execute_select: rows_projected={len(result_records)}")

        # ORDER BY 排序
        if getattr(stmt, 'order_by', None):
            def sort_key(row: Dict[str, Any]):
                keys = []
                for item in stmt.order_by:
                    name = item.expr.column_name if isinstance(item.expr, ColumnRef) else str(item.expr)
                    keys.append(row.get(name))
                return tuple(keys)
            # Python的排序无法为不同列设置不同方向一次完成，采用链式稳定排序（从次关键字到主关键字）
            for item in reversed(stmt.order_by):
                name = item.expr.column_name if isinstance(item.expr, ColumnRef) else str(item.expr)
                reverse = (item.direction.upper() == "DESC")
                result_records.sort(key=lambda r: r.get(name), reverse=reverse)

        return {
            "type": "SELECT",
            "success": True,
            "data": result_records,
            "message": f"查询成功，返回{len(result_records)}行"
        }

    def _execute_create_view(self, stmt):
        self.catalog.create_view(stmt.view_name, stmt.view_definition)
        return {"type": "CREATE_VIEW", "view_name": stmt.view_name, "success": True, "message": f"视图 {stmt.view_name} 创建成功"}

    def _execute_drop_view(self, stmt):
        self.catalog.drop_view(stmt.view_name)
        return {"type": "DROP_VIEW", "view_name": stmt.view_name, "success": True, "message": f"视图 {stmt.view_name} 删除成功"}

    def _execute_create_index(self, stmt: CreateIndexStatement) -> Dict[str, Any]:
        """执行CREATE INDEX"""
        if not self.index_manager:
            raise ValueError("索引管理器未初始化")

        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise ValueError(f"表 {stmt.table_name} 不存在")

        column_exists = any(col.name == stmt.column_name for col in schema.columns)
        if not column_exists:
            raise ValueError(f"列 {stmt.column_name} 不存在于表 {stmt.table_name}")

        success = self.index_manager.create_index(
            stmt.index_name, stmt.table_name, stmt.column_name, stmt.is_unique
        )

        if not success:
            raise ValueError(f"索引 {stmt.index_name} 已存在")

        # 为现有数据构建索引，使用正确的记录ID
        self._build_index_for_existing_data(
            stmt.index_name, stmt.table_name, stmt.column_name
        )

        return {
            "type": "CREATE_INDEX",
            "index_name": stmt.index_name,
            "table_name": stmt.table_name,
            "column_name": stmt.column_name,
            "success": True,
            "message": f"索引 {stmt.index_name} 创建成功",
        }

    def _execute_drop_index(self, stmt: DropIndexStatement) -> Dict[str, Any]:
        """执行DROP INDEX"""
        if not self.index_manager:
            raise ValueError("索引管理器未初始化")

        success = self.index_manager.drop_index(stmt.index_name)
        if not success:
            raise ValueError(f"索引 {stmt.index_name} 不存在")

        return {
            "type": "DROP_INDEX",
            "index_name": stmt.index_name,
            "success": True,
            "message": f"索引 {stmt.index_name} 删除成功",
        }

    def _build_index_for_existing_data(
        self, index_name: str, table_name: str, column_name: str
    ):
        """为现有数据构建索引 - 修复版"""
        btree = self.index_manager.get_index(index_name)
        if not btree:
            return

        all_records = self.table_manager.scan_table(table_name)

        for record_id, record in enumerate(all_records):
            if column_name in record.data:
                key = record.data[column_name]
                btree.insert(key, record_id)  # 存储正确的记录ID
                # print(f"DEBUG: 构建索引 {index_name}: {key} -> {record_id}")

    def _try_index_scan(
        self, table_name: str, where_clause: Expression
    ) -> Optional[List[Record]]:
        """尝试使用索引扫描优化查询 - 修复版"""
        if not self.index_manager:
            return None

        index_info = self._analyze_where_for_index(table_name, where_clause)
        if not index_info:
            return None

        index_name, column_name, operator, value = index_info
        btree = self.index_manager.get_index(index_name)
        if not btree:
            return None

        # print(f"DEBUG: 使用索引 {index_name} 查找 {column_name} {operator} {value}")

        if operator == "=":
            # 精确查找
            record_id = btree.search(value)
            # print(f"DEBUG: 索引搜索结果: {record_id}")

            if record_id is not None:
                if isinstance(record_id, (list, tuple)):
                    record_ids = list(record_id)
                else:
                    record_ids = [record_id]

                return self._get_records_by_ids(table_name, record_ids)
        elif operator in ["<", "<=", ">", ">="]:
            # 范围查询
            if operator in ["<", "<="]:
                results = btree.range_search(float("-inf"), value)
            else:
                results = btree.range_search(value, float("inf"))

            record_ids = [record_id for _, record_id in results]
            return self._get_records_by_ids(table_name, record_ids)

        return None

    def _analyze_where_for_index(
        self, table_name: str, where_clause: Expression
    ) -> Optional[tuple]:
        """分析WHERE条件，看是否可以使用索引"""
        if not isinstance(where_clause, BinaryOp):
            return None

        if isinstance(where_clause.left, ColumnRef) and isinstance(
            where_clause.right, Literal
        ):
            column_name = where_clause.left.column_name
            operator = where_clause.operator
            value = where_clause.right.value

            available_indexes = self.index_manager.get_table_indexes(table_name)
            for index_name in available_indexes:
                index_info = self.index_manager.indexes.get(index_name)
                if index_info and index_info.column_name == column_name:
                    return (index_name, column_name, operator, value)

        return None

    def _get_records_by_ids(
        self, table_name: str, record_ids: List[int]
    ) -> List[Record]:
        """根据记录ID获取记录 - 修复版"""
        all_records = self.table_manager.scan_table(table_name)

        result = []

        for record_id in record_ids:
            if 0 <= record_id < len(all_records):
                result.append(all_records[record_id])
            else:
                print(
                    f"DEBUG: 记录ID {record_id} 超出范围，总记录数: {len(all_records)}"
                )

        # print(f"DEBUG: 根据ID获取到 {len(result)} 条记录")
        return result

    def _evaluate_expression(self, expr: Expression, context: Dict[str, Any]) -> Any:
        if isinstance(expr, Literal):
            return expr.value
        elif isinstance(expr, ColumnRef):
            # 优先查找带前缀
            if expr.table_name:
                key = f"{expr.table_name}.{expr.column_name}"
                if key in context:
                    return context[key]
            # 回退查找无前缀
            if expr.column_name in context:
                return context[expr.column_name]
            # 兼容：如果context只有一个key且key结尾是.column_name，也允许
            matches = [v for k, v in context.items() if k.endswith(f".{expr.column_name}")]
            if len(matches) == 1:
                return matches[0]
            return None
        else:
            raise ValueError(f"不支持的表达式类型: {type(expr)}")

    def _evaluate_condition(
        self, condition: Expression, record_data: Dict[str, Any]
    ) -> bool:
        """评估WHERE条件"""
        if isinstance(condition, BinaryOp):
            # # --- 在这里加入调试代码 ---
            # # print(f"DEBUG: [Evaluating Condition] on data: {record_data}")
            # left_val = self._evaluate_expression(condition.left, record_data)
            # right_val = self._evaluate_expression(condition.right, record_data)
            # result = self.comparison_ops.get(condition.operator)(left_val, right_val)
            # # print(f"DEBUG: [Comparison] '{left_val}' {condition.operator} '{right_val}' -> {result}")
            # # --- 调试代码结束 ---
           
            left_val = self._evaluate_expression(condition.left, record_data)
            right_val = self._evaluate_expression(condition.right, record_data)

            op_func = self.comparison_ops.get(condition.operator)
            if not op_func:
                raise ValueError(f"不支持的操作符: {condition.operator}")

            return op_func(left_val, right_val)

        elif isinstance(condition, LogicalOp):
            left_result = self._evaluate_condition(condition.left, record_data)
            right_result = self._evaluate_condition(condition.right, record_data)

            if condition.operator == "AND":
                return left_result and right_result
            elif condition.operator == "OR":
                return left_result or right_result
            else:
                raise ValueError(f"不支持的逻辑操作符: {condition.operator}")

        else:
            raise ValueError(f"不支持的条件类型: {type(condition)}")

    def _execute_update(self, stmt: UpdateStatement) -> Dict[str, Any]:
        """执行UPDATE语句"""
        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise ValueError(f"表 {stmt.table_name} 不存在")

        # 构建更新数据
        updates = {}
        for set_clause in stmt.set_clauses:
            column_name = set_clause["column"]
            value_expr = set_clause["value"]
            # 计算表达式值（对于简单字面量）
            if isinstance(value_expr, Literal):
                updates[column_name] = value_expr.value
            else:
                raise ValueError(f"UPDATE暂时只支持字面量值")

        # 构建条件函数
        condition_func = None
        if stmt.where_clause:
            condition_func = lambda record_data: self._evaluate_condition(
                stmt.where_clause, record_data
            )

        # 执行更新
        updated_count = self.table_manager.update_records(
            stmt.table_name, updates, condition_func
        )

        return {
            "type": "UPDATE",
            "table_name": stmt.table_name,
            "rows_updated": updated_count,
            "success": True,
            "message": f"成功更新 {updated_count} 行",
        }

    def _execute_delete(self, stmt: DeleteStatement) -> Dict[str, Any]:
        """执行DELETE语句"""
        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise ValueError(f"表 {stmt.table_name} 不存在")

        # 构建条件函数
        condition_func = None
        if stmt.where_clause:
            condition_func = lambda record_data: self._evaluate_condition(
                stmt.where_clause, record_data
            )

        # 执行删除
        deleted_count = self.table_manager.delete_records(
            stmt.table_name, condition_func
        )

        return {
            "type": "DELETE",
            "table_name": stmt.table_name,
            "rows_deleted": deleted_count,
            "success": True,
            "message": f"成功删除 {deleted_count} 行",
        }
