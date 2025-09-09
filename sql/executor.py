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
        if isinstance(ast, CreateTableStatement):
            return self._execute_create_table(ast)
        elif isinstance(ast, InsertStatement):
            return self._execute_insert(ast)
        elif isinstance(ast, SelectStatement):
            return self._execute_select(ast)
        elif isinstance(ast, CreateIndexStatement):
            return self._execute_create_index(ast)
        elif isinstance(ast, DropIndexStatement):
            return self._execute_drop_index(ast)
        elif isinstance(ast, UpdateStatement):  # 新增
            return self._execute_update(ast)
        elif isinstance(ast, DeleteStatement):  # 新增
            return self._execute_delete(ast)
        else:
            raise ValueError(f"不支持的语句类型: {type(ast)}")

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
            unique_indexes = self.index_manager.get_unique_indexes_for_table(stmt.table_name) if self.index_manager else []

            # 插入前校验所有唯一性约束
            for index in unique_indexes:
                index_keys = [record_data.get(col) for col in index.columns]
                if all(key is not None for key in index_keys):
                    existing = self.index_manager.lookup(index.name, index_keys)
                    if existing:
                        constraint_columns = ', '.join(index.columns)
                        raise ValueError(f"唯一约束冲突: 列 '{constraint_columns}' 的值 '{index_keys}' 已存在")

            # 校验CHECK约束
            for column in schema.columns:
                if column.check is not None : 
                    context = record_data.copy()
                    if not self._evaluate_condition(column.check, context):
                        raise ValueError(f"CHECK约束不满足: {column.name}")
            for check_expr in getattr(schema, 'check_constraints', []):
                context = record_data.copy()
                if not self._evaluate_condition(check_expr, context):
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
                            raise ValueError(f"外键约束不满足: {column.name} -> {ref_table}({ref_column})")
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
            if stmt.where_clause is None or self._evaluate_condition(stmt.where_clause, record.data):
                filtered_records.append(record)

        # 检查是否有聚合函数
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
                            # 只统计非NULL
                            agg_result["COUNT"] = sum(1 for r in filtered_records if self._evaluate_expression(arg, r.data) is not None)
                        else:
                            raise ValueError("COUNT参数不支持")
                    elif func == "SUM":
                        if isinstance(arg, ColumnRef):
                            values = [self._evaluate_expression(arg, r.data) for r in filtered_records]
                            agg_result["SUM"] = sum(v for v in values if v is not None)
                        else:
                            raise ValueError("SUM参数不支持")
                    elif func == "AVG":
                        if isinstance(arg, ColumnRef):
                            values = [self._evaluate_expression(arg, r.data) for r in filtered_records if self._evaluate_expression(arg, r.data) is not None]
                            agg_result["AVG"] = sum(values) / len(values) if values else None
                        else:
                            raise ValueError("AVG参数不支持")
                    elif func == "MIN":
                        if isinstance(arg, ColumnRef):
                            values = [self._evaluate_expression(arg, r.data) for r in filtered_records if self._evaluate_expression(arg, r.data) is not None]
                            agg_result["MIN"] = min(values) if values else None
                        else:
                            raise ValueError("MIN参数不支持")
                    elif func == "MAX":
                        if isinstance(arg, ColumnRef):
                            values = [self._evaluate_expression(arg, r.data) for r in filtered_records if self._evaluate_expression(arg, r.data) is not None]
                            agg_result["MAX"] = max(values) if values else None
                        else:
                            raise ValueError("MAX参数不支持")
                    else:
                        raise ValueError(f"不支持的聚合函数: {func}")
            result_records = [agg_result]
        else:
            # 选择列
            result_records = []
            for record in filtered_records:
                if stmt.columns == ["*"]:
                    # 返回所有表结构定义的字段
                    row = {}
                    schema = self.catalog.get_table_schema(from_name if isinstance(from_name, str) else stmt.from_table)
                    for col in schema.columns:
                        row[col.name] = record.data.get(col.name)
                    result_records.append(row)
                else:
                    selected_data = {}
                    for col in stmt.columns:
                        if isinstance(col, ColumnRef):
                            if col.table_name:
                                key = f"{col.table_name}.{col.column_name}"
                                if key in record.data:
                                    selected_data[col.column_name] = record.data[key]
                                else:
                                    raise ValueError(f"列 {key} 不存在")
                            else:
                                matches = [k for k in record.data if k.endswith(f".{col.column_name}") or k == col.column_name]
                                if len(matches) == 1:
                                    key = matches[0]
                                elif len(matches) == 0:
                                    raise ValueError(f"列 {col.column_name} 不存在")
                                else:
                                    raise ValueError(f"列 {col.column_name} 不明确，请加表前缀")
                                if key in record.data:
                                    selected_data[col.column_name] = record.data[key]
                                else:
                                    raise ValueError(f"列 {key} 不存在")
                        elif isinstance(col, AggregateFunction):
                            # 聚合函数在非聚合上下文不支持
                            raise ValueError("聚合函数只能单独出现在SELECT列表中")
                        else:
                            if col in record.data:
                                selected_data[col] = record.data[col]
                            else:
                                raise ValueError(f"列 {col} 不存在")
                    result_records.append(selected_data)

        return {
            "type": "SELECT",
            "table_name": from_name,
            "rows_returned": len(result_records),
            "data": result_records,
            "success": True,
            "message": f"查询返回 {len(result_records)} 行",
        }

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
                print(f"DEBUG: 构建索引 {index_name}: {key} -> {record_id}")

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

        print(f"DEBUG: 使用索引 {index_name} 查找 {column_name} {operator} {value}")

        if operator == "=":
            # 精确查找
            record_id = btree.search(value)
            print(f"DEBUG: 索引搜索结果: {record_id}")

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

        print(f"DEBUG: 根据ID获取到 {len(result)} 条记录")
        return result

    def _evaluate_expression(self, expr: Expression, context: Dict[str, Any]) -> Any:
        if isinstance(expr, Literal):
            return expr.value
        elif isinstance(expr, ColumnRef):
            if expr.table_name:
                # 优先查找带前缀
                key = f"{expr.table_name}.{expr.column_name}"
                if key in context:
                    return context[key]
            # 回退查找无前缀
            return context.get(expr.column_name)
        else:
            raise ValueError(f"不支持的表达式类型: {type(expr)}")

    def _evaluate_condition(
        self, condition: Expression, record_data: Dict[str, Any]
    ) -> bool:
        """评估WHERE条件"""
        if isinstance(condition, BinaryOp):
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
