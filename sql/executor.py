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
        self.index_manager = index_manager  # 添加这行

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
        elif isinstance(ast, CreateIndexStatement):  # 新增
            return self._execute_create_index(ast)
        elif isinstance(ast, DropIndexStatement):  # 新增
            return self._execute_drop_index(ast)
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
            }

            data_type = data_type_map.get(col_def["type"])
            if not data_type:
                raise ValueError(f"不支持的数据类型: {col_def['type']}")

            # 解析约束
            nullable = True
            primary_key = False

            for constraint in col_def["constraints"]:
                if constraint == "NOT NULL":
                    nullable = False
                elif constraint == "PRIMARY KEY":
                    primary_key = True
                    nullable = False  # 主键默认不为空

            column = ColumnDefinition(
                name=col_def["name"],
                data_type=data_type,
                max_length=col_def["length"],
                nullable=nullable,
                primary_key=primary_key,
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

    # def _execute_insert(self, stmt: InsertStatement) -> Dict[str, Any]:
    #     """执行INSERT"""
    #     schema = self.catalog.get_table_schema(stmt.table_name)
    #     if not schema:
    #         raise ValueError(f"表 {stmt.table_name} 不存在")

    #     inserted_count = 0

    #     for value_row in stmt.values:
    #         # 构建记录数据
    #         record_data = {}

    #         if stmt.columns:
    #             # 指定了列名
    #             if len(stmt.columns) != len(value_row):
    #                 raise ValueError("列数和值数不匹配")

    #             for i, col_name in enumerate(stmt.columns):
    #                 value = self._evaluate_expression(value_row[i], {})
    #                 record_data[col_name] = value
    #         else:
    #             # 未指定列名，按顺序插入所有列
    #             if len(value_row) != len(schema.columns):
    #                 raise ValueError("值数和表列数不匹配")

    #             for i, column in enumerate(schema.columns):
    #                 value = self._evaluate_expression(value_row[i], {})
    #                 record_data[column.name] = value

    #         # 插入记录
    #         try:
    #             self.table_manager.insert_record(stmt.table_name, record_data)
    #             inserted_count += 1
    #         except Exception as e:
    #             raise ValueError(f"插入记录失败: {str(e)}")

    #     return {
    #         "type": "INSERT",
    #         "table_name": stmt.table_name,
    #         "rows_inserted": inserted_count,
    #         "success": True,
    #         "message": f"成功插入 {inserted_count} 行到表 {stmt.table_name}",
    #     }

    # def _execute_select(self, stmt: SelectStatement) -> Dict[str, Any]:
    #     """执行SELECT"""
    #     schema = self.catalog.get_table_schema(stmt.from_table)
    #     if not schema:
    #         raise ValueError(f"表 {stmt.from_table} 不存在")

    #     # 获取所有记录
    #     all_records = self.table_manager.scan_table(stmt.from_table)

    #     # 应用WHERE条件过滤
    #     filtered_records = []
    #     for record in all_records:
    #         if stmt.where_clause is None or self._evaluate_condition(
    #             stmt.where_clause, record.data
    #         ):
    #             filtered_records.append(record)

    #     # 选择列
    #     result_records = []
    #     for record in filtered_records:
    #         if stmt.columns == ["*"]:
    #             # 选择所有列
    #             result_records.append(dict(record.data))
    #         else:
    #             # 选择指定列
    #             selected_data = {}
    #             for col in stmt.columns:
    #                 if isinstance(col, ColumnRef):
    #                     col_name = col.column_name
    #                     if col_name in record.data:
    #                         selected_data[col_name] = record.data[col_name]
    #                     else:
    #                         raise ValueError(f"列 {col_name} 不存在")
    #                 else:
    #                     # 字符串形式的列名
    #                     if col in record.data:
    #                         selected_data[col] = record.data[col]
    #                     else:
    #                         raise ValueError(f"列 {col} 不存在")
    #             result_records.append(selected_data)

    #     return {
    #         "type": "SELECT",
    #         "table_name": stmt.from_table,
    #         "rows_returned": len(result_records),
    #         "data": result_records,
    #         "success": True,
    #         "message": f"查询返回 {len(result_records)} 行",
    #     }

    def _execute_insert(self, stmt: InsertStatement) -> Dict[str, Any]:
        """执行INSERT - 修改版，支持索引维护"""
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

    def _execute_select(self, stmt: SelectStatement) -> Dict[str, Any]:
        """执行SELECT - 修改版，支持索引优化"""
        schema = self.catalog.get_table_schema(stmt.from_table)
        if not schema:
            raise ValueError(f"表 {stmt.from_table} 不存在")

        # 新增：尝试使用索引优化查询
        if self.index_manager and stmt.where_clause:
            optimized_records = self._try_index_scan(stmt.from_table, stmt.where_clause)
            if optimized_records is not None:
                # 成功使用索引扫描
                all_records = optimized_records
            else:
                # 回退到全表扫描
                all_records = self.table_manager.scan_table(stmt.from_table)
        else:
            # 全表扫描
            all_records = self.table_manager.scan_table(stmt.from_table)

        # 应用WHERE条件过滤（如果没有被索引优化处理）
        filtered_records = []
        for record in all_records:
            if stmt.where_clause is None or self._evaluate_condition(
                stmt.where_clause, record.data
            ):
                filtered_records.append(record)

        # 选择列（保持原有逻辑）
        result_records = []
        for record in filtered_records:
            if stmt.columns == ["*"]:
                result_records.append(dict(record.data))
            else:
                selected_data = {}
                for col in stmt.columns:
                    if isinstance(col, ColumnRef):
                        col_name = col.column_name
                        if col_name in record.data:
                            selected_data[col_name] = record.data[col_name]
                        else:
                            raise ValueError(f"列 {col_name} 不存在")
                    else:
                        if col in record.data:
                            selected_data[col] = record.data[col]
                        else:
                            raise ValueError(f"列 {col} 不存在")
                result_records.append(selected_data)

        return {
            "type": "SELECT",
            "table_name": stmt.from_table,
            "rows_returned": len(result_records),
            "data": result_records,
            "success": True,
            "message": f"查询返回 {len(result_records)} 行",
        }

    # 新增方法：索引相关操作

    def _execute_create_index(self, stmt: CreateIndexStatement) -> Dict[str, Any]:
        """执行CREATE INDEX"""
        if not self.index_manager:
            raise ValueError("索引管理器未初始化")

        # 验证表和列是否存在
        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise ValueError(f"表 {stmt.table_name} 不存在")

        column_exists = any(col.name == stmt.column_name for col in schema.columns)
        if not column_exists:
            raise ValueError(f"列 {stmt.column_name} 不存在于表 {stmt.table_name}")

        # 创建索引
        success = self.index_manager.create_index(
            stmt.index_name, stmt.table_name, stmt.column_name, stmt.is_unique
        )

        if not success:
            raise ValueError(f"索引 {stmt.index_name} 已存在")

        # 为现有数据构建索引
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
        """为现有数据构建索引"""
        btree = self.index_manager.get_index(index_name)
        if not btree:
            return

        # 扫描表中所有记录
        all_records = self.table_manager.scan_table(table_name)

        for i, record in enumerate(all_records):
            if column_name in record.data:
                key = record.data[column_name]
                btree.insert(key, i)  # 使用记录索引作为值

    def _try_index_scan(
        self, table_name: str, where_clause: Expression
    ) -> Optional[List[Record]]:
        """尝试使用索引扫描优化查询"""
        if not self.index_manager:
            return None

        index_info = self._analyze_where_for_index(table_name, where_clause)
        if not index_info:
            return None

        index_name, column_name, operator, value = index_info
        btree = self.index_manager.get_index(index_name)
        if not btree:
            return None

        # 根据操作符类型执行不同的索引扫描
        if operator == "=":
            # 精确查找
            record_ids = btree.search(value)
            if record_ids is not None:
                # 修改这里：record_ids 可能是单个值，需要转换为列表
                if isinstance(record_ids, list):
                    return self._get_records_by_ids(table_name, record_ids)
                else:
                    return self._get_records_by_ids(table_name, [record_ids])
        elif operator in ["<", "<=", ">", ">="]:
            # 范围查询（简化实现）
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

        # 只处理简单的列=值条件
        if isinstance(where_clause.left, ColumnRef) and isinstance(
            where_clause.right, Literal
        ):

            column_name = where_clause.left.column_name
            operator = where_clause.operator
            value = where_clause.right.value

            # 检查是否有该列的索引
            available_indexes = self.index_manager.get_table_indexes(table_name)
            for index_name in available_indexes:
                index_info = self.index_manager.indexes.get(index_name)
                if index_info and index_info.column_name == column_name:
                    return (index_name, column_name, operator, value)

        return None

    def _get_records_by_ids(
        self, table_name: str, record_ids: List[int]
    ) -> List[Record]:
        """根据记录ID获取记录（简化实现）"""
        # 这里需要根据你的存储实现来获取特定ID的记录
        # 暂时回退到全表扫描然后过滤
        all_records = self.table_manager.scan_table(table_name)
        return [record for i, record in enumerate(all_records) if i in record_ids]

    def _evaluate_expression(self, expr: Expression, context: Dict[str, Any]) -> Any:
        """计算表达式的值"""
        if isinstance(expr, Literal):
            return expr.value
        elif isinstance(expr, ColumnRef):
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
