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

    def __init__(self, table_manager: TableManager, catalog: SystemCatalog):
        self.table_manager = table_manager
        self.catalog = catalog

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

    def _execute_insert(self, stmt: InsertStatement) -> Dict[str, Any]:
        """执行INSERT"""
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
                self.table_manager.insert_record(stmt.table_name, record_data)
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
        """执行SELECT"""
        schema = self.catalog.get_table_schema(stmt.from_table)
        if not schema:
            raise ValueError(f"表 {stmt.from_table} 不存在")

        # 获取所有记录
        all_records = self.table_manager.scan_table(stmt.from_table)

        # 应用WHERE条件过滤
        filtered_records = []
        for record in all_records:
            if stmt.where_clause is None or self._evaluate_condition(
                stmt.where_clause, record.data
            ):
                filtered_records.append(record)

        # 选择列
        result_records = []
        for record in filtered_records:
            if stmt.columns == ["*"]:
                # 选择所有列
                result_records.append(dict(record.data))
            else:
                # 选择指定列
                selected_data = {}
                for col in stmt.columns:
                    if isinstance(col, ColumnRef):
                        col_name = col.column_name
                        if col_name in record.data:
                            selected_data[col_name] = record.data[col_name]
                        else:
                            raise ValueError(f"列 {col_name} 不存在")
                    else:
                        # 字符串形式的列名
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
