"""
执行引擎 - 负责执行计划的物理执行
实现算子：CreateTable、Insert、SeqScan、Filter、Project
"""

from typing import List, Dict, Any, Iterator, Optional
from storage import Record
from catalog import SystemCatalog, ColumnDefinition, DataType
from table import TableManager
from .plan_nodes import *
from .ast_nodes import *


class ExecutionEngine:
    """执行引擎 - 专门负责执行计划的物理执行"""

    def __init__(self, table_manager: TableManager, catalog: SystemCatalog, index_manager=None):
        self.table_manager = table_manager
        self.catalog = catalog
        self.index_manager = index_manager

    def execute_plan(self, plan: PlanNode) -> Dict[str, Any]:
        """执行执行计划"""
        try:
            if isinstance(plan,
                          (SeqScanNode, IndexScanNode, FilterNode, ProjectNode, JoinNode, SortNode, AggregateNode)):
                # SELECT查询类计划
                result_iterator = self._execute_operator(plan)
                results = list(result_iterator)

                return {
                    "success": True,
                    "type": "SELECT",
                    "data": results,
                    "rows_returned": len(results),
                    "estimated_cost": plan.estimated_cost,
                    "estimated_rows": plan.estimated_rows,
                    "message": f"执行计划完成，返回 {len(results)} 行"
                }
            else:
                # DDL/DML操作
                return self._execute_ddl_dml_operator(plan)

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": f"执行计划执行失败: {str(e)}"
            }

    def _execute_operator(self, node: PlanNode) -> Iterator[Dict[str, Any]]:
        """递归执行算子"""
        if isinstance(node, SeqScanNode):
            yield from self._execute_seq_scan(node)
        elif isinstance(node, IndexScanNode):
            yield from self._execute_index_scan(node)
        elif isinstance(node, FilterNode):
            yield from self._execute_filter(node)
        elif isinstance(node, ProjectNode):
            yield from self._execute_project(node)
        elif isinstance(node, JoinNode):
            yield from self._execute_join(node)
        elif isinstance(node, SortNode):
            yield from self._execute_sort(node)
        elif isinstance(node, AggregateNode):
            yield from self._execute_aggregate(node)
        else:
            raise ValueError(f"不支持的算子类型: {type(node).__name__}")

    def _execute_seq_scan(self, node: SeqScanNode) -> Iterator[Dict[str, Any]]:
        """执行顺序扫描算子"""
        print(f"[ExecutionEngine] 执行SeqScan算子: table={node.table_name}")

        # 从表管理器获取所有记录
        records = self.table_manager.scan_table(node.table_name)

        for record in records:
            # 应用过滤条件（如果有）
            if node.filter_condition:
                if self._evaluate_condition(node.filter_condition, record.data):
                    yield record.data
            else:
                yield record.data

    def _execute_index_scan(self, node: IndexScanNode) -> Iterator[Dict[str, Any]]:
        """执行索引扫描算子"""
        print(f"[ExecutionEngine] 执行IndexScan算子: table={node.table_name}, index={node.index_name}")

        if not self.index_manager:
            raise ValueError("索引管理器未初始化")

        btree = self.index_manager.get_index(node.index_name)
        if not btree:
            raise ValueError(f"索引 {node.index_name} 不存在")

        # 根据扫描条件执行索引查询
        if isinstance(node.scan_condition, BinaryOp):
            key = self._evaluate_expression(node.scan_condition.right, {})

            if node.scan_condition.operator == "=":
                # 精确查找
                record_id = btree.search(key)
                if record_id is not None:
                    record = self._get_record_by_id(node.table_name, record_id)
                    if record:
                        yield record.data
            elif node.scan_condition.operator in ["<", "<=", ">", ">="]:
                # 范围查询
                if node.scan_condition.operator in ["<", "<="]:
                    results = btree.range_search(float("-inf"), key)
                else:
                    results = btree.range_search(key, float("inf"))

                for _, record_id in results:
                    record = self._get_record_by_id(node.table_name, record_id)
                    if record:
                        yield record.data

    def _execute_filter(self, node: FilterNode) -> Iterator[Dict[str, Any]]:
        """执行过滤算子"""
        print(f"[ExecutionEngine] 执行Filter算子: condition={node.condition}")

        child_iterator = self._execute_operator(node.children[0])

        for row in child_iterator:
            if self._evaluate_condition(node.condition, row):
                yield row

    def _execute_project(self, node: ProjectNode) -> Iterator[Dict[str, Any]]:
        """执行投影算子"""
        print(f"[ExecutionEngine] 执行Project算子: select_list={node.select_list}")

        child_iterator = self._execute_operator(node.children[0])

        for row in child_iterator:
            projected_row = {}

            for col in node.select_list:
                if isinstance(col, str):
                    if col == "*":
                        # 返回所有列
                        projected_row.update(row)
                    else:
                        projected_row[col] = row.get(col)
                elif isinstance(col, ColumnRef):
                    projected_row[col.column_name] = row.get(col.column_name)
                else:
                    # 表达式（简化处理）
                    projected_row[str(col)] = self._evaluate_expression(col, row)

            yield projected_row

    def _execute_join(self, node: JoinNode) -> Iterator[Dict[str, Any]]:
        """执行连接算子（嵌套循环连接）"""
        print(f"[ExecutionEngine] 执行Join算子: type={node.join_type}")

        left_iterator = self._execute_operator(node.children[0])

        for left_row in left_iterator:
            right_iterator = self._execute_operator(node.children[1])

            for right_row in right_iterator:
                # 合并左右行
                merged_row = {}
                merged_row.update(left_row)
                merged_row.update(right_row)

                # 评估连接条件
                if self._evaluate_condition(node.join_condition, merged_row):
                    yield merged_row

    def _execute_sort(self, node: SortNode) -> Iterator[Dict[str, Any]]:
        """执行排序算子"""
        print(f"[ExecutionEngine] 执行Sort算子: order_by={node.order_by}")

        child_iterator = self._execute_operator(node.children[0])
        rows = list(child_iterator)  # 需要先收集所有行

        # 构建排序键
        def sort_key(row):
            keys = []
            for order_item in node.order_by:
                col_name = order_item["column"]
                keys.append(row.get(col_name))
            return tuple(keys)

        # 排序
        reverse = any(item.get("direction", "ASC").upper() == "DESC"
                      for item in node.order_by)
        rows.sort(key=sort_key, reverse=reverse)

        for row in rows:
            yield row

    def _execute_aggregate(self, node: AggregateNode) -> Iterator[Dict[str, Any]]:
        """执行聚合算子"""
        print(f"[ExecutionEngine] 执行Aggregate算子: group_by={node.group_by}")

        child_iterator = self._execute_operator(node.children[0])
        rows = list(child_iterator)

        if node.group_by:
            # 分组聚合
            groups = {}
            for row in rows:
                key = tuple(row.get(col) for col in node.group_by)
                if key not in groups:
                    groups[key] = []
                groups[key].append(row)

            for key, group_rows in groups.items():
                result_row = {}
                # 添加分组列
                for i, col in enumerate(node.group_by):
                    result_row[col] = key[i]

                # 计算聚合函数
                for agg_func in node.aggregate_functions:
                    result_row[agg_func["alias"]] = self._calculate_aggregate(
                        agg_func, group_rows
                    )

                yield result_row
        else:
            # 全表聚合
            result_row = {}
            for agg_func in node.aggregate_functions:
                result_row[agg_func["alias"]] = self._calculate_aggregate(
                    agg_func, rows
                )
            yield result_row

    def _execute_ddl_dml_operator(self, node: PlanNode) -> Dict[str, Any]:
        """执行DDL/DML算子"""
        if isinstance(node, CreateTableNode):
            return self._execute_create_table_operator(node)
        elif isinstance(node, InsertNode):
            return self._execute_insert_operator(node)
        elif isinstance(node, UpdateNode):
            return self._execute_update_operator(node)
        elif isinstance(node, DeleteNode):
            return self._execute_delete_operator(node)
        else:
            raise ValueError(f"不支持的DDL/DML算子: {type(node).__name__}")

    def _execute_create_table_operator(self, node: CreateTableNode) -> Dict[str, Any]:
        """执行建表算子"""
        print(f"[ExecutionEngine] 执行CreateTable算子: table={node.table_name}")

        columns = []
        for col_def in node.columns:
            # 转换列定义
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

            column = ColumnDefinition(
                name=col_def["name"],
                data_type=data_type,
                max_length=col_def.get("length"),
                nullable=col_def.get("nullable", True)
            )
            columns.append(column)

        self.table_manager.create_table(node.table_name, columns)

        return {
            "type": "CREATE_TABLE",
            "success": True,
            "table_name": node.table_name,
            "estimated_cost": node.estimated_cost,
            "message": f"表 {node.table_name} 创建成功"
        }

    def _execute_insert_operator(self, node: InsertNode) -> Dict[str, Any]:
        """执行插入算子"""
        print(f"[ExecutionEngine] 执行Insert算子: table={node.table_name}")

        inserted_count = 0

        for values in node.values:
            record_data = {}
            for i, col_name in enumerate(node.columns):
                record_data[col_name] = values[i]

            record_id = self.table_manager.insert_record(node.table_name, record_data)

            # 更新索引
            if self.index_manager:
                self.index_manager.insert_into_indexes(
                    node.table_name, record_data, record_id
                )

            inserted_count += 1

        return {
            "type": "INSERT",
            "success": True,
            "rows_inserted": inserted_count,
            "estimated_cost": node.estimated_cost,
            "message": f"成功插入 {inserted_count} 行"
        }

    def _get_record_by_id(self, table_name: str, record_id: Any) -> Optional[Record]:
        """根据记录ID获取记录"""
        if isinstance(record_id, tuple) and len(record_id) == 2:
            page_id, slot_id = record_id

            # 使用table_manager的位置访问方法
            for p_id, s_id, record in self.table_manager.scan_table_with_locations(table_name):
                if p_id == page_id and s_id == slot_id:
                    return record
            return None
        else:
            # 兼容旧的记录ID格式
            all_records = self.table_manager.scan_table(table_name)
            if isinstance(record_id, int) and 0 <= record_id < len(all_records):
                return all_records[record_id]
        return None

    def _evaluate_condition(self, condition: Expression, context: Dict[str, Any]) -> bool:
        """评估条件表达式"""
        if isinstance(condition, BinaryOp):
            left_val = self._evaluate_expression(condition.left, context)
            right_val = self._evaluate_expression(condition.right, context)

            if condition.operator == "=":
                return left_val == right_val
            elif condition.operator == "!=":
                return left_val != right_val
            elif condition.operator == "<":
                return left_val < right_val
            elif condition.operator == "<=":
                return left_val <= right_val
            elif condition.operator == ">":
                return left_val > right_val
            elif condition.operator == ">=":
                return left_val >= right_val
            else:
                raise ValueError(f"不支持的操作符: {condition.operator}")
        elif isinstance(condition, LogicalOp):
            left_result = self._evaluate_condition(condition.left, context)
            right_result = self._evaluate_condition(condition.right, context)

            if condition.operator == "AND":
                return left_result and right_result
            elif condition.operator == "OR":
                return left_result or right_result
            else:
                raise ValueError(f"不支持的逻辑操作符: {condition.operator}")

        return True

    def _evaluate_expression(self, expr: Expression, context: Dict[str, Any]) -> Any:
        """评估表达式"""
        if isinstance(expr, Literal):
            return expr.value
        elif isinstance(expr, ColumnRef):
            return context.get(expr.column_name)
        else:
            raise ValueError(f"不支持的表达式类型: {type(expr)}")

    def _calculate_aggregate(self, agg_func: Dict[str, Any], rows: List[Dict[str, Any]]) -> Any:
        """计算聚合函数"""
        func_name = agg_func["function"].upper()
        column = agg_func["column"]

        if func_name == "COUNT":
            if column == "*":
                return len(rows)
            else:
                return len([row for row in rows if row.get(column) is not None])
        elif func_name == "SUM":
            values = [row.get(column) for row in rows if row.get(column) is not None]
            return sum(values) if values else 0
        elif func_name == "AVG":
            values = [row.get(column) for row in rows if row.get(column) is not None]
            return sum(values) / len(values) if values else None
        elif func_name == "MIN":
            values = [row.get(column) for row in rows if row.get(column) is not None]
            return min(values) if values else None
        elif func_name == "MAX":
            values = [row.get(column) for row in rows if row.get(column) is not None]
            return max(values) if values else None
        else:
            raise ValueError(f"不支持的聚合函数: {func_name}")