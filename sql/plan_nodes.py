# sql/plan_nodes.py
"""
执行计划节点定义
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union
from .ast_nodes import Expression


class PlanNode(ABC):
    """执行计划节点基类"""

    def __init__(self):
        self.output_schema: List[str] = []  # 输出列名
        self.estimated_rows: int = 0
        self.estimated_cost: float = 0.0
        self.children: List['PlanNode'] = []

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        pass

    @abstractmethod
    def to_tree_string(self, indent: int = 0) -> str:
        """转换为树形字符串"""
        pass


class SeqScanNode(PlanNode):
    """顺序扫描节点"""

    def __init__(self, table_name: str, filter_condition: Optional[Expression] = None):
        super().__init__()
        self.table_name = table_name
        self.filter_condition = filter_condition
        self.node_type = "SeqScan"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_type": self.node_type,
            "table_name": self.table_name,
            "filter": str(self.filter_condition) if self.filter_condition else None,
            "estimated_rows": self.estimated_rows,
            "estimated_cost": self.estimated_cost,
            "output_schema": self.output_schema
        }

    def to_tree_string(self, indent: int = 0) -> str:
        prefix = "  " * indent
        result = f"{prefix}SeqScan on {self.table_name}\n"
        if self.filter_condition:
            result += f"{prefix}  Filter: {self.filter_condition}\n"
        result += f"{prefix}  Estimated rows: {self.estimated_rows}\n"
        result += f"{prefix}  Estimated cost: {self.estimated_cost:.2f}\n"
        return result


class IndexScanNode(PlanNode):
    """索引扫描节点"""

    def __init__(self, table_name: str, index_name: str, scan_condition: Expression):
        super().__init__()
        self.table_name = table_name
        self.index_name = index_name
        self.scan_condition = scan_condition
        self.node_type = "IndexScan"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_type": self.node_type,
            "table_name": self.table_name,
            "index_name": self.index_name,
            "scan_condition": str(self.scan_condition),
            "estimated_rows": self.estimated_rows,
            "estimated_cost": self.estimated_cost,
            "output_schema": self.output_schema
        }

    def to_tree_string(self, indent: int = 0) -> str:
        prefix = "  " * indent
        result = f"{prefix}IndexScan on {self.table_name} using {self.index_name}\n"
        result += f"{prefix}  Index condition: {self.scan_condition}\n"
        result += f"{prefix}  Estimated rows: {self.estimated_rows}\n"
        result += f"{prefix}  Estimated cost: {self.estimated_cost:.2f}\n"
        return result


class FilterNode(PlanNode):
    """过滤节点"""

    def __init__(self, child: PlanNode, condition: Expression):
        super().__init__()
        self.children = [child]
        self.condition = condition
        self.node_type = "Filter"
        self.output_schema = child.output_schema

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_type": self.node_type,
            "condition": str(self.condition),
            "estimated_rows": self.estimated_rows,
            "estimated_cost": self.estimated_cost,
            "output_schema": self.output_schema,
            "children": [child.to_dict() for child in self.children]
        }

    def to_tree_string(self, indent: int = 0) -> str:
        prefix = "  " * indent
        result = f"{prefix}Filter\n"
        result += f"{prefix}  Condition: {self.condition}\n"
        result += f"{prefix}  Estimated rows: {self.estimated_rows}\n"
        result += f"{prefix}  Estimated cost: {self.estimated_cost:.2f}\n"
        for child in self.children:
            result += child.to_tree_string(indent + 1)
        return result


class ProjectNode(PlanNode):
    """投影节点"""

    def __init__(self, child: PlanNode, select_list: List[Union[str, Expression]]):
        super().__init__()
        self.children = [child]
        self.select_list = select_list
        self.node_type = "Project"
        # 构建输出模式
        self.output_schema = []
        for item in select_list:
            if isinstance(item, str):
                self.output_schema.append(item)
            else:
                self.output_schema.append(str(item))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_type": self.node_type,
            "select_list": [str(item) for item in self.select_list],
            "estimated_rows": self.estimated_rows,
            "estimated_cost": self.estimated_cost,
            "output_schema": self.output_schema,
            "children": [child.to_dict() for child in self.children]
        }

    def to_tree_string(self, indent: int = 0) -> str:
        prefix = "  " * indent
        result = f"{prefix}Project\n"
        result += f"{prefix}  Select list: {[str(item) for item in self.select_list]}\n"
        result += f"{prefix}  Estimated rows: {self.estimated_rows}\n"
        result += f"{prefix}  Estimated cost: {self.estimated_cost:.2f}\n"
        for child in self.children:
            result += child.to_tree_string(indent + 1)
        return result


class JoinNode(PlanNode):
    """连接节点"""

    def __init__(self, left_child: PlanNode, right_child: PlanNode,
                 join_type: str, join_condition: Expression):
        super().__init__()
        self.children = [left_child, right_child]
        self.join_type = join_type
        self.join_condition = join_condition
        self.node_type = "Join"
        # 合并输出模式
        self.output_schema = left_child.output_schema + right_child.output_schema

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_type": self.node_type,
            "join_type": self.join_type,
            "join_condition": str(self.join_condition),
            "estimated_rows": self.estimated_rows,
            "estimated_cost": self.estimated_cost,
            "output_schema": self.output_schema,
            "children": [child.to_dict() for child in self.children]
        }

    def to_tree_string(self, indent: int = 0) -> str:
        prefix = "  " * indent
        result = f"{prefix}{self.join_type} Join\n"
        result += f"{prefix}  Join condition: {self.join_condition}\n"
        result += f"{prefix}  Estimated rows: {self.estimated_rows}\n"
        result += f"{prefix}  Estimated cost: {self.estimated_cost:.2f}\n"
        for child in self.children:
            result += child.to_tree_string(indent + 1)
        return result


class SortNode(PlanNode):
    """排序节点"""

    def __init__(self, child: PlanNode, order_by: List[Dict[str, Any]]):
        super().__init__()
        self.children = [child]
        self.order_by = order_by
        self.node_type = "Sort"
        self.output_schema = child.output_schema

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_type": self.node_type,
            "order_by": self.order_by,
            "estimated_rows": self.estimated_rows,
            "estimated_cost": self.estimated_cost,
            "output_schema": self.output_schema,
            "children": [child.to_dict() for child in self.children]
        }

    def to_tree_string(self, indent: int = 0) -> str:
        prefix = "  " * indent
        result = f"{prefix}Sort\n"
        result += f"{prefix}  Order by: {self.order_by}\n"
        result += f"{prefix}  Estimated rows: {self.estimated_rows}\n"
        result += f"{prefix}  Estimated cost: {self.estimated_cost:.2f}\n"
        for child in self.children:
            result += child.to_tree_string(indent + 1)
        return result


class AggregateNode(PlanNode):
    """聚合节点"""

    def __init__(self, child: PlanNode, group_by: List[str] = None,
                 aggregate_functions: List[Dict[str, Any]] = None):
        super().__init__()
        self.children = [child]
        self.group_by = group_by or []
        self.aggregate_functions = aggregate_functions or []
        self.node_type = "Aggregate"
        # 构建输出模式
        self.output_schema = self.group_by.copy()
        for agg in self.aggregate_functions:
            self.output_schema.append(agg.get("alias", f"{agg['function']}({agg['column']})"))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_type": self.node_type,
            "group_by": self.group_by,
            "aggregate_functions": self.aggregate_functions,
            "estimated_rows": self.estimated_rows,
            "estimated_cost": self.estimated_cost,
            "output_schema": self.output_schema,
            "children": [child.to_dict() for child in self.children]
        }

    def to_tree_string(self, indent: int = 0) -> str:
        prefix = "  " * indent
        result = f"{prefix}Aggregate\n"
        if self.group_by:
            result += f"{prefix}  Group by: {self.group_by}\n"
        if self.aggregate_functions:
            result += f"{prefix}  Aggregates: {self.aggregate_functions}\n"
        result += f"{prefix}  Estimated rows: {self.estimated_rows}\n"
        result += f"{prefix}  Estimated cost: {self.estimated_cost:.2f}\n"
        for child in self.children:
            result += child.to_tree_string(indent + 1)
        return result


class InsertNode(PlanNode):
    """插入节点"""

    def __init__(self, table_name: str, columns: List[str], values: List[List[Any]]):
        super().__init__()
        self.table_name = table_name
        self.columns = columns
        self.values = values
        self.node_type = "Insert"
        self.estimated_rows = len(values)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_type": self.node_type,
            "table_name": self.table_name,
            "columns": self.columns,
            "values": self.values,
            "estimated_rows": self.estimated_rows,
            "estimated_cost": self.estimated_cost
        }

    def to_tree_string(self, indent: int = 0) -> str:
        prefix = "  " * indent
        result = f"{prefix}Insert into {self.table_name}\n"
        result += f"{prefix}  Columns: {self.columns}\n"
        result += f"{prefix}  Values count: {len(self.values)}\n"
        result += f"{prefix}  Estimated cost: {self.estimated_cost:.2f}\n"
        return result


class UpdateNode(PlanNode):
    """更新节点"""

    def __init__(self, table_name: str, set_clauses: List[Dict[str, Any]],
                 child: Optional[PlanNode] = None):
        super().__init__()
        self.table_name = table_name
        self.set_clauses = set_clauses
        if child:
            self.children = [child]
        self.node_type = "Update"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_type": self.node_type,
            "table_name": self.table_name,
            "set_clauses": self.set_clauses,
            "estimated_rows": self.estimated_rows,
            "estimated_cost": self.estimated_cost,
            "children": [child.to_dict() for child in self.children]
        }

    def to_tree_string(self, indent: int = 0) -> str:
        prefix = "  " * indent
        result = f"{prefix}Update {self.table_name}\n"
        result += f"{prefix}  Set: {self.set_clauses}\n"
        result += f"{prefix}  Estimated rows: {self.estimated_rows}\n"
        result += f"{prefix}  Estimated cost: {self.estimated_cost:.2f}\n"
        for child in self.children:
            result += child.to_tree_string(indent + 1)
        return result


class DeleteNode(PlanNode):
    """删除节点"""

    def __init__(self, table_name: str, child: Optional[PlanNode] = None):
        super().__init__()
        self.table_name = table_name
        if child:
            self.children = [child]
        self.node_type = "Delete"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_type": self.node_type,
            "table_name": self.table_name,
            "estimated_rows": self.estimated_rows,
            "estimated_cost": self.estimated_cost,
            "children": [child.to_dict() for child in self.children]
        }

    def to_tree_string(self, indent: int = 0) -> str:
        prefix = "  " * indent
        result = f"{prefix}Delete from {self.table_name}\n"
        result += f"{prefix}  Estimated rows: {self.estimated_rows}\n"
        result += f"{prefix}  Estimated cost: {self.estimated_cost:.2f}\n"
        for child in self.children:
            result += child.to_tree_string(indent + 1)
        return result


class CreateTableNode(PlanNode):
    """建表节点"""

    def __init__(self, table_name: str, columns: List[Dict[str, Any]]):
        super().__init__()
        self.table_name = table_name
        self.columns = columns
        self.node_type = "CreateTable"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_type": self.node_type,
            "table_name": self.table_name,
            "columns": self.columns,
            "estimated_cost": self.estimated_cost
        }

    def to_tree_string(self, indent: int = 0) -> str:
        prefix = "  " * indent
        result = f"{prefix}Create Table {self.table_name}\n"
        result += f"{prefix}  Columns: {len(self.columns)}\n"
        result += f"{prefix}  Estimated cost: {self.estimated_cost:.2f}\n"
        return result