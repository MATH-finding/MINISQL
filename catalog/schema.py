"""
表结构定义
"""

from typing import List, Dict, Any, Optional
from .data_types import ColumnDefinition


class TableSchema:
    """表结构定义"""

    def __init__(self, table_name: str, columns: List[ColumnDefinition], check_constraints=None, foreign_keys=None):
        self.table_name = table_name
        self.columns = columns
        self.column_map = {col.name: i for i, col in enumerate(columns)}
        self.primary_key_columns = [col.name for col in columns if col.primary_key]
        self.check_constraints = check_constraints or []
        self.foreign_keys = foreign_keys or []

    def get_column(self, column_name: str) -> Optional[ColumnDefinition]:
        """获取列定义"""
        index = self.column_map.get(column_name)
        return self.columns[index] if index is not None else None

    def validate_record(self, record_data: Dict[str, Any]) -> bool:
        """验证记录是否符合表结构"""
        # 检查是否有未定义的列
        for col_name in record_data:
            if col_name not in self.column_map:
                return False

        # 检查每列的值
        for column in self.columns:
            value = record_data.get(column.name)
            if not column.validate_value(value):
                return False

        # 检查主键不能为空
        for pk_col in self.primary_key_columns:
            if record_data.get(pk_col) is None:
                return False

        return True

    def serialize_record(self, record_data: Dict[str, Any]) -> bytes:
        """序列化记录"""
        result = b""
        for column in self.columns:
            value = record_data.get(column.name)
            result += column.serialize_value(value)
        return result

    def deserialize_record(self, data: bytes) -> Dict[str, Any]:
        """反序列化记录"""
        result = {}
        offset = 0

        for column in self.columns:
            value, consumed = column.deserialize_value(data, offset)
            result[column.name] = value
            offset += consumed

        return result
