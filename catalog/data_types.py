"""
数据类型定义
"""

from enum import Enum
from typing import Any, Tuple
import struct


class DataType(Enum):
    """支持的数据类型"""

    INTEGER = "INTEGER"
    VARCHAR = "VARCHAR"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"


class ColumnDefinition:
    """列定义"""

    def __init__(
        self,
        name: str,
        data_type: DataType,
        max_length: int = None,
        nullable: bool = True,
        primary_key: bool = False,
    ):
        self.name = name
        self.data_type = data_type
        self.max_length = max_length  # VARCHAR类型使用
        self.nullable = nullable
        self.primary_key = primary_key

    def validate_value(self, value: Any) -> bool:
        """验证值是否符合列定义"""
        if value is None:
            return self.nullable

        if self.data_type == DataType.INTEGER:
            return isinstance(value, int)
        elif self.data_type == DataType.VARCHAR:
            if not isinstance(value, str):
                return False
            return self.max_length is None or len(value) <= self.max_length
        elif self.data_type == DataType.FLOAT:
            return isinstance(value, (int, float))
        elif self.data_type == DataType.BOOLEAN:
            return isinstance(value, bool)

        return False

    def serialize_value(self, value: Any) -> bytes:
        """将值序列化为字节"""
        if value is None:
            return b"\x00"  # NULL标记

        if self.data_type == DataType.INTEGER:
            return b"\x01" + struct.pack("<i", value)
        elif self.data_type == DataType.VARCHAR:
            str_bytes = value.encode("utf-8")
            return b"\x01" + struct.pack("<H", len(str_bytes)) + str_bytes
        elif self.data_type == DataType.FLOAT:
            return b"\x01" + struct.pack("<f", float(value))
        elif self.data_type == DataType.BOOLEAN:
            return b"\x01" + struct.pack("<?", value)

        raise ValueError(f"不支持的数据类型: {self.data_type}")

    def deserialize_value(self, data: bytes, offset: int = 0) -> tuple[Any, int]:
        """从字节反序列化值，返回(value, consumed_bytes)"""
        if data[offset] == 0:  # NULL
            return None, 1

        offset += 1  # 跳过非NULL标记

        if self.data_type == DataType.INTEGER:
            value = struct.unpack("<i", data[offset : offset + 4])[0]
            return value, 5
        elif self.data_type == DataType.VARCHAR:
            length = struct.unpack("<H", data[offset : offset + 2])[0]
            str_bytes = data[offset + 2 : offset + 2 + length]
            value = str_bytes.decode("utf-8")
            return value, 3 + length
        elif self.data_type == DataType.FLOAT:
            value = struct.unpack("<f", data[offset : offset + 4])[0]
            return value, 5
        elif self.data_type == DataType.BOOLEAN:
            value = struct.unpack("<?", data[offset : offset + 1])[0]
            return value, 2

        raise ValueError(f"不支持的数据类型: {self.data_type}")

    def __repr__(self):
        return f"Column({self.name}, {self.data_type.value})"
