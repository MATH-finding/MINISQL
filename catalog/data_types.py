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
    # 新增数据类型
    CHAR = "CHAR"           # 固定长度字符串
    DECIMAL = "DECIMAL"     # 精确小数
    DATE = "DATE"          # 日期类型
    TIME = "TIME"          # 时间类型
    DATETIME = "DATETIME"   # 日期时间类型
    BIGINT = "BIGINT"      # 64位整数
    TINYINT = "TINYINT"    # 8位整数
    TEXT = "TEXT"          # 长文本

class ColumnDefinition:
    """列定义"""

    def __init__(
        self,
        name: str,
        data_type: DataType,
        max_length: int = None,
        nullable: bool = True,
        primary_key: bool = False,
        precision: int = None,
        scale: int = None,
    ):
        self.name = name
        self.data_type = data_type
        self.max_length = max_length  # VARCHAR类型使用
        self.nullable = nullable
        self.primary_key = primary_key
        self.precision = precision  # DECIMAL类型使用
        self.scale = scale        # DECIMAL类型使用

    def validate_value(self, value: Any) -> bool:
        """验证值是否符合列定义"""
        if value is None:
            return self.nullable

        if self.data_type == DataType.INTEGER:
            return isinstance(value, int) and -2147483648 <= value <= 2147483647
        elif self.data_type == DataType.BIGINT:
            return isinstance(value, int) and -9223372036854775808 <= value <= 9223372036854775807
        elif self.data_type == DataType.TINYINT:
            return isinstance(value, int) and -128 <= value <= 127
        elif self.data_type == DataType.VARCHAR:
            if not isinstance(value, str):
                return False
            return self.max_length is None or len(value) <= self.max_length
        elif self.data_type == DataType.CHAR:
            if not isinstance(value, str):
                return False
            return self.max_length is None or len(value) <= self.max_length
        elif self.data_type == DataType.TEXT:
            return isinstance(value, str)
        elif self.data_type == DataType.FLOAT:
            return isinstance(value, (int, float))
        elif self.data_type == DataType.DECIMAL:
            if not isinstance(value, (int, float, str)):
                return False
            # 验证精度和小数位数
            if self.precision and self.scale:
                decimal_str = str(value)
                if '.' in decimal_str:
                    integer_part, decimal_part = decimal_str.split('.')
                    if len(integer_part) + len(decimal_part) > self.precision:
                        return False
                    if len(decimal_part) > self.scale:
                        return False
            return True
        elif self.data_type == DataType.BOOLEAN:
            return isinstance(value, bool)
        elif self.data_type == DataType.DATE:
            return isinstance(value, str) and self._is_valid_date(value)
        elif self.data_type == DataType.TIME:
            return isinstance(value, str) and self._is_valid_time(value)
        elif self.data_type == DataType.DATETIME:
            return isinstance(value, str) and self._is_valid_datetime(value)

        return False

    def _is_valid_date(self, date_str: str) -> bool:
        """验证日期格式 YYYY-MM-DD"""
        try:
            from datetime import datetime
            datetime.strptime(date_str, '%Y-%m-%d')
            return True
        except ValueError:
            return False

    def _is_valid_time(self, time_str: str) -> bool:
        """验证时间格式 HH:MM:SS"""
        try:
            from datetime import datetime
            datetime.strptime(time_str, '%H:%M:%S')
            return True
        except ValueError:
            return False

    def _is_valid_datetime(self, datetime_str: str) -> bool:
        """验证日期时间格式 YYYY-MM-DD HH:MM:SS"""
        try:
            from datetime import datetime
            datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
            return True
        except ValueError:
            return False


    def serialize_value(self, value: Any) -> bytes:
        """将值序列化为字节"""
        if value is None:
            return b"\x00"  # NULL标记

        if self.data_type == DataType.INTEGER:
            return b"\x01" + struct.pack("<i", value)
        elif self.data_type == DataType.BIGINT:
            return b"\x01" + struct.pack("<q", value)
        elif self.data_type == DataType.TINYINT:
            return b"\x01" + struct.pack("<b", value)
        elif self.data_type == DataType.VARCHAR:
            str_bytes = value.encode("utf-8")
            return b"\x01" + struct.pack("<H", len(str_bytes)) + str_bytes
        elif self.data_type == DataType.CHAR:
            # 固定长度，需要填充
            str_bytes = value.encode("utf-8")
            if len(str_bytes) > self.max_length:
                str_bytes = str_bytes[:self.max_length]
            else:
                str_bytes = str_bytes.ljust(self.max_length, b'\x00')
            return b"\x01" + str_bytes
        elif self.data_type == DataType.TEXT:
            str_bytes = value.encode("utf-8")
            return b"\x01" + struct.pack("<I", len(str_bytes)) + str_bytes
        elif self.data_type == DataType.FLOAT:
            return b"\x01" + struct.pack("<f", float(value))
        elif self.data_type == DataType.DECIMAL:
            # 将DECIMAL存储为字符串
            decimal_str = str(value)
            str_bytes = decimal_str.encode("utf-8")
            return b"\x01" + struct.pack("<H", len(str_bytes)) + str_bytes
        elif self.data_type == DataType.BOOLEAN:
            return b"\x01" + struct.pack("<?", value)
        elif self.data_type in (DataType.DATE, DataType.TIME, DataType.DATETIME):
            str_bytes = value.encode("utf-8")
            return b"\x01" + struct.pack("<H", len(str_bytes)) + str_bytes

        raise ValueError(f"不支持的数据类型: {self.data_type}")

    def deserialize_value(self, data: bytes, offset: int = 0) -> tuple[Any, int]:
        """从字节反序列化值，返回(value, consumed_bytes)"""
        if data[offset] == 0:  # NULL
            return None, 1

        offset += 1  # 跳过非NULL标记

        if self.data_type == DataType.INTEGER:
            value = struct.unpack("<i", data[offset : offset + 4])[0]
            return value, 5
        elif self.data_type == DataType.BIGINT:
            value = struct.unpack("<q", data[offset : offset + 8])[0]
            return value, 9
        elif self.data_type == DataType.TINYINT:
            value = struct.unpack("<b", data[offset : offset + 1])[0]
            return value, 2
        elif self.data_type == DataType.VARCHAR:
            length = struct.unpack("<H", data[offset : offset + 2])[0]
            str_bytes = data[offset + 2 : offset + 2 + length]
            value = str_bytes.decode("utf-8")
            return value, 3 + length
        elif self.data_type == DataType.CHAR:
            str_bytes = data[offset : offset + self.max_length]
            value = str_bytes.rstrip(b'\x00').decode("utf-8")
            return value, 1 + self.max_length
        elif self.data_type == DataType.TEXT:
            length = struct.unpack("<I", data[offset : offset + 4])[0]
            str_bytes = data[offset + 4 : offset + 4 + length]
            value = str_bytes.decode("utf-8")
            return value, 5 + length
        elif self.data_type == DataType.FLOAT:
            value = struct.unpack("<f", data[offset : offset + 4])[0]
            return value, 5
        elif self.data_type == DataType.DECIMAL:
            length = struct.unpack("<H", data[offset : offset + 2])[0]
            str_bytes = data[offset + 2 : offset + 2 + length]
            value = str_bytes.decode("utf-8")
            return value, 3 + length
        elif self.data_type == DataType.BOOLEAN:
            value = struct.unpack("<?", data[offset : offset + 1])[0]
            return value, 2
        elif self.data_type in (DataType.DATE, DataType.TIME, DataType.DATETIME):
            length = struct.unpack("<H", data[offset : offset + 2])[0]
            str_bytes = data[offset + 2 : offset + 2 + length]
            value = str_bytes.decode("utf-8")
            return value, 3 + length

        raise ValueError(f"不支持的数据类型: {self.data_type}")

    def __repr__(self):
        return f"Column({self.name}, {self.data_type.value})"
