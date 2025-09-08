"""
表管理器 - 提供表级操作接口
"""

from typing import Dict, List, Any, Optional
from storage import Record, RecordManager
from catalog import SystemCatalog, ColumnDefinition


class TableManager:
    """表管理器，提供表级别的操作接口"""

    def __init__(self, catalog: SystemCatalog, record_manager: RecordManager):
        self.catalog = catalog
        self.record_manager = record_manager

    def create_table(self, table_name: str, columns: List[ColumnDefinition]):
        """创建表"""
        self.catalog.create_table(table_name, columns)

    def insert_record(self, table_name: str, record_data: Dict[str, Any]) -> bool:
        """插入记录到表"""
        schema = self.catalog.get_table_schema(table_name)
        if not schema:
            raise ValueError(f"表 {table_name} 不存在")

        # 验证记录
        if not schema.validate_record(record_data):
            raise ValueError("记录数据不符合表结构")

        # 检查主键唯一性（简化实现）
        if schema.primary_key_columns:
            existing_records = self.scan_table(table_name)
            for existing in existing_records:
                pk_match = True
                for pk_col in schema.primary_key_columns:
                    if existing.get(pk_col) != record_data.get(pk_col):
                        pk_match = False
                        break
                if pk_match:
                    raise ValueError(f"主键冲突: {schema.primary_key_columns}")

        # 尝试插入到现有页面
        pages = self.catalog.get_table_pages(table_name)
        record = Record(record_data)

        for page_id in pages:
            if self.record_manager.insert_record(page_id, record):
                return True

        # 现有页面都没有空间，分配新页面
        new_page_id = self.catalog.allocate_page_for_table(table_name)
        return self.record_manager.insert_record(new_page_id, record)

    def scan_table(self, table_name: str) -> List[Record]:
        """扫描表的所有记录"""
        schema = self.catalog.get_table_schema(table_name)
        if not schema:
            raise ValueError(f"表 {table_name} 不存在")

        all_records = []
        pages = self.catalog.get_table_pages(table_name)

        for page_id in pages:
            records = self.record_manager.get_records(page_id)
            all_records.extend(records)

        return all_records

    def count_records(self, table_name: str) -> int:
        """统计表中记录数量"""
        return len(self.scan_table(table_name))

    def delete_records(self, table_name: str, condition_func=None) -> int:
        """删除符合条件的记录（简化实现）"""
        # 真实数据库会有更高效的删除机制
        deleted_count = 0
        pages = self.catalog.get_table_pages(table_name)

        for page_id in pages:
            records = self.record_manager.get_records(page_id)
            for i, record in enumerate(records):
                if condition_func is None or condition_func(record):
                    if self.record_manager.delete_record(page_id, i):
                        deleted_count += 1

        return deleted_count
