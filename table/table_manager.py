"""
表管理器 - 提供表级操作接口
"""

from typing import Dict, List, Any, Optional
from storage import Record, RecordManager
from catalog import SystemCatalog, ColumnDefinition


class TableManager:
    """表管理器，提供表级别的操作接口"""

    def __init__(self, catalog, record_manager):
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
                    if existing.data.get(pk_col) != record_data.get(pk_col):
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
                if condition_func is None or condition_func(record.data):
                    if self.record_manager.delete_record(page_id, i):
                        deleted_count += 1

        return deleted_count

    def drop_table(self, table_name: str) -> bool:
        """删除表"""
        schema = self.catalog.get_table_schema(table_name)
        if not schema:
            return False

        # 删除所有表数据页面
        pages = self.catalog.get_table_pages(table_name)
        for page_id in pages:
            # 这里应该释放页面，但由于当前实现中没有页面释放机制
            # 所以只是清空页面内容
            records = self.record_manager.get_records(page_id)
            for i in range(len(records)):
                self.record_manager.delete_record(page_id, i)

        # 从系统目录中删除表
        return self.catalog.drop_table(table_name)

    def truncate_table(self, table_name: str) -> int:
        """清空表数据，保留表结构"""
        schema = self.catalog.get_table_schema(table_name)
        if not schema:
            raise ValueError(f"表 {table_name} 不存在")

        # 统计要删除的记录数
        record_count = self.count_records(table_name)

        # 清空所有数据页面
        pages = self.catalog.get_table_pages(table_name)
        for page_id in pages:
            records = self.record_manager.get_records(page_id)
            # 从后往前删除，避免索引问题
            for i in range(len(records) - 1, -1, -1):
                self.record_manager.delete_record(page_id, i)

        return record_count

    def update_records(
        self, table_name: str, updates: Dict[str, Any], condition_func=None
    ) -> int:
        """更新表中的记录

        Args:
            table_name: 表名
            updates: 要更新的字段和值 {'column_name': new_value}
            condition_func: 过滤条件函数，接受记录数据返回bool

        Returns:
            更新的记录数量
        """
        schema = self.catalog.get_table_schema(table_name)
        if not schema:
            raise ValueError(f"表 {table_name} 不存在")

        # 验证更新的列是否存在
        for column_name in updates.keys():
            if not any(col.name == column_name for col in schema.columns):
                raise ValueError(f"列 {column_name} 不存在于表 {table_name}")

        updated_count = 0
        table_pages = self.catalog.get_table_pages(table_name)

        # 遍历每个页面
        for page_id in table_pages:
            records = self.record_manager.get_records(page_id)

            # 检查每条记录
            for record_index, record in enumerate(records):
                if condition_func is None or condition_func(record.data):
                    # 创建更新后的记录数据
                    new_data = record.data.copy()
                    new_data.update(updates)

                    # 创建新记录
                    new_record = Record(new_data)

                    # 使用RecordManager的update_record方法更新
                    if self.record_manager.update_record(
                        page_id, record_index, new_record
                    ):
                        updated_count += 1
                    else:
                        print(
                            f"更新记录失败: page_id={page_id}, record_index={record_index}"
                        )

        return updated_count

    def insert_record_with_location(self, table_name: str, record_data: Dict[str, Any]) -> Optional[tuple[int, int]]:
        """插入记录并返回 (page_id, record_index)；失败返回 None。"""
        schema = self.catalog.get_table_schema(table_name)
        if not schema:
            raise ValueError(f"表 {table_name} 不存在")
        if not schema.validate_record(record_data):
            raise ValueError("记录数据不符合表结构")

        # 简化：主键检查沿用原逻辑
        if schema.primary_key_columns:
            existing_records = self.scan_table(table_name)
            for existing in existing_records:
                if all(existing.get(pk) == record_data.get(pk) for pk in schema.primary_key_columns):
                    raise ValueError(f"主键冲突: {schema.primary_key_columns}")

        pages = self.catalog.get_table_pages(table_name)
        record = Record(record_data)
        for page_id in pages:
            idx = self.record_manager.insert_record_with_index(page_id, record)
            if idx is not None:
                return (page_id, idx)
        # 分配新页
        new_page_id = self.catalog.allocate_page_for_table(table_name)
        idx = self.record_manager.insert_record_with_index(new_page_id, record)
        if idx is None:
            return None
        return (new_page_id, idx)

    def scan_table_with_locations(self, table_name: str) -> List[tuple[int, int, Record]]:
        """扫描表，返回 (page_id, record_index, Record)。"""
        schema = self.catalog.get_table_schema(table_name)
        if not schema:
            raise ValueError(f"表 {table_name} 不存在")
        results: List[tuple[int, int, Record]] = []
        for page_id in self.catalog.get_table_pages(table_name):
            rows = self.record_manager.get_records_with_indices(page_id)
            for idx, rec in rows:
                results.append((page_id, idx, rec))
        return results

    def delete_at(self, table_name: str, page_id: int, record_index: int) -> bool:
        """按位置删除一条记录。"""
        return self.record_manager.delete_record(page_id, record_index)

    def update_at(self, table_name: str, page_id: int, record_index: int, new_data: Dict[str, Any]) -> bool:
        """按位置更新一条记录。"""
        new_record = Record(new_data)
        return self.record_manager.update_record(page_id, record_index, new_record)
