# catalog/index_manager.py
"""
索引管理器
"""

from typing import Dict, List, Optional, Any
from storage.btree import BPlusTree
from storage.buffer_manager import BufferManager
from storage.page_manager import PageManager
from .schema import TableSchema


class IndexInfo:
    """索引信息"""

    def __init__(
        self,
        index_name: str,
        table_name: str,
        column_name: str,
        root_page_id: int,
        is_unique: bool = False,
    ):
        self.index_name = index_name
        self.table_name = table_name
        self.column_name = column_name
        self.root_page_id = root_page_id
        self.is_unique = is_unique


class IndexManager:
    """索引管理器"""

    def __init__(self, buffer_manager: BufferManager, page_manager: PageManager):
        self.buffer_manager = buffer_manager
        self.page_manager = page_manager
        self.indexes: Dict[str, IndexInfo] = {}  # 索引名 -> 索引信息
        self.table_indexes: Dict[str, List[str]] = {}  # 表名 -> 索引名列表

    def create_index(
        self,
        index_name: str,
        table_name: str,
        column_name: str,
        is_unique: bool = False,
    ) -> bool:
        """创建索引"""
        if index_name in self.indexes:
            return False  # 索引已存在

        # 创建B+树
        btree = BPlusTree(self.buffer_manager, self.page_manager)

        # 记录索引信息
        index_info = IndexInfo(
            index_name, table_name, column_name, btree.root_page_id, is_unique
        )
        self.indexes[index_name] = index_info

        # 更新表的索引列表
        if table_name not in self.table_indexes:
            self.table_indexes[table_name] = []
        self.table_indexes[table_name].append(index_name)

        return True

    def drop_index(self, index_name: str) -> bool:
        """删除索引"""
        if index_name not in self.indexes:
            return False

        index_info = self.indexes[index_name]

        # 从表的索引列表中移除
        if index_info.table_name in self.table_indexes:
            self.table_indexes[index_info.table_name].remove(index_name)
            if not self.table_indexes[index_info.table_name]:
                del self.table_indexes[index_info.table_name]

        # 删除索引信息
        del self.indexes[index_name]

        # TODO: 释放B+树占用的页面
        return True

    def get_index(self, index_name: str) -> Optional[BPlusTree]:
        """获取索引的B+树"""
        if index_name not in self.indexes:
            return None

        index_info = self.indexes[index_name]
        return BPlusTree(
            self.buffer_manager,
            self.page_manager,
            root_page_id=index_info.root_page_id,
            is_unique=index_info.is_unique,  # 传递唯一性标记
        )

    def get_table_indexes(self, table_name: str) -> List[str]:
        """获取表的所有索引名"""
        return self.table_indexes.get(table_name, [])

    def insert_into_indexes(
        self, table_name: str, record: Dict[str, Any], rid: tuple[int, int]
    ) -> bool:
        """将记录插入到所有相关索引中，支持唯一性检查"""
        if table_name not in self.table_indexes:
            return True

        for index_name in self.table_indexes[table_name]:
            index_info = self.indexes[index_name]
            if index_info.column_name in record:
                btree = self.get_index(index_name)
                key = record[index_info.column_name]
                try:
                    if not btree.insert(key, rid):
                        return False
                except ValueError as e:
                    # 唯一性约束违反
                    raise e

        return True

    def update_index_for_record(
        self,
        table_name: str,
        old_record: Dict[str, Any],
        new_record: Dict[str, Any],
        rid: tuple[int, int],
    ) -> bool:
        """更新记录时维护索引，支持唯一性检查"""
        if table_name not in self.table_indexes:
            return True

        for index_name in self.table_indexes[table_name]:
            index_info = self.indexes[index_name]
            column_name = index_info.column_name

            if column_name in old_record or column_name in new_record:
                btree = self.get_index(index_name)

                # 如果键值发生变化
                old_key = old_record.get(column_name)
                new_key = new_record.get(column_name)

                if old_key != new_key:
                    # 如果是唯一索引，先检查新键是否已存在
                    if index_info.is_unique and new_key is not None:
                        existing_value = btree.search(new_key)
                        if existing_value is not None:
                            raise ValueError(
                                f"唯一性约束违反：键 {new_key} 已存在于索引 {index_name}"
                            )

                    # 删除旧键（TODO: 需要实现delete方法）
                    # btree.delete(old_key, rid)

                    # 插入新键
                    if new_key is not None:
                        try:
                            if not btree.insert(new_key, rid):
                                return False
                        except ValueError as e:
                            raise e

        return True

    def delete_from_indexes(
        self, table_name: str, record: Dict[str, Any], record_id: int
    ) -> bool:
        """从所有相关索引中删除记录"""
        # TODO: 实现B+树删除操作
        pass
