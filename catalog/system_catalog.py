"""
系统目录管理
"""

from typing import Dict, List, Optional
import pickle
from storage import BufferManager, PageManager
from .data_types import ColumnDefinition
from .schema import TableSchema


class SystemCatalog:
    def __init__(self, buffer_manager: BufferManager):
        self.buffer_manager = buffer_manager
        self.tables: Dict[str, TableSchema] = {}
        self.table_pages: Dict[str, List[int]] = {}  # 表名 -> 页面列表
        self.catalog_page_id = 0  # 系统目录页面
        # 视图元数据：view_name -> view_definition
        self.views = {}  # 内存实现，生产环境可用表实现
        self._load_catalog()

    def _load_catalog(self):
        """从磁盘加载系统目录"""
        try:
            page = self.buffer_manager.get_page(self.catalog_page_id)
            try:
                # 读取目录数据长度
                data_length = page.read_int(0)
                if data_length > 0:
                    # 读取序列化的目录数据
                    catalog_bytes = page.read_bytes(4, data_length)
                    catalog_data = pickle.loads(catalog_bytes)

                    self.tables = catalog_data.get("tables", {})
                    self.table_pages = catalog_data.get("table_pages", {})
            finally:
                self.buffer_manager.unpin_page(self.catalog_page_id, False)
        except:
            # 目录页面不存在，创建新的
            self._create_empty_catalog()

    def _create_empty_catalog(self):
        """创建空的系统目录"""
        try:
            page = self.buffer_manager.page_manager.allocate_page()
            self.catalog_page_id = page.page_id
            page.write_int(0, 0)  # 数据长度为0
            self.buffer_manager.page_manager.write_page(page)
        except:
            # 页面已存在，直接使用
            pass

    def _save_catalog(self):
        """保存系统目录到磁盘"""
        catalog_data = {"tables": self.tables, "table_pages": self.table_pages}
        catalog_bytes = pickle.dumps(catalog_data)

        page = self.buffer_manager.get_page(self.catalog_page_id)
        try:
            # 检查页面是否够大
            if len(catalog_bytes) + 4 > page.PAGE_SIZE:
                raise RuntimeError("系统目录太大，无法存储在单个页面中")

            page.write_int(0, len(catalog_bytes))
            page.write_bytes(4, catalog_bytes)
        finally:
            self.buffer_manager.unpin_page(self.catalog_page_id, True)

    def create_table(self, table_name: str, columns: List[ColumnDefinition]):
        """创建表"""
        if table_name in self.tables:
            raise ValueError(f"表 {table_name} 已存在")

        schema = TableSchema(table_name, columns)
        self.tables[table_name] = schema
        self.table_pages[table_name] = []  # 初始无页面

        self._save_catalog()
        print(f"表 {table_name} 创建成功")

    def get_table_schema(self, table_name: str) -> Optional[TableSchema]:
        """获取表结构"""
        return self.tables.get(table_name)

    def list_tables(self) -> List[str]:
        """列出所有表"""
        return list(self.tables.keys())

    def allocate_page_for_table(self, table_name: str) -> int:
        """为表分配新页面"""
        if table_name not in self.tables:
            raise ValueError(f"表 {table_name} 不存在")

        page = self.buffer_manager.page_manager.allocate_page()

        # 初始化页面头部
        page.write_int(0, 0)  # 记录数量
        page.write_int(4, 8)  # 空闲空间偏移
        self.buffer_manager.page_manager.write_page(page)

        # 记录到表的页面列表
        self.table_pages[table_name].append(page.page_id)
        self._save_catalog()

        return page.page_id

    def get_table_pages(self, table_name: str) -> List[int]:
        """获取表的所有页面"""
        return self.table_pages.get(table_name, [])

    def create_view(self, view_name, view_definition):
        if view_name in self.views:
            raise ValueError(f"视图 {view_name} 已存在")
        self.views[view_name] = view_definition

    def drop_view(self, view_name):
        if view_name not in self.views:
            raise ValueError(f"视图 {view_name} 不存在")
        del self.views[view_name]

    def get_view_definition(self, view_name):
        if view_name not in self.views:
            raise ValueError(f"视图 {view_name} 不存在")
        return self.views[view_name]
