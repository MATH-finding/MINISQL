"""
数据库主接口
"""

from typing import Dict, Any
from storage import PageManager, BufferManager, RecordManager
from catalog import SystemCatalog
from catalog.index_manager import IndexManager  # 新增导入
from table import TableManager
from sql import SQLLexer, SQLParser, SQLExecutor


class SimpleDatabase:
    """简化版数据库系统主接口"""

    def __init__(self, db_file: str, cache_size: int = 100):
        self.db_file = db_file

        # 初始化存储层
        self.page_manager = PageManager(db_file)
        self.buffer_manager = BufferManager(self.page_manager, cache_size)
        self.record_manager = RecordManager(self.buffer_manager)

        # 初始化目录层
        self.catalog = SystemCatalog(self.buffer_manager)

        # 添加索引管理器
        self.index_manager = IndexManager(self.buffer_manager, self.page_manager)

        # 初始化表管理层
        self.table_manager = TableManager(self.catalog, self.record_manager)

        # 会话管理：多个执行器共享同一存储与目录
        self._executors = []
        self._current_session = None
        self.new_session()  # 默认创建一个会话

        print(f"数据库 {db_file} 已连接")

    def new_session(self) -> int:
        executor = SQLExecutor(self.table_manager, self.catalog, self.index_manager)
        self._executors.append(executor)
        self._current_session = len(self._executors) - 1
        return self._current_session

    def use_session(self, idx: int) -> bool:
        if 0 <= idx < len(self._executors):
            self._current_session = idx
            return True
        return False

    def list_sessions(self) -> list:
        result = []
        for i, ex in enumerate(self._executors):
            result.append({
                "id": i,
                "session_id": getattr(ex, "session_id", i + 1),
                "autocommit": ex.txn.autocommit(),
                "in_txn": ex.txn.in_txn(),
                "isolation": ex.txn.isolation_level(),
                "current": (i == self._current_session),
            })
        return result

    @property
    def sql_executor(self) -> SQLExecutor:
        return self._executors[self._current_session]

    def execute_sql(self, sql: str) -> Dict[str, Any]:
        """执行SQL语句（路由到当前会话）"""
        try:
            lexer = SQLLexer(sql.strip())
            tokens = lexer.tokenize()
            parser = SQLParser(tokens)
            ast = parser.parse()
            result = self.sql_executor.execute(ast)
            return result
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": f"SQL执行失败: {str(e)}",
            }

    # 新增：索引相关的管理方法
    def create_index(
        self,
        index_name: str,
        table_name: str,
        column_name: str,
        is_unique: bool = False,
    ) -> Dict[str, Any]:
        """创建索引"""
        try:
            success = self.index_manager.create_index(
                index_name, table_name, column_name, is_unique
            )
            if success:
                return {"success": True, "message": f"索引 {index_name} 创建成功"}
            else:
                return {"success": False, "message": f"索引 {index_name} 已存在"}
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": f"创建索引失败: {str(e)}",
            }

    def drop_index(self, index_name: str) -> Dict[str, Any]:
        """删除索引"""
        try:
            success = self.index_manager.drop_index(index_name)
            if success:
                return {"success": True, "message": f"索引 {index_name} 删除成功"}
            else:
                return {"success": False, "message": f"索引 {index_name} 不存在"}
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": f"删除索引失败: {str(e)}",
            }

    def list_indexes(self, table_name: str) -> dict:
        """列出表的所有索引"""
        if not self.index_manager:
            return {"success": True, "table_name": table_name, "indexes": []}

        try:
            table_indexes = self.index_manager.get_table_indexes(table_name)
            index_list = []

            for index_name in table_indexes:
                index_info = self.index_manager.indexes.get(index_name)
                if index_info:
                    index_list.append(
                        {
                            "index_name": index_name,
                            "column_name": index_info.column_name,
                            "is_unique": index_info.is_unique,
                        }
                    )

            return {"success": True, "table_name": table_name, "indexes": index_list}
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "table_name": table_name,
                "indexes": [],
            }

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """获取表信息 - 扩展版，包含索引信息"""
        schema = self.catalog.get_table_schema(table_name)
        if not schema:
            return {"error": f"表 {table_name} 不存在"}

        columns_info = []
        for col in schema.columns:
            col_info = {
                "name": col.name,
                "type": col.data_type.value,
                "max_length": col.max_length,
                "nullable": col.nullable,
                "primary_key": col.primary_key,
            }
            columns_info.append(col_info)

        record_count = self.table_manager.count_records(table_name)

        # 新增：获取表的索引信息
        table_indexes = self.index_manager.get_table_indexes(table_name)
        indexes_info = []
        for index_name in table_indexes:
            index_info = self.index_manager.indexes.get(index_name)
            if index_info:
                indexes_info.append(
                    {
                        "name": index_name,
                        "column": index_info.column_name,
                        "unique": index_info.is_unique,
                    }
                )

        return {
            "table_name": table_name,
            "columns": columns_info,
            "indexes": indexes_info,  # 新增
            "record_count": record_count,
            "pages": self.catalog.get_table_pages(table_name),
        }

    def list_tables(self) -> list:
        """列出所有表"""
        return self.catalog.list_tables()

    def get_database_stats(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        cache_stats = self.buffer_manager.get_cache_stats()

        # 新增：统计索引数量
        total_indexes = len(self.index_manager.indexes)

        return {
            "database_file": self.db_file,
            "file_size_pages": self.page_manager.get_file_size(),
            "tables_count": len(self.catalog.list_tables()),
            "indexes_count": total_indexes,  # 新增
            "cache_stats": cache_stats,
        }

    def flush_all(self):
        """刷新所有缓存到磁盘"""
        self.buffer_manager.flush_all()

    def close(self):
        """关闭数据库连接"""
        try:
            self.flush_all()
            print(f"数据库 {self.db_file} 已关闭")
        except Exception as e:
            print(f"关闭数据库时发生错误: {e}")
