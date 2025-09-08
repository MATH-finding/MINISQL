"""
数据库主接口
"""

from typing import Dict, Any
from storage import PageManager, BufferManager, RecordManager
from catalog import SystemCatalog
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

        # 初始化表管理层
        self.table_manager = TableManager(self.catalog, self.record_manager)

        # 初始化SQL处理层
        self.sql_executor = SQLExecutor(self.table_manager, self.catalog)

        print(f"数据库 {db_file} 已连接")

    def execute_sql(self, sql: str) -> Dict[str, Any]:
        """执行SQL语句"""
        try:
            # 词法分析
            lexer = SQLLexer(sql.strip())
            tokens = lexer.tokenize()

            # 语法分析
            parser = SQLParser(tokens)
            ast = parser.parse()

            # 执行SQL
            result = self.sql_executor.execute(ast)

            return result

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": f"SQL执行失败: {str(e)}",
            }

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """获取表信息"""
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

        return {
            "table_name": table_name,
            "columns": columns_info,
            "record_count": record_count,
            "pages": self.catalog.get_table_pages(table_name),
        }

    def list_tables(self) -> list:
        """列出所有表"""
        return self.catalog.list_tables()

    def get_database_stats(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        cache_stats = self.buffer_manager.get_cache_stats()

        return {
            "database_file": self.db_file,
            "file_size_pages": self.page_manager.get_file_size(),
            "tables_count": len(self.catalog.list_tables()),
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
