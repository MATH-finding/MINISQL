"""
日志管理器 - 为不同组件提供统一的日志接口
"""

from .logger import DatabaseLogger, LogLevel


class LogManager:
    """日志管理器"""

    def __init__(self, db_name: str):
        self.logger = DatabaseLogger(db_name)

    def log_sql_execution(
        self, sql: str, success: bool, execution_time: float, result_count: int = 0
    ):
        """记录SQL执行日志"""
        status = "成功" if success else "失败"
        sql_preview = sql[:100] + "..." if len(sql) > 100 else sql
        message = f"SQL执行{status}: {sql_preview} (耗时: {execution_time:.3f}ms, 结果: {result_count}行)"

        if success:
            self.logger.info(message, "SQL_EXECUTOR")
        else:
            self.logger.error(message, "SQL_EXECUTOR")

    def log_cache_stats(
        self, hits: int, misses: int, hit_rate: float, total_pages: int
    ):
        """记录缓存统计 - 专门对应buffer_manager的统计需求"""
        message = f"缓存统计 - 命中: {hits}, 未命中: {misses}, 命中率: {hit_rate:.2%}, 缓存页数: {total_pages}"
        self.logger.info(message, "BUFFER_MANAGER")

    def log_page_operation(self, operation: str, page_id: int, success: bool = True):
        """记录页面操作"""
        status = "成功" if success else "失败"
        message = f"页面{operation}{status}: 页面ID {page_id}"

        if success:
            self.logger.debug(message, "PAGE_MANAGER")
        else:
            self.logger.error(message, "PAGE_MANAGER")

    def log_table_operation(self, operation: str, table_name: str, details: str = ""):
        """记录表操作"""
        message = f"表{operation}: {table_name}"
        if details:
            message += f" - {details}"
        self.logger.info(message, "TABLE_MANAGER")

    def log_index_operation(
        self, operation: str, index_name: str, table_name: str = ""
    ):
        """记录索引操作"""
        message = f"索引{operation}: {index_name}"
        if table_name:
            message += f" (表: {table_name})"
        self.logger.info(message, "INDEX_MANAGER")

    def log_buffer_flush(self, flushed_count: int):
        """记录缓冲区刷新"""
        message = f"缓冲区刷新完成，写入 {flushed_count} 个脏页到磁盘"
        self.logger.info(message, "BUFFER_MANAGER")

    def log_error(self, component: str, error_message: str, details: str = ""):
        """记录错误"""
        message = f"{error_message}"
        if details:
            message += f" - {details}"
        self.logger.error(message, component)

    def set_log_level(self, level: LogLevel):
        """设置日志级别"""
        self.logger.set_log_level(level)

    def close(self):
        self.logger.close()
