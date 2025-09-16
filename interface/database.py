"""
数据库主接口
"""

import os
from typing import Dict, Any
from storage import PageManager, BufferManager, RecordManager
from catalog import SystemCatalog
from catalog.index_manager import IndexManager
from table import TableManager
from sql import (
    SQLLexer,
    SQLParser,
    SQLExecutor,
    SelectStatement,
    InsertStatement,
    UpdateStatement,
    DeleteStatement,
    CreateTableStatement,
    DropTableStatement,
    Statement,
    SemanticAnalyzer,
    SemanticError,
    DiagnosticEngine
)
from typing import Optional,List



class SimpleDatabase:
    """简化版数据库系统主接口"""

    def __init__(self, db_file: str, cache_size: int = 100):
        self.db_file = db_file

        # 添加日志管理器初始化
        from db_logging.log_manager import LogManager

        db_name = os.path.splitext(os.path.basename(db_file))[0]
        self.log_manager = LogManager(db_name)

        # 初始化存储层
        self.page_manager = PageManager(db_file)
        self.buffer_manager = BufferManager(self.page_manager, cache_size)

        # 为buffer_manager设置日志管理器
        self.buffer_manager.set_log_manager(self.log_manager)

        self.record_manager = RecordManager(self.buffer_manager)

        # 初始化目录层
        self.catalog = SystemCatalog(self.buffer_manager)

        # 添加索引管理器
        self.index_manager = IndexManager(
            self.buffer_manager, self.page_manager, self.catalog
        )

        # 初始化表管理层
        self.table_manager = TableManager(self.catalog, self.record_manager)

        # 会话管理：多个执行器共享同一存储与目录
        self._executors = []
        self._current_session = None
        self.new_session()  # 默认创建一个会话

        # 为了兼容性，也设置executor别名
        self.executor = self.sql_executor

        # 用户会话管理
        self.current_user = None
        self.is_authenticated = False

        # 语义分析器 与 诊断纠错引擎
        self.semantic = SemanticAnalyzer(self.catalog)
        self.diag = DiagnosticEngine(self.catalog, auto_correct=True)

        print(f"数据库 {db_file} 已连接")

    def new_session(self) -> int:
        """创建新会话"""
        executor = SQLExecutor(self.table_manager, self.catalog, self.index_manager)
        self._executors.append(executor)
        self._current_session = len(self._executors) - 1
        return self._current_session

    def use_session(self, idx: int) -> bool:
        """切换到指定会话"""
        if 0 <= idx < len(self._executors):
            self._current_session = idx
            return True
        return False

    def list_sessions(self) -> list:
        """列出所有会话"""
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
        """获取当前会话的SQL执行器"""
        return self._executors[self._current_session]

    def login(self, username: str, password: str) -> Dict[str, Any]:
        """用户登录"""
        if self.catalog.authenticate_user(username, password):
            self.current_user = username
            self.is_authenticated = True
            # 同步设置所有executor的current_user
            for executor in self._executors:
                executor.current_user = username
            return {"success": True, "message": f"用户 {username} 登录成功"}
        else:
            return {"success": False, "message": "用户名或密码错误"}

    def logout(self) -> Dict[str, Any]:
        """用户登出"""
        user = self.current_user
        self.current_user = None
        self.is_authenticated = False
        # 同步清理所有executor的current_user
        for executor in self._executors:
            executor.current_user = None
        return {"success": True, "message": f"用户 {user} 已登出"}

    def get_current_user(self) -> Optional[str]:
        """获取当前用户"""
        return self.current_user

    def execute_sql(self, sql: str) -> Dict[str, Any]:
        """执行SQL语句 - 带权限检查"""
        if not self.is_authenticated:
            return {
                "success": False,
                "error": "未登录",
                "message": "请先登录后再执行SQL语句",
            }

        try:
            # 词法分析
            lexer = SQLLexer(sql.strip())
            tokens = lexer.tokenize()

            # 语法分析
            parser = SQLParser(tokens)
            ast = parser.parse()

            # 权限检查 - admin用户跳过权限检查
            if self.current_user != "admin":
                privilege_check = self._check_statement_privilege(ast)
                if not privilege_check["allowed"]:
                    return {
                        "success": False,
                        "error": "权限不足",
                        "message": privilege_check["message"],
                    }

            # 语义分析
            auto_corrected = False
            auto_hints = []
            try:
                analyzed = self.semantic.analyze(ast)
                ast_to_run = analyzed.ast
            except SemanticError as se:
                # 尝试纠错一次
                corr = self.diag.try_correct(ast, str(se))
                if corr.changed and corr.ast is not None:
                    try:
                        analyzed = self.semantic.analyze(corr.ast)
                        ast_to_run = analyzed.ast
                        auto_corrected = True
                        auto_hints = corr.hints or []
                    except SemanticError as se2:
                        return {"success": False, "error": str(se2), "message": f"语义错误: {str(se2)}", "hints": corr.hints, "data": []}
                else:
                    return {"success": False, "error": str(se), "message": f"语义错误: {str(se)}", "data": []}

            # 执行SQL
            result = self.sql_executor.execute(ast_to_run)

            # 标记与提示：若应用了自动纠错，则附加hints
            if auto_corrected:
                result["auto_corrected"] = True
                if auto_hints:
                    result["hints"] = auto_hints
                # 追加消息说明
                base_msg = result.get("message", "")
                note = "（已应用智能纠错）"
                result["message"] = (base_msg + note) if base_msg else "已应用智能纠错"

            # 记录SQL执行日志
            if self.log_manager:
                success = result.get("success", False)
                result_count = len(result.get("data") or []) if "data" in result else 0
                self.log_manager.log_sql_execution(sql, success, 0.0, result_count)

            return result

        except (SemanticError,) as e:
            if self.log_manager:
                self.log_manager.log_sql_execution(sql, False, 0.0, 0)
            return {"success": False, "error": str(e), "message": f"语义错误: {str(e)}", "data": []}
        except Exception as e:
            if self.log_manager:
                self.log_manager.log_sql_execution(sql, False, 0.0, 0)

            return {
                "success": False,
                "error": str(e),
                "message": f"SQL执行失败: {str(e)}",
                "data": [],
            }

    def _check_statement_privilege(self, ast: Statement) -> Dict[str, Any]:
        """检查语句执行权限"""
        # admin用户已在外层被跳过，这里不会执行到admin用户

        if isinstance(ast, SelectStatement):
            table_name = (
                ast.from_table if isinstance(ast.from_table, str) else "unknown"
            )
            if not self.catalog.check_privilege(
                self.current_user, table_name, "SELECT"
            ):
                return {
                    "allowed": False,
                    "message": f"用户 {self.current_user} 没有表 {table_name} 的 SELECT 权限",
                }

        elif isinstance(ast, InsertStatement):
            if not self.catalog.check_privilege(
                self.current_user, ast.table_name, "INSERT"
            ):
                return {
                    "allowed": False,
                    "message": f"用户 {self.current_user} 没有表 {ast.table_name} 的 INSERT 权限",
                }

        elif isinstance(ast, UpdateStatement):
            if not self.catalog.check_privilege(
                self.current_user, ast.table_name, "UPDATE"
            ):
                return {
                    "allowed": False,
                    "message": f"用户 {self.current_user} 没有表 {ast.table_name} 的 UPDATE 权限",
                }

        elif isinstance(ast, DeleteStatement):
            if not self.catalog.check_privilege(
                self.current_user, ast.table_name, "DELETE"
            ):
                return {
                    "allowed": False,
                    "message": f"用户 {self.current_user} 没有表 {ast.table_name} 的 DELETE 权限",
                }

        elif isinstance(ast, CreateTableStatement):
            if not self.catalog.check_privilege(self.current_user, "*", "CREATE"):
                return {
                    "allowed": False,
                    "message": f"用户 {self.current_user} 没有 CREATE 权限",
                }

        elif isinstance(ast, DropTableStatement):
            if not self.catalog.check_privilege(
                self.current_user, ast.table_name, "DROP"
            ):
                return {
                    "allowed": False,
                    "message": f"用户 {self.current_user} 没有表 {ast.table_name} 的 DROP 权限",
                }

        return {"allowed": True}

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

            # 记录索引操作日志
            if self.log_manager:
                if success:
                    self.log_manager.log_index_operation("创建", index_name, table_name)
                else:
                    self.log_manager.log_error(
                        "INDEX_MANAGER", f"索引 {index_name} 已存在"
                    )

            if success:
                return {"success": True, "message": f"索引 {index_name} 创建成功"}
            else:
                return {"success": False, "message": f"索引 {index_name} 已存在"}
        except Exception as e:
            if self.log_manager:
                self.log_manager.log_error("INDEX_MANAGER", f"创建索引失败", str(e))
            return {
                "success": False,
                "error": str(e),
                "message": f"创建索引失败: {str(e)}",
            }

    def drop_index(self, index_name: str) -> Dict[str, Any]:
        """删除索引"""
        try:
            success = self.index_manager.drop_index(index_name)

            # 记录索引操作日志
            if self.log_manager:
                if success:
                    self.log_manager.log_index_operation("删除", index_name)
                else:
                    self.log_manager.log_error(
                        "INDEX_MANAGER", f"索引 {index_name} 不存在"
                    )

            if success:
                return {"success": True, "message": f"索引 {index_name} 删除成功"}
            else:
                return {"success": False, "message": f"索引 {index_name} 不存在"}
        except Exception as e:
            if self.log_manager:
                self.log_manager.log_error("INDEX_MANAGER", f"删除索引失败", str(e))
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
            if self.log_manager:
                self.log_manager.log_error("INDEX_MANAGER", f"列出索引失败", str(e))
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

        # 获取表的索引信息
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
            "indexes": indexes_info,
            "record_count": record_count,
            "pages": self.catalog.get_table_pages(table_name),
        }

    def list_tables(self) -> list:
        """列出所有表"""
        return self.catalog.list_tables()

    def get_database_stats(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        cache_stats = self.buffer_manager.get_cache_stats()

        # 统计索引数量
        total_indexes = len(self.index_manager.indexes)

        return {
            "database_file": self.db_file,
            "file_size_pages": self.page_manager.get_file_size(),
            "tables_count": len(self.catalog.list_tables()),
            "indexes_count": total_indexes,
            "cache_stats": cache_stats,
        }

    def set_log_level(self, level: str) -> Dict[str, Any]:
        """设置日志级别"""
        from db_logging.logger import LogLevel

        level_mapping = {
            "DEBUG": LogLevel.DEBUG,
            "INFO": LogLevel.INFO,
            "WARNING": LogLevel.WARNING,
            "ERROR": LogLevel.ERROR,
            "CRITICAL": LogLevel.CRITICAL,
        }

        level_upper = level.upper()
        if level_upper not in level_mapping:
            return {
                "success": False,
                "message": f"无效的日志级别: {level}. 可选值: DEBUG, INFO, WARNING, ERROR, CRITICAL",
            }

        try:
            self.log_manager.set_log_level(level_mapping[level_upper])
            return {"success": True, "message": f"日志级别已设置为: {level_upper}"}
        except Exception as e:
            return {"success": False, "message": f"设置日志级别失败: {str(e)}"}

    def get_log_stats(self) -> Dict[str, Any]:
        """获取日志统计信息"""
        try:
            # 从buffer_manager获取缓存统计
            cache_stats = self.buffer_manager.get_cache_stats()

            # 获取当前日志级别
            current_level = self.log_manager.logger.min_level.name

            return {
                "log_file": self.log_manager.logger.log_file,
                "current_log_level": current_level,
                "cache_hits": cache_stats["cache_hits"],
                "cache_misses": cache_stats["cache_misses"],
                "hit_rate": cache_stats["hit_rate"],
            }
        except Exception as e:
            raise Exception(f"获取日志统计失败: {str(e)}")

    def flush_all(self):
        """刷新所有缓存到磁盘"""
        self.buffer_manager.flush_all()

    def list_all_indexes(self) -> Dict[str, Dict[str, Any]]:
        """列出所有索引，按表分组"""
        if not hasattr(self, 'index_manager') or not self.index_manager:
            return {}

        all_indexes = {}
        for table_name in self.catalog.list_tables():
            table_indexes = self.index_manager.get_table_indexes(table_name)
            if table_indexes:
                all_indexes[table_name] = {}
                for index_name in table_indexes:
                    index_info = self.index_manager.indexes.get(index_name)
                    if index_info:
                        all_indexes[table_name][index_name] = {
                            'column': index_info.column_name,
                            'unique': index_info.is_unique
                        }

        return all_indexes

    def close(self):
        """关闭数据库连接"""
        try:
            self.flush_all()
            # 关闭日志管理器
            if hasattr(self, "log_manager"):
                self.log_manager.close()
            print(f"数据库 {self.db_file} 已关闭")
        except Exception as e:
            print(f"关闭数据库时发生错误: {e}")


    def list_views(self) -> List[str]:
        """列出所有视图"""
        return list(self.catalog.views.keys())


    def get_view_info(self, view_name: str) -> Dict[str, Any]:
        """获取视图信息"""
        if view_name not in self.catalog.views:
            return {"error": f"视图 {view_name} 不存在"}

        return {
            "view_name": view_name,
            "definition": self.catalog.views[view_name],
            "is_view": True
        }


    def get_view_data(self, view_name: str, page: int = 1, page_size: int = 100) -> Dict[str, Any]:
        """获取视图数据（执行视图定义的SQL）"""
        if view_name not in self.catalog.views:
            return {"success": False, "message": f"视图 {view_name} 不存在"}

        try:
            # 执行视图定义的SQL
            view_sql = self.catalog.views[view_name]
            result = self.execute_sql(view_sql)

            if not result.get("success"):
                return result

            # 分页处理
            data = result.get("data", [])
            total_count = len(data)
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            paged_data = data[start_idx:end_idx]

            return {
                "success": True,
                "data": {
                    "rows": paged_data,
                    "total": total_count,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": (total_count + page_size - 1) // page_size
                }
            }
        except Exception as e:
            return {"success": False, "message": f"执行视图查询失败: {str(e)}"}

    # 在你的主接口中添加选择执行方式的方法
    def execute_sql_with_options(self, sql: str, use_execution_plan: bool = False, explain_only: bool = False) -> Dict[
        str, Any]:
        """执行SQL的增强版本"""
        try:
            from sql.lexer import SQLLexer
            from sql.parser import SQLParser

            # 解析SQL
            lexer = SQLLexer(sql)
            tokens = lexer.tokenize()
            parser = SQLParser(tokens)
            ast = parser.parse()

            if explain_only:
                # 只生成执行计划，不执行
                return self.executor.explain_query(ast)
            elif use_execution_plan:
                # 使用执行计划执行
                return self.executor.execute_with_execution_plan(ast, use_plan=True)
            else:
                # 使用原有执行器
                return self.executor.execute(ast)

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": f"SQL执行失败: {str(e)}"
            }
