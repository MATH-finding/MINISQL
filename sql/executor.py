"""
SQL执行器
"""

from typing import List, Dict, Any, Optional, Union
import operator
from storage import Record
from catalog import DataType, ColumnDefinition, SystemCatalog
from table import TableManager
from .ast_nodes import *


class TableLockManager:
    """表级S/X锁（进程内共享，非阻塞，冲突时报错）。"""

    _read_locks: Dict[str, set] = {}
    _write_locks: Dict[str, Optional[int]] = {}

    @classmethod
    def acquire_shared(cls, table: str, session_id: int):
        writer = cls._write_locks.get(table)
        if writer and writer != session_id:
            raise ValueError(f"表 {table} 存在写锁，读锁获取失败")
        readers = cls._read_locks.setdefault(table, set())
        readers.add(session_id)

    @classmethod
    def acquire_exclusive(cls, table: str, session_id: int):
        writer = cls._write_locks.get(table)
        readers = cls._read_locks.get(table, set())
        if (writer and writer != session_id) or any(r != session_id for r in readers):
            raise ValueError(f"表 {table} 存在冲突锁，写锁获取失败")
        cls._write_locks[table] = session_id

    @classmethod
    def release_all_for_session(cls, session_id: int):
        # 释放读锁
        for table, readers in list(cls._read_locks.items()):
            if session_id in readers:
                readers.discard(session_id)
                if not readers:
                    cls._read_locks.pop(table, None)
        # 释放写锁
        for table, writer in list(cls._write_locks.items()):
            if writer == session_id:
                cls._write_locks.pop(table, None)


class TransactionManager:
    """MySQL风格的极简事务管理（支持回滚）：autocommit 与显式事务。"""

    def __init__(self):
        self._autocommit: bool = True
        self._in_txn: bool = False
        self._next_txn_id: int = 1
        self._current_txn_id: Optional[int] = None
        self._isolation_level: str = "READ COMMITTED"
        # REPEATABLE READ 快照：table -> List[dict]
        self._rr_snapshot: Dict[str, List[Dict[str, Any]]] = {}

    def set_autocommit(self, enabled: bool):
        # MySQL: SET autocommit=0 开启事务性上下文；=1 退出并隐式提交当前事务
        if enabled and self._in_txn:
            # 切换为autocommit=1时，隐式提交
            self.commit()
        self._autocommit = enabled
        # autocommit切换后清理快照
        if self._autocommit:
            self._rr_snapshot.clear()

    def autocommit(self) -> bool:
        return self._autocommit

    def begin(self) -> int:
        if self._in_txn:
            # MySQL 多次BEGIN通常报错或忽略，这里报错
            raise ValueError("已在事务中")
        self._in_txn = True
        self._current_txn_id = self._next_txn_id
        self._next_txn_id += 1
        # 新事务清空快照
        self._rr_snapshot.clear()
        return self._current_txn_id

    def commit(self):
        if not self._in_txn:
            # COMMIT 在非事务中为 no-op；为了明确，这里也允许静默成功
            return
        # TODO: 这里可集成WAL fsync
        self._in_txn = False
        self._current_txn_id = None
        self._rr_snapshot.clear()

    def rollback(self):
        # 支持回滚
        if not self._in_txn:
            raise ValueError("当前不在事务中，无法回滚")
        self._in_txn = False
        self._current_txn_id = None
        self._rr_snapshot.clear()

    def in_txn(self) -> bool:
        return self._in_txn

    def current_txn_id(self) -> Optional[int]:
        return self._current_txn_id

    def set_isolation_level(self, level: str):
        # 允许：READ UNCOMMITTED / READ COMMITTED / REPEATABLE READ / SERIALIZABLE
        self._isolation_level = level
        # 更改隔离级别时清空快照
        self._rr_snapshot.clear()

    def isolation_level(self) -> str:
        return self._isolation_level

    def get_rr_snapshot_for_table(self, table_name: str) -> Optional[List[Dict[str, Any]]]:
        return self._rr_snapshot.get(table_name)

    def set_rr_snapshot_for_table(self, table_name: str, rows: List[Dict[str, Any]]):
        self._rr_snapshot[table_name] = rows


class SQLExecutor:
    """SQL执行器，将AST转换为数据库操作（注释已在词法分析阶段去除）"""

    _next_session_id = 1

    def __init__(
        self, table_manager: TableManager, catalog: SystemCatalog, index_manager=None
    ):
        self.table_manager = table_manager
        self.catalog = catalog
        self.index_manager = index_manager
        self.txn = TransactionManager()
        # 事务内延迟写入缓冲：txn_id -> List[Tuple[table_name, record_data]]
        self._pending_inserts: Dict[int, List[Dict[str, Any]]] = {}
        # 会话ID（用于锁管理）
        self.session_id = SQLExecutor._next_session_id
        SQLExecutor._next_session_id += 1
        # Undo 日志：txn_id -> List[dict]（先进后出）
        self._undo_log: Dict[int, List[Dict[str, Any]]] = {}

        # 操作符映射
        self.comparison_ops = {
            "=": operator.eq,
            "!=": operator.ne,
            "<": operator.lt,
            "<=": operator.le,
            ">": operator.gt,
            ">=": operator.ge,
        }

    def _maybe_lock_shared(self, table_name: str):
        if self.txn.isolation_level() == "SERIALIZABLE":
            TableLockManager.acquire_shared(table_name, self.session_id)
            # 在autocommit下，语句结束释放

    def _maybe_lock_exclusive(self, table_name: str):
        if self.txn.isolation_level() == "SERIALIZABLE":
            TableLockManager.acquire_exclusive(table_name, self.session_id)

    def _maybe_release_autocommit_locks(self):
        if self.txn.isolation_level() == "SERIALIZABLE" and self.txn.autocommit():
            TableLockManager.release_all_for_session(self.session_id)

    def execute(self, ast: Statement) -> Dict[str, Any]:
        """执行SQL语句"""
        try:
            # DEBUG: 打印进入execute的AST类型
            # print(f"[EXECUTOR DEBUG] execute() with AST: {type(ast).__name__}")

            # 用户管理语句
            if isinstance(ast, CreateUserStatement):
                result = self._execute_create_user(ast)
            elif isinstance(ast, DropUserStatement):
                result = self._execute_drop_user(ast)
            elif isinstance(ast, GrantStatement):
                result = self._execute_grant(ast)
            elif isinstance(ast, RevokeStatement):
                result = self._execute_revoke(ast)
            # 表管理语句
            elif isinstance(ast, CreateTableStatement):
                result = self._execute_create_table(ast)
            elif isinstance(ast, DropTableStatement):
                result = self._execute_drop_table(ast)
            elif isinstance(ast, TruncateTableStatement):
                result = self._execute_truncate_table(ast)
            elif isinstance(ast, InsertStatement):
                result = self._execute_insert_immediate_undo(ast)
            elif isinstance(ast, SelectStatement):
                # 查询视图重写
                if (
                    isinstance(ast.from_table, str)
                    and hasattr(self.catalog, 'views')
                    and ast.from_table.lower() in self.catalog.views
                ):
                    # print(f"[EXECUTOR DEBUG] SELECT on view detected: view={ast.from_table}")
                    view_sql = self.catalog.get_view_definition(ast.from_table.lower())
                    # print(f"[EXECUTOR DEBUG] view SQL: {view_sql}")
                    from sql.lexer import SQLLexer
                    from sql.parser import SQLParser

                    lexer = SQLLexer(view_sql)
                    tokens = lexer.tokenize()
                    parser = SQLParser(tokens)
                    view_ast = parser.parse()
                    # print(f"[EXECUTOR DEBUG] parsed view AST: {type(view_ast).__name__} -> {view_ast}")

                    # 步骤 1: 执行视图定义，得到原始结果（list of dict）
                    view_result = self.execute(view_ast)
                    # print(f"[EXECUTOR DEBUG] view execution result meta: success={view_result.get('success')}, rows={len(view_result.get('data', []) ) if isinstance(view_result.get('data'), list) else 'N/A'}")
                    if not view_result.get("success") or not isinstance(
                        view_result.get("data"), list
                    ):
                        return view_result
                    current_rows = view_result["data"]

                    # 步骤 2: 应用外部WHERE过滤
                    if ast.where_clause:
                        # print(f"[EXECUTOR DEBUG] applying outer WHERE on {len(current_rows)} rows: {ast.where_clause}")
                        filtered_rows = []
                        for row in current_rows:
                            # 确保row是dict类型，如果不是则转换
                            row_dict = dict(row) if hasattr(row, '__dict__') or hasattr(row, 'data') else row
                            if self._evaluate_condition(ast.where_clause, row_dict):
                                filtered_rows.append(row_dict)
                        current_rows = filtered_rows
                        # print(f"[EXECUTOR DEBUG] rows after WHERE: {len(current_rows)}")

                    # 步骤 3: 投影
                    final_data = []
                    columns_to_project = (
                        view_ast.columns if ast.columns == ["*"] else ast.columns
                    )
                    is_select_all = columns_to_project == ["*"]
                    # print(f"[EXECUTOR DEBUG] projection columns: {columns_to_project}")
                    for row in current_rows:
                        if is_select_all:
                            proj = dict(row)
                        else:
                            proj = {}
                            for col in columns_to_project:
                                if isinstance(col, ColumnRef):
                                    col_name = col.column_name
                                else:
                                    col_name = str(col)
                                if col_name in row:
                                    proj[col_name] = row[col_name]
                        final_data.append(proj)
                    # print(f"[EXECUTOR DEBUG] final projected rows: {len(final_data)}")
                    return {
                        "type": "SELECT",
                        "success": True,
                        "data": final_data,
                        "message": f"查询成功，返回{len(final_data)}行",
                    }
                result = self._execute_select(ast)
            # 索引管理语句
            elif isinstance(ast, CreateIndexStatement):
                result = self._execute_create_index(ast)
            elif isinstance(ast, DropIndexStatement):
                result = self._execute_drop_index(ast)
            # 视图管理语句
            elif isinstance(ast, CreateViewStatement):
                result = self._execute_create_view(ast)
            elif isinstance(ast, DropViewStatement):
                result = self._execute_drop_view(ast)
            # 触发器管理语句
            elif isinstance(ast, CreateTriggerStatement):
                result = self._execute_create_trigger(ast)
            elif isinstance(ast, DropTriggerStatement):
                result = self._execute_drop_trigger(ast)
            # ALTER TABLE
            elif isinstance(ast, AlterTableStatement):
                result = self._execute_alter_table(ast)
            # 数据操作语句
            elif isinstance(ast, UpdateStatement):
                result = self._execute_update_with_undo(ast)
            elif isinstance(ast, DeleteStatement):
                result = self._execute_delete_with_undo(ast)
            # 事务管理语句
            elif isinstance(ast, BeginTransaction):
                txn_id = self.txn.begin()
                return {"type": "BEGIN", "success": True, "message": f"事务已开始 (id={txn_id})"}
            elif isinstance(ast, CommitTransaction):
                # 立即写入方案下，COMMIT 只需结束事务与释放资源
                active_id = self.txn.current_txn_id()
                self.txn.commit()
                if active_id is not None:
                    self._undo_log.pop(active_id, None)
                TableLockManager.release_all_for_session(self.session_id)
                return {"type": "COMMIT", "success": True, "message": "事务已提交"}
            elif isinstance(ast, RollbackTransaction):
                # 无活动事务时报错
                if not self.txn.in_txn():
                    return {"type": "ROLLBACK", "success": False, "message": "当前不在事务中，无法回滚"}
                self._rollback_playback()
                self._undo_log.pop(self.txn.current_txn_id(), None)
                TableLockManager.release_all_for_session(self.session_id)
                # 结束事务
                self.txn.commit()
                return {"type": "ROLLBACK", "success": True, "message": "事务已回滚"}
            elif isinstance(ast, SetAutocommit):
                switching_to_on = ast.enabled and self.txn.in_txn()
                if switching_to_on:
                    # MySQL 语义：切回 autocommit=1 会隐式提交当前事务
                    if self.txn.current_txn_id() is not None:
                        self._undo_log.pop(self.txn.current_txn_id(), None)
                self.txn.set_autocommit(ast.enabled)
                if ast.enabled:
                    TableLockManager.release_all_for_session(self.session_id)
                return {"type": "SET", "success": True, "message": f"AUTOCOMMIT={(1 if ast.enabled else 0)}"}
            elif isinstance(ast, SetIsolationLevel):
                self.txn.set_isolation_level(ast.level)
                TableLockManager.release_all_for_session(self.session_id)
                return {"type": "SET", "success": True, "message": f"ISOLATION LEVEL {ast.level}"}
            else:
                raise ValueError(f"不支持的语句类型: {type(ast)}")

            if self.txn.autocommit() and isinstance(
                ast, (InsertStatement, UpdateStatement, DeleteStatement, CreateTableStatement, DropIndexStatement, CreateIndexStatement, DropTableStatement, TruncateTableStatement)
            ):
                self._maybe_release_autocommit_locks()

            return result
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "data": None,
                "message": f"SQL执行失败: {str(e)}",
            }
            # 统一异常返回结构，避免KeyError
            

    # --- 新增：CHECK 评估（NULL 视为通过，不违反约束） ---
    def _evaluate_check(self, condition: Expression, record_data: Dict[str, Any]) -> bool:
        try:
            # 如果任一侧取值为 None，按 SQL 三值逻辑视为 UNKNOWN，不违反约束
            if isinstance(condition, BinaryOp):
                left_val = self._evaluate_expression(condition.left, record_data)
                right_val = self._evaluate_expression(condition.right, record_data)
                if left_val is None or right_val is None:
                    return True
            return self._evaluate_condition(condition, record_data)
        except Exception:
            # 任意异常都视为不违反（保守处理），避免插入失败
            return True
            

    def _execute_create_user(self, stmt: CreateUserStatement) -> Dict[str, Any]:
        """执行CREATE USER"""
        # 检查用户是否已存在
        if hasattr(self.catalog, 'list_users') and stmt.username in (self.catalog.list_users() or []):
            if getattr(stmt, 'if_not_exists', False):
                return {
                    "type": "CREATE_USER",
                    "username": stmt.username,
                    "success": True,
                    "message": f"用户 {stmt.username} 已存在，已忽略 (IF NOT EXISTS)",
                }
            else:
                return {"success": False, "message": f"用户 {stmt.username} 已存在"}
        success = self.catalog.create_user(stmt.username, stmt.password)

        if success:
            return {
                "type": "CREATE_USER",
                "username": stmt.username,
                "success": True,
                "message": f"用户 {stmt.username} 创建成功",
            }
        else:
            return {"success": False, "message": f"用户 {stmt.username} 已存在"}

    def _execute_drop_user(self, stmt: DropUserStatement) -> Dict[str, Any]:
        """执行DROP USER"""
        # 检查用户是否已存在
        if hasattr(self.catalog, 'list_users') and stmt.username not in (self.catalog.list_users() or []):
            if getattr(stmt, 'if_exists', False):
                return {
                    "type": "DROP_USER",
                    "username": stmt.username,
                    "success": True,
                    "message": f"用户 {stmt.username} 不存在，已忽略 (IF EXISTS)",
                }
            else:
                return {"success": False, "message": f"用户 {stmt.username} 不存在"}
        success = self.catalog.drop_user(stmt.username)

        if success:
            return {
                "type": "DROP_USER",
                "username": stmt.username,
                "success": True,
                "message": f"用户 {stmt.username} 删除成功",
            }
        else:
            return {
                "success": False,
                "message": f"用户 {stmt.username} 不存在或无法删除",
            }

    def _execute_create_view(self, stmt):
        # 检查视图是否已存在
        if hasattr(self.catalog, 'views') and stmt.view_name in getattr(self.catalog, 'views', {}):
            if getattr(stmt, 'if_not_exists', False):
                return {
                    "type": "CREATE_VIEW",
                    "view_name": stmt.view_name,
                    "success": True,
                    "message": f"视图 {stmt.view_name} 已存在，已忽略 (IF NOT EXISTS)",
                }
            else:
                raise ValueError(f"视图 {stmt.view_name} 已存在")
        self.catalog.create_view(stmt.view_name, stmt.view_definition)
        return {
            "type": "CREATE_VIEW",
            "view_name": stmt.view_name,
            "success": True,
            "message": f"视图 {stmt.view_name} 创建成功",
        }

    def _execute_drop_view(self, stmt):
        if hasattr(self.catalog, 'views') and stmt.view_name not in getattr(self.catalog, 'views', {}):
            if getattr(stmt, 'if_exists', False):
                return {
                    "type": "DROP_VIEW",
                    "view_name": stmt.view_name,
                    "success": True,
                    "message": f"视图 {stmt.view_name} 不存在，已忽略 (IF EXISTS)",
                }
            else:
                raise ValueError(f"视图 {stmt.view_name} 不存在")
        self.catalog.drop_view(stmt.view_name)
        return {
            "type": "DROP_VIEW",
            "view_name": stmt.view_name,
            "success": True,
            "message": f"视图 {stmt.view_name} 删除成功",
        }

    def _execute_grant(self, stmt: GrantStatement) -> Dict[str, Any]:
        """执行GRANT"""
        success = self.catalog.grant_privilege(
            stmt.username, stmt.table_name, stmt.privilege
        )

        if success:
            return {
                "type": "GRANT",
                "privilege": stmt.privilege,
                "table_name": stmt.table_name,
                "username": stmt.username,
                "success": True,
                "message": f"成功授予用户 {stmt.username} 表 {stmt.table_name} 的 {stmt.privilege} 权限",
            }
        else:
            return {
                "success": False,
                "message": f"授权失败：用户 {stmt.username} 不存在",
            }

    def _execute_revoke(self, stmt: RevokeStatement) -> Dict[str, Any]:
        """执行REVOKE"""
        success = self.catalog.revoke_privilege(
            stmt.username, stmt.table_name, stmt.privilege
        )

        if success:
            return {
                "type": "REVOKE",
                "privilege": stmt.privilege,
                "table_name": stmt.table_name,
                "username": stmt.username,
                "success": True,
                "message": f"成功撤销用户 {stmt.username} 表 {stmt.table_name} 的 {stmt.privilege} 权限",
            }
        else:
            return {
                "success": False,
                "message": f"撤权失败：用户 {stmt.username} 不存在或无此权限",
            }

    # 立即写入 + Undo 的 INSERT
    def _execute_insert_immediate_undo(self, stmt: InsertStatement) -> Dict[str, Any]:
        """执行INSERT - 统一唯一性索引校验，支持DEFAULT、CHECK、FOREIGN KEY + 事务Undo"""
        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise ValueError(f"表 {stmt.table_name} 不存在")

        # 执行BEFORE INSERT触发器
        before_results = self._execute_triggers(stmt.table_name, "INSERT", "BEFORE")
        
        # 检查BEFORE触发器是否都成功
        for result in before_results:
            if not result.get("success", False):
                return {
                    "type": "INSERT",
                    "success": False,
                    "message": f"BEFORE INSERT触发器失败: {result.get('message', '')}"
                }

        # SERIALIZABLE: 写锁
        self._maybe_lock_exclusive(stmt.table_name)

        inserted_count = 0
        for value_row in stmt.values:
            record_data: Dict[str, Any] = {}
            if stmt.columns:
                if len(stmt.columns) != len(value_row):
                    raise ValueError("列数和值数不匹配")
                for i, col_name in enumerate(stmt.columns):
                    value = self._evaluate_expression(value_row[i], {})
                    record_data[col_name] = value
            else:
                if len(value_row) != len(schema.columns):
                    raise ValueError("值数和表列数不匹配")
                for i, column in enumerate(schema.columns):
                    value = self._evaluate_expression(value_row[i], {})
                    record_data[column.name] = value

            # 补全DEFAULT值
            for column in schema.columns:
                if column.name not in record_data or record_data[column.name] is None:
                    if hasattr(column, 'default') and column.default is not None:
                        # 类型转换：如果是数字类型，转换为int/float
                        if column.data_type.name in ("INTEGER", "BIGINT", "TINYINT"):
                            record_data[column.name] = int(column.default)
                        elif column.data_type.name in ("FLOAT", "DECIMAL"):
                            record_data[column.name] = float(column.default)
                        else:
                            record_data[column.name] = column.default
                # 如果依然没有值，且不是nullable，报错
                if column.name not in record_data and not column.nullable:
                    raise ValueError(f"列 {column.name} 不能为空")

            # 获取所有唯一性索引（主键、UNIQUE列、唯一索引、复合唯一索引）
            unique_indexes = []
            if self.index_manager and hasattr(self.index_manager, 'get_unique_indexes_for_table'):
                try:
                    unique_indexes = self.index_manager.get_unique_indexes_for_table(stmt.table_name)
                except:
                    unique_indexes = []

            # 插入前校验所有唯一性约束
            for index in unique_indexes:
                index_keys = [record_data.get(col) for col in index.columns]
                if all(key is not None for key in index_keys):
                    if hasattr(self.index_manager, 'lookup'):
                        existing = self.index_manager.lookup(index.name, index_keys)
                    else:
                        # 简化的唯一性检查：扫描表
                        all_records = self.table_manager.scan_table(stmt.table_name)
                        existing = any(
                            all(record.data.get(col) == key for col, key in zip(index.columns, index_keys))
                            for record in all_records
                        )
                    
                    if existing:
                        constraint_columns = ", ".join(index.columns)
                        raise ValueError(
                            f"唯一约束冲突: 列 '{constraint_columns}' 的值 '{index_keys}' 已存在"
                        )

            # 校验CHECK约束
            for column in schema.columns:
                if hasattr(column, 'check') and column.check is not None:
                    context = record_data.copy()
                    if not self._evaluate_condition(column.check, context):
                        raise ValueError(f"CHECK约束不满足: {column.name}")
            if hasattr(schema, 'check_constraints'):
                for check_expr in schema.check_constraints:
                    context = record_data.copy()
                    if not self._evaluate_condition(check_expr, context):
                        raise ValueError("表级CHECK约束不满足")

            # 校验FOREIGN KEY约束
            for column in schema.columns:
                if hasattr(column, 'foreign_key') and column.foreign_key:
                    ref_value = record_data.get(column.name)
                    if ref_value is not None:
                        ref_table = column.foreign_key["ref_table"]
                        ref_column = column.foreign_key["ref_column"]
                        ref_schema = self.catalog.get_table_schema(ref_table)
                        if not ref_schema:
                            raise ValueError(f"外键引用表不存在: {ref_table}")
                        found = False
                        # 这里的全表扫描效率很低，未来可以用索引优化
                        for ref_record in self.table_manager.scan_table(ref_table):
                            if ref_record.data.get(ref_column) == ref_value:
                                found = True
                                break
                        if not found:
                            raise ValueError(
                                f"外键约束不满足: {column.name} -> {ref_table}({ref_column})"
                            )

            # 实际插入
            page_id, ridx = None, None
            record_id = None
            
            if hasattr(self.table_manager, 'insert_record_with_location'):
                loc = self.table_manager.insert_record_with_location(stmt.table_name, record_data)
                if not loc:
                    raise ValueError("插入记录失败")
                page_id, ridx = loc
                record_id = (page_id, ridx)
            else:
                # 直接用insert_record返回(page_id, ridx)
                page_id, ridx = self.table_manager.insert_record(stmt.table_name, record_data)
                record_id = (page_id, ridx)

            # 索引维护
            if self.index_manager:
                if hasattr(self.index_manager, 'insert_into_indexes'):
                    self.index_manager.insert_into_indexes(stmt.table_name, record_data, record_id)
                else:
                    # 兼容老版本索引管理器
                    table_indexes = self.index_manager.get_table_indexes(stmt.table_name)
                    for index_name in table_indexes:
                        index_info = self.index_manager.indexes.get(index_name)
                        if index_info and index_info.column_name in record_data:
                            btree = self.index_manager.get_index(index_name)
                            if btree:
                                key = record_data[index_info.column_name]
                                btree.insert(key, record_id)

            # 若在事务中，记录补偿删除的 Undo
            if self.txn.in_txn():
                if page_id is not None and ridx is not None:
                    self._push_undo({
                        "type": "INSERT",
                        "table": stmt.table_name,
                        "page_id": page_id,
                        "index": ridx,
                    })

            inserted_count += 1

        # 执行AFTER INSERT触发器
        after_results = self._execute_triggers(stmt.table_name, "INSERT", "AFTER")

        return {
            "type": "INSERT",
            "table_name": stmt.table_name,
            "rows_inserted": inserted_count,
            "success": True,
            "trigger_results": {
                "before": before_results,
                "after": after_results
            },
            "message": f"成功插入 {inserted_count} 行到表 {stmt.table_name}",
        }

    def _execute_insert(self, stmt: InsertStatement) -> Dict[str, Any]:
        """执行INSERT - 修复版，正确处理记录ID（兼容版本）"""
        return self._execute_insert_immediate_undo(stmt)

    def _apply_commit(self):
        """提交当前事务的延迟写入（只处理INSERT）并更新索引"""
        txn_id = self.txn.current_txn_id()
        if txn_id is None:
            return
        pending = self._pending_inserts.get(txn_id, [])
        if not pending:
            return
        applied = 0
        for item in pending:
            table_name = item["table"]
            data = item["data"]
            # 实际插入（拿到RID）
            if hasattr(self.table_manager, 'insert_record_with_location'):
                loc = self.table_manager.insert_record_with_location(table_name, data)
                if not loc:
                    continue
                page_id, ridx = loc
                # 索引维护：写入RID
                if self.index_manager:
                    self.index_manager.insert_into_indexes(table_name, data, (page_id, ridx))
            applied += 1
        # 清理
        self._pending_inserts.pop(txn_id, None)

    def _discard_pending(self):
        txn_id = self.txn.current_txn_id()
        if txn_id is None:
            return
        self._pending_inserts.pop(txn_id, None)

    def _ensure_undo_stack(self) -> Optional[int]:
        txn_id = self.txn.current_txn_id()
        if txn_id is None:
            return None
        if txn_id not in self._undo_log:
            self._undo_log[txn_id] = []
        return txn_id

    def _push_undo(self, entry: Dict[str, Any]):
        txn_id = self._ensure_undo_stack()
        if txn_id is None:
            return
        self._undo_log[txn_id].append(entry)

    def _rollback_playback(self):
        txn_id = self.txn.current_txn_id()
        if txn_id is None:
            return
        stack = self._undo_log.get(txn_id, [])
        while stack:
            entry = stack.pop()
            typ = entry.get("type")
            table = entry.get("table")
            if typ == "UPDATE":
                page_id = entry["page_id"]
                idx = entry["index"]
                old_data = entry["old_data"]
                if hasattr(self.table_manager, 'update_at'):
                    self.table_manager.update_at(table, page_id, idx, old_data)
            elif typ == "DELETE":
                # 将旧值插回；可能插入到新位置
                old_data = entry["old_data"]
                if hasattr(self.table_manager, 'insert_record_with_location'):
                    self.table_manager.insert_record_with_location(table, old_data)
            elif typ == "INSERT":
                # 如果将来改为立即写入插入，这里需要补偿删除
                page_id = entry.get("page_id")
                idx = entry.get("index")
                if page_id is not None and idx is not None and hasattr(self.table_manager, 'delete_at'):
                    self.table_manager.delete_at(table, page_id, idx)
            else:
                pass

    def _execute_create_table(self, stmt: CreateTableStatement) -> Dict[str, Any]:
        """执行CREATE TABLE，支持 IF NOT EXISTS"""
        schema = self.catalog.get_table_schema(stmt.table_name)
        if schema:
            if getattr(stmt, 'if_not_exists', False):
                return {
                    "type": "CREATE_TABLE",
                    "table_name": stmt.table_name,
                    "success": True,
                    "message": f"表 {stmt.table_name} 已存在，已忽略 (IF NOT EXISTS)",
                }
            else:
                raise ValueError(f"表 {stmt.table_name} 已存在")

        columns = []

        for col_def in stmt.columns:
            # 转换数据类型
            data_type_map = {
                "INTEGER": DataType.INTEGER,
                "VARCHAR": DataType.VARCHAR,
                "FLOAT": DataType.FLOAT,
                "BOOLEAN": DataType.BOOLEAN,
                "CHAR": DataType.CHAR,
                "DECIMAL": DataType.DECIMAL,
                "DATE": DataType.DATE,
                "TIME": DataType.TIME,
                "DATETIME": DataType.DATETIME,
                "BIGINT": DataType.BIGINT,
                "TINYINT": DataType.TINYINT,
                "TEXT": DataType.TEXT,
            }

            data_type = data_type_map.get(col_def["type"])
            if not data_type:
                raise ValueError(f"不支持的数据类型: {col_def['type']}")

            # 解析约束
            nullable = True
            primary_key = False
            unique = False
            default = col_def.get("default")
            check = col_def.get("check")
            foreign_key = col_def.get("foreign_key")

            for constraint in col_def["constraints"]:
                if constraint == "NOT NULL":
                    nullable = False
                elif constraint == "PRIMARY KEY":
                    primary_key = True
                    nullable = False
                elif constraint == "UNIQUE":
                    unique = True

            column = ColumnDefinition(
                name=col_def["name"],
                data_type=data_type,
                max_length=col_def["length"],
                nullable=nullable,
                primary_key=primary_key,
                unique=unique,
                default=default,
                check=check,
                foreign_key=foreign_key,
            )
            columns.append(column)

        self.table_manager.create_table(stmt.table_name, columns)

        return {
            "type": "CREATE_TABLE",
            "table_name": stmt.table_name,
            "columns_created": len(columns),
            "success": True,
            "message": f"表 {stmt.table_name} 创建成功",
        }

    def _execute_drop_table(self, stmt: DropTableStatement) -> Dict[str, Any]:
        """执行DROP TABLE，支持 IF EXISTS"""
        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            if getattr(stmt, 'if_exists', False):
                return {
                    "type": "DROP_TABLE",
                    "table_name": stmt.table_name,
                    "success": True,
                    "message": f"表 {stmt.table_name} 不存在，已忽略 (IF EXISTS)",
                }
            else:
                raise ValueError(f"表 {stmt.table_name} 不存在")

        # 删除表相关的所有索引
        if self.index_manager:
            indexes_to_drop = []
            for index_name, index_info in self.index_manager.indexes.items():
                if index_info.table_name == stmt.table_name:
                    indexes_to_drop.append(index_name)

            for index_name in indexes_to_drop:
                self.index_manager.drop_index(index_name)

        # 删除表数据和元数据
        self.table_manager.drop_table(stmt.table_name)

        return {
            "type": "DROP_TABLE",
            "table_name": stmt.table_name,
            "success": True,
            "message": f"表 {stmt.table_name} 删除成功",
        }

    def _execute_truncate_table(self, stmt: TruncateTableStatement) -> Dict[str, Any]:
        """执行TRUNCATE TABLE"""
        # 检查表是否存在
        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise ValueError(f"表 {stmt.table_name} 不存在")

        # 清空表数据但保留结构
        cleared_count = self.table_manager.truncate_table(stmt.table_name)

        # 清理相关索引数据但保留索引结构
        if self.index_manager:
            table_indexes = self.index_manager.get_table_indexes(stmt.table_name)
            for index_name in table_indexes:
                btree = self.index_manager.get_index(index_name)
                if btree and hasattr(btree, 'clear'):
                    btree.clear()

        return {
            "type": "TRUNCATE_TABLE",
            "table_name": stmt.table_name,
            "rows_cleared": cleared_count,
            "success": True,
            "message": f"表 {stmt.table_name} 数据清空成功，共清除 {cleared_count} 行",
        }

    def _execute_insert(self, stmt: InsertStatement) -> Dict[str, Any]:
        """执行INSERT - 统一唯一性索引校验，支持DEFAULT、CHECK、FOREIGN KEY"""
        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise ValueError(f"表 {stmt.table_name} 不存在")

        inserted_count = 0

        for value_row in stmt.values:
            # 构建记录数据
            record_data = {}

            if stmt.columns:
                # 指定了列名
                if len(stmt.columns) != len(value_row):
                    raise ValueError("列数和值数不匹配")

                for i, col_name in enumerate(stmt.columns):
                    value = self._evaluate_expression(value_row[i], {})
                    record_data[col_name] = value
            else:
                # 未指定列名，按顺序插入所有列
                if len(value_row) != len(schema.columns):
                    raise ValueError("值数和表列数不匹配")

                for i, column in enumerate(schema.columns):
                    value = self._evaluate_expression(value_row[i], {})
                    record_data[column.name] = value

            # 补全DEFAULT值
            for column in schema.columns:
                if column.name not in record_data or record_data[column.name] is None:
                    if column.default is not None:
                        # 类型转换：如果是数字类型，转换为int/float
                        if column.data_type.name in ("INTEGER", "BIGINT", "TINYINT"):
                            record_data[column.name] = int(column.default)
                        elif column.data_type.name in ("FLOAT", "DECIMAL"):
                            record_data[column.name] = float(column.default)
                        else:
                            record_data[column.name] = column.default
                # 如果依然没有值，且不是nullable，报错
                if column.name not in record_data and not column.nullable:
                    raise ValueError(f"列 {column.name} 不能为空")

            # 获取所有唯一性索引（主键、UNIQUE列、唯一索引、复合唯一索引）
            unique_indexes = (
                self.index_manager.get_unique_indexes_for_table(stmt.table_name)
                if self.index_manager
                else []
            )

            # 插入前校验所有唯一性约束
            for index in unique_indexes:
                index_keys = [record_data.get(col) for col in index.columns]
                if all(key is not None for key in index_keys):
                    existing = self.index_manager.lookup(index.name, index_keys)
                    if existing:
                        constraint_columns = ", ".join(index.columns)
                        raise ValueError(
                            f"唯一约束冲突: 列 '{constraint_columns}' 的值 '{index_keys}' 已存在"
                        )

            # 校验CHECK约束
            for column in schema.columns:
                if column.check is not None:
                    context = record_data.copy()
                    if not self._evaluate_check(column.check, context):
                        raise ValueError(f"CHECK约束不满足: {column.name}")
            for check_expr in getattr(schema, "check_constraints", []):
                context = record_data.copy()
                if not self._evaluate_check(check_expr, context):
                    raise ValueError("表级CHECK约束不满足")

            # 校验FOREIGN KEY约束
            for column in schema.columns:
                if column.foreign_key:
                    ref_value = record_data.get(column.name)
                    if ref_value is not None:
                        ref_table = column.foreign_key["ref_table"]
                        ref_column = column.foreign_key["ref_column"]
                        ref_schema = self.catalog.get_table_schema(ref_table)
                        if not ref_schema:
                            raise ValueError(f"外键引用表不存在: {ref_table}")
                        found = False
                        # 这里的全表扫描效率很低，未来可以用索引优化
                        for ref_record in self.table_manager.scan_table(ref_table):
                            if ref_record.data.get(ref_column) == ref_value:
                                found = True
                                break
                        if not found:
                            raise ValueError(
                                f"外键约束不满足: {column.name} -> {ref_table}({ref_column})"
                            )
            # 插入记录
            try:
                record_id = self.table_manager.insert_record(
                    stmt.table_name, record_data
                )

                # 新增：如果有索引管理器，同时更新索引
                if self.index_manager:
                    self.index_manager.insert_into_indexes(
                        stmt.table_name, record_data, record_id
                    )

                inserted_count += 1
            except Exception as e:
                raise ValueError(f"插入记录失败: {str(e)}")

        return {
            "type": "INSERT",
            "table_name": stmt.table_name,
            "rows_inserted": inserted_count,
            "success": True,
            "message": f"成功插入 {inserted_count} 行到表 {stmt.table_name}",
        }

    # 合并后的 _execute_select 函数
    def _execute_select(self, stmt: SelectStatement) -> Dict[str, Any]:
        """执行SELECT - 扩展支持JOIN和聚合函数 + 事务隔离级别"""

        # 递归获取结果集
        def eval_from(from_table):
            if isinstance(from_table, str):
                # 单表
                schema = self.catalog.get_table_schema(from_table)
                if not schema:
                    raise ValueError(f"表 {from_table} 不存在")

                # SERIALIZABLE: 申请读锁
                self._maybe_lock_shared(from_table)

                # 读取基线集合：取决于隔离级别
                base_records: List[Record] = []
                level = self.txn.isolation_level()
                in_txn_ctx = self.txn.in_txn() and not self.txn.autocommit()

                # REPEATABLE READ / SERIALIZABLE: 第一次读时拍快照
                if in_txn_ctx and level in ("REPEATABLE READ", "SERIALIZABLE"):
                    snap = self.txn.get_rr_snapshot_for_table(from_table)
                    if snap is None:
                        committed_rows = self.table_manager.scan_table(from_table)
                        snapshot_rows = [dict(r.data) for r in committed_rows]
                        self.txn.set_rr_snapshot_for_table(from_table, snapshot_rows)
                        snap = snapshot_rows
                    # 基线用快照
                    base_dict_rows = snap
                    base_records = [Record(dict(row)) for row in base_dict_rows]
                else:
                    # READ COMMITTED / READ UNCOMMITTED: 每次读最新已提交
                    committed_rows = self.table_manager.scan_table(from_table)
                    base_records = committed_rows

                # 当前事务缓冲数据（仅当前会话可见）
                extra_rows: List[Record] = []
                if in_txn_ctx:
                    txn_id = self.txn.current_txn_id()
                    for item in self._pending_inserts.get(txn_id, []):
                        if item["table"] == from_table:
                            row = item["data"]
                            extra_rows.append(Record(dict(row)))

                return base_records + extra_rows, from_table
            elif isinstance(from_table, JoinClause):
                # JOIN
                left_records, left_name = eval_from(from_table.left)
                right_records, right_name = eval_from(from_table.right)
                result = []
                for lrow in left_records:
                    for rrow in right_records:
                        # 合并两表数据，字段加前缀
                        merged = {}
                        for k, v in lrow.data.items():
                            merged[f"{left_name}.{k}"] = v
                        for k, v in rrow.data.items():
                            merged[f"{right_name}.{k}"] = v
                        # ON条件上下文
                        if self._evaluate_condition(from_table.on, merged):
                            result.append(type(lrow)(merged))
                return result, f"({left_name} {from_table.join_type} JOIN {right_name})"
            else:
                raise ValueError("未知的from_table类型")

        all_records, from_name = eval_from(stmt.from_table)

        # WHERE过滤
        filtered_records = []
        for record in all_records:
            context = record.data.copy()
            # 对于单表，去除所有表前缀（兼容视图嵌套）
            if isinstance(stmt.from_table, str):
                context = {k.split(".")[-1]: v for k, v in context.items()}
            if stmt.where_clause is None or self._evaluate_condition(stmt.where_clause, context):
                # 只保留过滤后的context，便于后续投影
                record._filtered_context = context
                filtered_records.append(record)

        # 如果存在 GROUP BY
        if getattr(stmt, 'group_by', None):
            # 构建分组
            groups: Dict[tuple, List[Any]] = {}
            group_key_names: List[str] = []
            # 预先解析 group key 名称（按列名）
            for g in stmt.group_by:
                group_key_names.append(g.column_name if isinstance(g, ColumnRef) else str(g))

            def build_group_key(ctx: Dict[str, Any]) -> tuple:
                keys = []
                for g in stmt.group_by:
                    if isinstance(g, ColumnRef):
                        keys.append(self._evaluate_expression(g, ctx))
                    else:
                        # 简化实现：只支持列名
                        keys.append(ctx.get(str(g)))
                return tuple(keys)

            for rec in filtered_records:
                ctx = getattr(rec, '_filtered_context', rec.data)
                key = build_group_key(ctx)
                groups.setdefault(key, []).append(ctx)

            # 判断是否含聚合
            has_agg = any(isinstance(c, AggregateFunction) for c in stmt.columns)

            result_records: List[Dict[str, Any]] = []
            if has_agg:
                # 逐组计算聚合
                for key_tuple, rows in groups.items():
                    row_out: Dict[str, Any] = {}
                    # 输出分组键
                    for idx, name in enumerate(group_key_names):
                        row_out[name] = key_tuple[idx]

                    for col in stmt.columns:
                        if isinstance(col, AggregateFunction):
                            func = col.func_name.upper()
                            arg = col.arg
                            if func == "COUNT":
                                if arg == "*":
                                    row_out["COUNT"] = len(rows)
                                elif isinstance(arg, ColumnRef):
                                    non_null = 0
                                    for r in rows:
                                        val = self._evaluate_expression(arg, r)
                                        if val is not None:
                                            non_null += 1
                                    row_out["COUNT"] = non_null
                                else:
                                    raise ValueError("COUNT参数不支持")
                            elif func == "SUM":
                                if isinstance(arg, ColumnRef):
                                    vals = [self._evaluate_expression(arg, r) for r in rows]
                                    row_out["SUM"] = sum(v for v in vals if v is not None)
                                else:
                                    raise ValueError("SUM参数不支持")
                            elif func == "AVG":
                                if isinstance(arg, ColumnRef):
                                    vals = [self._evaluate_expression(arg, r) for r in rows]
                                    vals = [v for v in vals if v is not None]
                                    row_out["AVG"] = (sum(vals) / len(vals) if vals else None)
                                else:
                                    raise ValueError("AVG参数不支持")
                            elif func == "MIN":
                                if isinstance(arg, ColumnRef):
                                    vals = [self._evaluate_expression(arg, r) for r in rows]
                                    vals = [v for v in vals if v is not None]
                                    row_out["MIN"] = (min(vals) if vals else None)
                                else:
                                    raise ValueError("MIN参数不支持")
                            elif func == "MAX":
                                if isinstance(arg, ColumnRef):
                                    vals = [self._evaluate_expression(arg, r) for r in rows]
                                    vals = [v for v in vals if v is not None]
                                    row_out["MAX"] = (max(vals) if vals else None)
                                else:
                                    raise ValueError("MAX参数不支持")
                            else:
                                raise ValueError(f"不支持的聚合函数: {func}")
                        elif isinstance(col, ColumnRef):
                            # 非聚合列必须在分组键中
                            if col.column_name in group_key_names:
                                row_out[col.column_name] = row_out.get(col.column_name)
                            else:
                                raise ValueError("GROUP BY 查询中，非聚合列必须包含在分组键中")
                        elif isinstance(col, str) and col == "*":
                            # 忽略 * 在分组+聚合场景
                            pass
                        else:
                            raise ValueError("GROUP BY 查询的列仅支持分组列或聚合函数")
                    result_records.append(row_out)
            else:
                # 无聚合：返回唯一分组键
                for key_tuple in groups:
                    row_out = {}
                    for idx, name in enumerate(group_key_names):
                        row_out[name] = key_tuple[idx]
                    result_records.append(row_out)

        else:
            # 检查是否有聚合函数（无分组 -> 全表聚合）
            if any(isinstance(col, AggregateFunction) for col in stmt.columns):
                agg_result = {}
                for col in stmt.columns:
                    if isinstance(col, AggregateFunction):
                        func = col.func_name.upper()
                        arg = col.arg
                        if func == "COUNT":
                            if arg == "*":
                                agg_result["COUNT"] = len(filtered_records)
                            elif isinstance(arg, ColumnRef):
                                non_null = 0
                                for r in filtered_records:
                                    ctx = getattr(r, '_filtered_context', r.data)
                                    val = self._evaluate_expression(arg, ctx)
                                    if val is not None:
                                        non_null += 1
                                agg_result["COUNT"] = non_null
                            else:
                                raise ValueError("COUNT参数不支持")
                        elif func == "SUM":
                            if isinstance(arg, ColumnRef):
                                values = [
                                    self._evaluate_expression(arg, getattr(r, '_filtered_context', r.data))
                                    for r in filtered_records
                                ]
                                agg_result["SUM"] = sum(v for v in values if v is not None)
                            else:
                                raise ValueError("SUM参数不支持")
                        elif func == "AVG":
                            if isinstance(arg, ColumnRef):
                                values = [
                                    self._evaluate_expression(arg, getattr(r, '_filtered_context', r.data))
                                    for r in filtered_records
                                ]
                                values = [v for v in values if v is not None]
                                agg_result["AVG"] = (
                                    sum(values) / len(values) if values else None
                                )
                            else:
                                raise ValueError("AVG参数不支持")
                        elif func == "MIN":
                            if isinstance(arg, ColumnRef):
                                values = [
                                    self._evaluate_expression(arg, getattr(r, '_filtered_context', r.data))
                                    for r in filtered_records
                                ]
                                values = [v for v in values if v is not None]
                                agg_result["MIN"] = min(values) if values else None
                            else:
                                raise ValueError("MIN参数不支持")
                        elif func == "MAX":
                            if isinstance(arg, ColumnRef):
                                values = [
                                    self._evaluate_expression(arg, getattr(r, '_filtered_context', r.data))
                                    for r in filtered_records
                                ]
                                values = [v for v in values if v is not None]
                                agg_result["MAX"] = max(values) if values else None
                            else:
                                raise ValueError("MAX参数不支持")
                        else:
                            raise ValueError(f"不支持的聚合函数: {func}")
                result_records = [agg_result]
            else:
                # 选择列（原有逻辑）
                result_records = []
                for record in filtered_records:
                    context = getattr(record, '_filtered_context', record.data)
                    if stmt.columns == ["*"]:
                        # 返回所有字段的实际值
                        result_records.append(dict(context))
                    else:
                        selected_data = {}
                        for col in stmt.columns:
                            if isinstance(col, ColumnRef):
                                col_name = col.column_name
                                if col_name in context:
                                    selected_data[col_name] = context[col_name]
                                else:
                                    matches = [
                                        k
                                        for k in record.data
                                        if k.endswith(f".{col.column_name}")
                                        or k == col.column_name
                                    ]
                                    if len(matches) == 1:
                                        selected_data[col_name] = context[matches[0]]
                                    elif len(matches) == 0:
                                        raise ValueError(f"列 {col_name} 不存在")
                                    else:
                                        raise ValueError(f"列 {col.column_name} 不明确，请加表前缀")
                            elif isinstance(col, AggregateFunction):
                                raise ValueError("聚合函数只能单独出现在SELECT列表中")
                            else:
                                if col in context:
                                    selected_data[col] = context[col]
                                else:
                                    raise ValueError(f"列 {col} 不存在")
                        result_records.append(selected_data)
        # print(f"[EXECUTOR DEBUG] _execute_select: rows_projected={len(result_records)}")

        # ORDER BY 排序
        if getattr(stmt, 'order_by', None):
            def sort_key(row: Dict[str, Any]):
                keys = []
                for item in stmt.order_by:
                    name = item.expr.column_name if isinstance(item.expr, ColumnRef) else str(item.expr)
                    keys.append(row.get(name))
                return tuple(keys)
            # Python的排序无法为不同列设置不同方向一次完成，采用链式稳定排序（从次关键字到主关键字）
            for item in reversed(stmt.order_by):
                name = item.expr.column_name if isinstance(item.expr, ColumnRef) else str(item.expr)
                reverse = (item.direction.upper() == "DESC")
                result_records.sort(key=lambda r: r.get(name), reverse=reverse)

        return {
            "type": "SELECT",
            "table_name": from_name,
            "rows_returned": len(result_records),
            "data": result_records,
            "success": True,
            "message": f"查询返回 {len(result_records)} 行",
        }

    def _execute_create_index(self, stmt: CreateIndexStatement) -> Dict[str, Any]:
        """执行CREATE INDEX，支持 IF NOT EXISTS"""
        # 检查索引是否已存在
        if self.index_manager and stmt.index_name in getattr(self.index_manager, 'indexes', {}):
            if getattr(stmt, 'if_not_exists', False):
                return {
                    "type": "CREATE_INDEX",
                    "index_name": stmt.index_name,
                    "success": True,
                    "message": f"索引 {stmt.index_name} 已存在，已忽略 (IF NOT EXISTS)",
                }
            else:
                raise ValueError(f"索引 {stmt.index_name} 已存在")
        if not self.index_manager:
            raise ValueError("索引管理器未初始化")

        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise ValueError(f"表 {stmt.table_name} 不存在")

        column_exists = any(col.name == stmt.column_name for col in schema.columns)
        if not column_exists:
            raise ValueError(f"列 {stmt.column_name} 不存在于表 {stmt.table_name}")

        success = self.index_manager.create_index(
            stmt.index_name, stmt.table_name, stmt.column_name, stmt.is_unique
        )

        if not success:
            raise ValueError(f"索引 {stmt.index_name} 已存在")

        # 为现有数据构建索引，使用正确的记录ID
        self._build_index_for_existing_data(
            stmt.index_name, stmt.table_name, stmt.column_name
        )

        return {
            "type": "CREATE_INDEX",
            "index_name": stmt.index_name,
            "table_name": stmt.table_name,
            "column_name": stmt.column_name,
            "success": True,
            "message": f"索引 {stmt.index_name} 创建成功",
        }

    def _execute_drop_index(self, stmt: DropIndexStatement) -> Dict[str, Any]:
        """执行DROP INDEX，支持 IF EXISTS"""
        if self.index_manager and stmt.index_name not in getattr(self.index_manager, 'indexes', {}):
            if getattr(stmt, 'if_exists', False):
                return {
                    "type": "DROP_INDEX",
                    "index_name": stmt.index_name,
                    "success": True,
                    "message": f"索引 {stmt.index_name} 不存在，已忽略 (IF EXISTS)",
                }
            else:
                raise ValueError(f"索引 {stmt.index_name} 不存在")
        if not self.index_manager:
            raise ValueError("索引管理器未初始化")

        success = self.index_manager.drop_index(stmt.index_name)
        if not success:
            raise ValueError(f"索引 {stmt.index_name} 不存在")

        return {
            "type": "DROP_INDEX",
            "index_name": stmt.index_name,
            "success": True,
            "message": f"索引 {stmt.index_name} 删除成功",
        }

    def _build_index_for_existing_data(self, index_name: str, table_name: str, column_name: str):
        """为现有数据构建索引 - 修复版"""
        btree = self.index_manager.get_index(index_name)
        if not btree:
            return

        all_records = self.table_manager.scan_table(table_name)

        for record_id, record in enumerate(all_records):
            if column_name in record.data:
                key = record.data[column_name]
                btree.insert(key, record_id)  # 存储正确的记录ID

    def _try_index_scan(self, table_name: str, where_clause: Expression) -> Optional[List[Record]]:
        """尝试使用索引扫描优化查询 - 修复版"""
        if not self.index_manager:
            return None

        index_info = self._analyze_where_for_index(table_name, where_clause)
        if not index_info:
            return None

        index_name, column_name, operator, value = index_info
        btree = self.index_manager.get_index(index_name)
        if not btree:
            return None

        if operator == "=":
            # 精确查找
            record_id = btree.search(value)
            if record_id is not None:
                if isinstance(record_id, (list, tuple)):
                    record_ids = list(record_id)
                else:
                    record_ids = [record_id]
                return self._get_records_by_ids(table_name, record_ids)
        elif operator in ["<", "<=", ">", ">="]:
            # 范围查询
            if operator in ["<", "<="]:
                results = btree.range_search(float("-inf"), value)
            else:
                results = btree.range_search(value, float("inf"))

            record_ids = [record_id for _, record_id in results]
            return self._get_records_by_ids(table_name, record_ids)

        return None

    def _analyze_where_for_index(self, table_name: str, where_clause: Expression) -> Optional[tuple]:
        """分析WHERE条件，看是否可以使用索引"""
        if not isinstance(where_clause, BinaryOp):
            return None

        if isinstance(where_clause.left, ColumnRef) and isinstance(where_clause.right, Literal):
            column_name = where_clause.left.column_name
            operator = where_clause.operator
            value = where_clause.right.value

            available_indexes = self.index_manager.get_table_indexes(table_name)
            for index_name in available_indexes:
                index_info = self.index_manager.indexes.get(index_name)
                if index_info and index_info.column_name == column_name:
                    return (index_name, column_name, operator, value)

        return None

    def _get_records_by_ids(self, table_name: str, record_ids: List[int]) -> List[Record]:
        """根据记录ID获取记录 - 修复版"""
        if hasattr(self.table_manager, 'scan_table_with_locations'):
            # 使用位置信息的版本
            rid_set = set(record_ids)
            results: List[Record] = []
            for page_id, idx, rec in self.table_manager.scan_table_with_locations(table_name):
                if (page_id, idx) in rid_set:
                    results.append(rec)
            return results
        else:
            # 兼容老版本
            all_records = self.table_manager.scan_table(table_name)
            result = []
            for record_id in record_ids:
                if 0 <= record_id < len(all_records):
                    result.append(all_records[record_id])
            return result

    def _execute_update_with_undo(self, stmt: UpdateStatement) -> Dict[str, Any]:
        """执行UPDATE with Undo支持"""
        # SERIALIZABLE: 写锁
        self._maybe_lock_exclusive(stmt.table_name)

        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise ValueError(f"表 {stmt.table_name} 不存在")

        # 执行BEFORE UPDATE触发器
        before_results = self._execute_triggers(stmt.table_name, "UPDATE", "BEFORE")
        
        # 检查BEFORE触发器是否都成功
        for result in before_results:
            if not result.get("success", False):
                return {
                    "type": "UPDATE",
                    "success": False,
                    "message": f"BEFORE UPDATE触发器失败: {result.get('message', '')}"
                }

        # 预计算 updates 值
        updates: Dict[str, Any] = {}
        for set_clause in stmt.set_clauses:
            column_name = set_clause["column"]
            value_expr = set_clause["value"]
            if isinstance(value_expr, Literal):
                updates[column_name] = value_expr.value
            else:
                raise ValueError("UPDATE暂时只支持字面量值")

        updated = 0

        # 如果支持位置更新
        if hasattr(self.table_manager, 'scan_table_with_locations') and hasattr(self.table_manager, 'update_at'):
            for page_id, idx, rec in self.table_manager.scan_table_with_locations(stmt.table_name):
                if stmt.where_clause is None or self._evaluate_condition(stmt.where_clause, rec.data):
                    old_data = dict(rec.data)
                    new_data = dict(old_data)
                    new_data.update(updates)
                    if self.table_manager.update_at(stmt.table_name, page_id, idx, new_data):
                        updated += 1
                        # 写入undo
                        self._push_undo({
                            "type": "UPDATE",
                            "table": stmt.table_name,
                            "page_id": page_id,
                            "index": idx,
                            "old_data": old_data,
                        })
        else:
            # 兼容版本
            condition_func = None
            if stmt.where_clause:
                condition_func = lambda record_data: self._evaluate_condition(stmt.where_clause, record_data)

            if hasattr(self.table_manager, 'update_records'):
                updated = self.table_manager.update_records(stmt.table_name, updates, condition_func)

        # 执行AFTER UPDATE触发器
        after_results = self._execute_triggers(stmt.table_name, "UPDATE", "AFTER")

        return {
            "type": "UPDATE",
            "table_name": stmt.table_name,
            "rows_updated": updated,
            "success": True,
            "message": f"成功更新 {updated} 行",
            "trigger_results": {
                "before": before_results,
                "after": after_results
            }
        }

    def _execute_update(self, stmt: UpdateStatement) -> Dict[str, Any]:
        """执行UPDATE语句（兼容版本）"""
        return self._execute_update_with_undo(stmt)

    def _execute_delete_with_undo(self, stmt: DeleteStatement) -> Dict[str, Any]:
        """执行DELETE with Undo支持"""
        # SERIALIZABLE: 写锁
        self._maybe_lock_exclusive(stmt.table_name)

        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise ValueError(f"表 {stmt.table_name} 不存在")

        # 执行BEFORE DELETE触发器
        before_results = self._execute_triggers(stmt.table_name, "DELETE", "BEFORE")
        
        # 检查BEFORE触发器是否都成功
        for result in before_results:
            if not result.get("success", False):
                return {
                    "type": "DELETE",
                    "success": False,
                    "message": f"BEFORE DELETE触发器失败: {result.get('message', '')}"
                }

        deleted = 0

        # 如果支持位置删除
        if hasattr(self.table_manager, 'scan_table_with_locations') and hasattr(self.table_manager, 'delete_at'):
            for page_id, idx, rec in self.table_manager.scan_table_with_locations(stmt.table_name):
                if stmt.where_clause is None or self._evaluate_condition(stmt.where_clause, rec.data):
                    old_data = dict(rec.data)
                    if self.table_manager.delete_at(stmt.table_name, page_id, idx):
                        deleted += 1
                        self._push_undo({
                            "type": "DELETE",
                            "table": stmt.table_name,
                            "page_id": page_id,
                            "index": idx,
                            "old_data": old_data,
                        })
        else:
            # 兼容版本
            condition_func = None
            if stmt.where_clause:
                condition_func = lambda record_data: self._evaluate_condition(stmt.where_clause, record_data)

            if hasattr(self.table_manager, 'delete_records'):
                deleted = self.table_manager.delete_records(stmt.table_name, condition_func)

        # 执行AFTER DELETE触发器
        after_results = self._execute_triggers(stmt.table_name, "DELETE", "AFTER")

        return {
            "type": "DELETE",
            "table_name": stmt.table_name,
            "rows_deleted": deleted,
            "success": True,
            "message": f"成功删除 {deleted} 行",
            "trigger_results": {
                "before": before_results,
                "after": after_results
            }
        }

    def _execute_delete(self, stmt: DeleteStatement) -> Dict[str, Any]:
        """执行DELETE语句（兼容版本）"""
        return self._execute_delete_with_undo(stmt)

    def _evaluate_expression(self, expr: Expression, context: Dict[str, Any]) -> Any:
        if isinstance(expr, Literal):
            return expr.value
        elif isinstance(expr, ColumnRef):
            if hasattr(expr, 'table_name') and expr.table_name:
                # 优先查找带前缀
                key = f"{expr.table_name}.{expr.column_name}"
                if key in context:
                    return context[key]
            # 回退查找无前缀
            col_name = expr.column_name if hasattr(expr, 'column_name') else str(expr)
            return context.get(col_name)
        else:
            raise ValueError(f"不支持的表达式类型: {type(expr)}")

    def _evaluate_condition(self, condition: Expression, record_data: Dict[str, Any]) -> bool:
        """评估WHERE条件"""
        if isinstance(condition, BinaryOp):
            left_val = self._evaluate_expression(condition.left, record_data)
            right_val = self._evaluate_expression(condition.right, record_data)

            op_func = self.comparison_ops.get(condition.operator)
            if not op_func:
                raise ValueError(f"不支持的操作符: {condition.operator}")

            return op_func(left_val, right_val)

        elif isinstance(condition, LogicalOp):
            left_result = self._evaluate_condition(condition.left, record_data)
            right_result = self._evaluate_condition(condition.right, record_data)

            if condition.operator == "AND":
                return left_result and right_result
            elif condition.operator == "OR":
                return left_result or right_result
            else:
                raise ValueError(f"不支持的逻辑操作符: {condition.operator}")

        else:
            raise ValueError(f"不支持的条件类型: {type(condition)}")

    def execute_sqls(self, sqls: str) -> list:
        """支持多条SQL（以英文分号分隔）依次执行，返回所有结果"""
        stmts = [s.strip() for s in sqls.split(';') if s.strip()]
        results = []
        for stmt in stmts:
            # 每条语句补英文分号
            if not stmt.endswith(';'):
                stmt += ';'
            try:
                from sql.lexer import SQLLexer
                from sql.parser import SQLParser
                lexer = SQLLexer(stmt)
                tokens = lexer.tokenize()
                parser = SQLParser(tokens)
                ast = parser.parse()
                res = self.execute(ast)
                results.append(res)
            except Exception as e:
                results.append({"success": False, "error": str(e), "message": f"SQL执行失败: {e}"})
        return results

    # =============== 触发器执行方法 ===============
    def _execute_create_trigger(self, ast: CreateTriggerStatement) -> Dict[str, Any]:
        """执行CREATE TRIGGER语句"""
        # 权限检查 - 需要CREATE权限
        if self.current_user and not self.catalog.check_privilege(self.current_user, ast.table_name, "CREATE"):
            return {"success": False, "message": f"用户 {self.current_user} 没有表 {ast.table_name} 的CREATE权限"}

        try:
            success = self.catalog.create_trigger(
                ast.trigger_name,
                ast.timing,
                ast.event,
                ast.table_name,
                ast.statement
            )
            
            if success:
                return {
                    "type": "CREATE_TRIGGER",
                    "success": True,
                    "message": f"触发器 {ast.trigger_name} 创建成功"
                }
            else:
                return {
                    "type": "CREATE_TRIGGER",
                    "success": False,
                    "message": f"触发器 {ast.trigger_name} 已存在"
                }
                
        except Exception as e:
            return {
                "type": "CREATE_TRIGGER",
                "success": False,
                "message": f"创建触发器失败: {str(e)}"
            }

    def _execute_drop_trigger(self, ast: DropTriggerStatement) -> Dict[str, Any]:
        """执行DROP TRIGGER语句"""
        try:
            success = self.catalog.drop_trigger(ast.trigger_name, ast.if_exists)
            
            if success:
                return {
                    "type": "DROP_TRIGGER",
                    "success": True,
                    "message": f"触发器 {ast.trigger_name} 删除成功"
                }
            else:
                return {
                    "type": "DROP_TRIGGER",
                    "success": False,
                    "message": f"触发器 {ast.trigger_name} 不存在"
                }
                
        except Exception as e:
            return {
                "type": "DROP_TRIGGER",
                "success": False,
                "message": f"删除触发器失败: {str(e)}"
            }

    def _execute_triggers(self, table_name: str, event: str, timing: str) -> List[Dict[str, Any]]:
        """执行触发器"""
        triggers = self.catalog.get_triggers_for_event(table_name, event, timing)
        results = []
        
        for trigger in triggers:
            try:
                # 解析并执行触发器体
                from sql.lexer import SQLLexer
                from sql.parser import SQLParser
                
                # 添加分号确保语句完整
                trigger_sql = trigger['statement']
                if not trigger_sql.endswith(';'):
                    trigger_sql += ';'
                
                lexer = SQLLexer(trigger_sql)
                tokens = lexer.tokenize()
                parser = SQLParser(tokens)
                trigger_ast = parser.parse()
                
                # 递归执行触发器体
                result = self.execute(trigger_ast)
                results.append({
                    "trigger_name": trigger['name'],
                    "success": result.get("success", False),
                    "message": result.get("message", "")
                })
                
                # 如果触发器执行失败，可以选择是否中断整个操作
                if not result.get("success", False):
                    print(f"警告: 触发器 {trigger['name']} 执行失败: {result.get('message', '')}")
                    
            except Exception as e:
                results.append({
                    "trigger_name": trigger['name'],
                    "success": False,
                    "message": f"触发器执行异常: {str(e)}"
                })
                print(f"警告: 触发器 {trigger['name']} 执行异常: {str(e)}")
        
        return results

    def _execute_alter_table(self, stmt):
        """执行ALTER TABLE ADD/DROP COLUMN"""
        if stmt.action == 'ADD':
            # 需要将dict转为ColumnDefinition
            from catalog import ColumnDefinition, DataType
            col_def = stmt.column_def
            data_type_map = {
                "INTEGER": DataType.INTEGER,
                "VARCHAR": DataType.VARCHAR,
                "FLOAT": DataType.FLOAT,
                "BOOLEAN": DataType.BOOLEAN,
                "CHAR": DataType.CHAR,
                "DECIMAL": DataType.DECIMAL,
                "DATE": DataType.DATE,
                "TIME": DataType.TIME,
                "DATETIME": DataType.DATETIME,
                "BIGINT": DataType.BIGINT,
                "TINYINT": DataType.TINYINT,
                "TEXT": DataType.TEXT,
            }
            data_type = data_type_map.get(col_def["type"])
            column = ColumnDefinition(
                name=col_def["name"],
                data_type=data_type,
                max_length=col_def["length"],
                nullable=True if "NULL" in col_def.get("constraints", []) else False,
                primary_key="PRIMARY KEY" in col_def.get("constraints", []),
                unique="UNIQUE" in col_def.get("constraints", []),
                default=col_def.get("default"),
                check=col_def.get("check"),
                foreign_key=col_def.get("foreign_key"),
            )
            self.catalog.add_column(stmt.table_name, column)
            return {"type": "ALTER_TABLE", "success": True, "message": f"表 {stmt.table_name} 添加列 {column.name} 成功"}
        elif stmt.action == 'DROP':
            self.catalog.drop_column(stmt.table_name, stmt.column_name)
            return {"type": "ALTER_TABLE", "success": True, "message": f"表 {stmt.table_name} 删除列 {stmt.column_name} 成功"}
        else:
            return {"type": "ALTER_TABLE", "success": False, "message": f"不支持的ALTER操作: {stmt.action}"}
