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
    """MySQL风格的极简事务管理（无回滚）：autocommit 与显式事务。"""

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
        # 暂不支持
        raise NotImplementedError("当前不支持ROLLBACK")

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
    """SQL执行器，将AST转换为数据库操作"""

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
        if isinstance(ast, CreateTableStatement):
            result = self._execute_create_table(ast)
        elif isinstance(ast, InsertStatement):
            result = self._execute_insert_immediate_undo(ast)
        elif isinstance(ast, SelectStatement):
            result = self._execute_select(ast)
        elif isinstance(ast, CreateIndexStatement):
            result = self._execute_create_index(ast)
        elif isinstance(ast, DropIndexStatement):
            result = self._execute_drop_index(ast)
        elif isinstance(ast, UpdateStatement):
            result = self._execute_update_with_undo(ast)
        elif isinstance(ast, DeleteStatement):
            result = self._execute_delete_with_undo(ast)
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
            ast, (InsertStatement, UpdateStatement, DeleteStatement, CreateTableStatement, DropIndexStatement, CreateIndexStatement)
        ):
            self._maybe_release_autocommit_locks()

        return result

    # 立即写入 + Undo 的 INSERT
    def _execute_insert_immediate_undo(self, stmt: InsertStatement) -> Dict[str, Any]:
        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise ValueError(f"表 {stmt.table_name} 不存在")

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

            # 实际插入
            loc = self.table_manager.insert_record_with_location(stmt.table_name, record_data)
            if not loc:
                raise ValueError("插入记录失败")
            page_id, ridx = loc

            # 索引维护
            if self.index_manager:
                self.index_manager.insert_into_indexes(stmt.table_name, record_data, (page_id, ridx))

            # 若在事务中，记录补偿删除的 Undo
            if self.txn.in_txn():
                self._push_undo({
                    "type": "INSERT",
                    "table": stmt.table_name,
                    "page_id": page_id,
                    "index": ridx,
                })

            inserted_count += 1

        return {
            "type": "INSERT",
            "table_name": stmt.table_name,
            "rows_inserted": inserted_count,
            "success": True,
            "message": f"成功插入 {inserted_count} 行到表 {stmt.table_name}",
        }

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
                self.table_manager.update_at(table, page_id, idx, old_data)
            elif typ == "DELETE":
                # 将旧值插回；可能插入到新位置
                old_data = entry["old_data"]
                self.table_manager.insert_record_with_location(table, old_data)
            elif typ == "INSERT":
                # 如果将来改为立即写入插入，这里需要补偿删除
                page_id = entry.get("page_id")
                idx = entry.get("index")
                if page_id is not None and idx is not None:
                    self.table_manager.delete_at(table, page_id, idx)
            else:
                pass

    def _execute_create_table(self, stmt: CreateTableStatement) -> Dict[str, Any]:
        """执行CREATE TABLE"""
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

            for constraint in col_def["constraints"]:
                if constraint == "NOT NULL":
                    nullable = False
                elif constraint == "PRIMARY KEY":
                    primary_key = True
                    nullable = False

            column = ColumnDefinition(
                name=col_def["name"],
                data_type=data_type,
                max_length=col_def["length"],
                nullable=nullable,
                primary_key=primary_key,
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

    def _execute_insert(self, stmt: InsertStatement) -> Dict[str, Any]:
        """执行INSERT - 修复版，正确处理记录ID"""
        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise ValueError(f"表 {stmt.table_name} 不存在")

        inserted_count = 0

        for value_row in stmt.values:
            # 构建记录数据
            record_data = {}

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

            try:
                # 获取插入前的记录数，作为新记录的ID
                all_records = self.table_manager.scan_table(stmt.table_name)
                record_id = len(all_records)  # 新记录的ID

                # 插入记录到表中
                self.table_manager.insert_record(stmt.table_name, record_data)

                # 更新索引，使用正确的record_id
                if self.index_manager:
                    table_indexes = self.index_manager.get_table_indexes(
                        stmt.table_name
                    )
                    for index_name in table_indexes:
                        index_info = self.index_manager.indexes.get(index_name)
                        if index_info and index_info.column_name in record_data:
                            btree = self.index_manager.get_index(index_name)
                            if btree:
                                key = record_data[index_info.column_name]
                                btree.insert(key, record_id)  # 存储正确的记录ID
                                print(
                                    f"DEBUG: 索引更新 {index_name}: {key} -> {record_id}"
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
        """执行SELECT - 扩展支持JOIN和聚合函数"""
        # 递归获取结果集
        def eval_from(from_table):
            if isinstance(from_table, str):
                # 单表
                schema = self.catalog.get_table_schema(from_table)
                if not schema:
                    raise ValueError(f"表 {from_table} 不存在")
                return self.table_manager.scan_table(from_table), from_table
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
            if stmt.where_clause is None or self._evaluate_condition(stmt.where_clause, record.data):
                filtered_records.append(record)

        # 检查是否有聚合函数
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
                            # 只统计非NULL
                            agg_result["COUNT"] = sum(1 for r in filtered_records if self._evaluate_expression(arg, r.data) is not None)
                        else:
                            raise ValueError("COUNT参数不支持")
                    elif func == "SUM":
                        if isinstance(arg, ColumnRef):
                            values = [self._evaluate_expression(arg, r.data) for r in filtered_records]
                            agg_result["SUM"] = sum(v for v in values if v is not None)
                        else:
                            raise ValueError("SUM参数不支持")
                    elif func == "AVG":
                        if isinstance(arg, ColumnRef):
                            values = [self._evaluate_expression(arg, r.data) for r in filtered_records if self._evaluate_expression(arg, r.data) is not None]
                            agg_result["AVG"] = sum(values) / len(values) if values else None
                        else:
                            raise ValueError("AVG参数不支持")
                    elif func == "MIN":
                        if isinstance(arg, ColumnRef):
                            values = [self._evaluate_expression(arg, r.data) for r in filtered_records if self._evaluate_expression(arg, r.data) is not None]
                            agg_result["MIN"] = min(values) if values else None
                        else:
                            raise ValueError("MIN参数不支持")
                    elif func == "MAX":
                        if isinstance(arg, ColumnRef):
                            values = [self._evaluate_expression(arg, r.data) for r in filtered_records if self._evaluate_expression(arg, r.data) is not None]
                            agg_result["MAX"] = max(values) if values else None
                        else:
                            raise ValueError("MAX参数不支持")
                    else:
                        raise ValueError(f"不支持的聚合函数: {func}")
            result_records = [agg_result]
        else:
            # 选择列
            result_records = []
            for record in filtered_records:
                if stmt.columns == ["*"]:
                    result_records.append(dict(record.data))
                else:
                    selected_data = {}
                    for col in stmt.columns:
                        if isinstance(col, ColumnRef):
                            if col.table_name:
                                key = f"{col.table_name}.{col.column_name}"
                                if key in record.data:
                                    selected_data[col.column_name] = record.data[key]
                                else:
                                    raise ValueError(f"列 {key} 不存在")
                            else:
                                matches = [k for k in record.data if k.endswith(f".{col.column_name}") or k == col.column_name]
                                if len(matches) == 1:
                                    key = matches[0]
                                elif len(matches) == 0:
                                    raise ValueError(f"列 {col.column_name} 不存在")
                                else:
                                    raise ValueError(f"列 {col.column_name} 不明确，请加表前缀")
                                if key in record.data:
                                    selected_data[col.column_name] = record.data[key]
                                else:
                                    raise ValueError(f"列 {key} 不存在")
                        elif isinstance(col, AggregateFunction):
                            # 聚合函数在非聚合上下文不支持
                            raise ValueError("聚合函数只能单独出现在SELECT列表中")
                        else:
                            if col in record.data:
                                selected_data[col] = record.data[col]
                            else:
                                raise ValueError(f"列 {col} 不存在")
                    result_records.append(selected_data)
        """执行SELECT - 基于隔离级别的可见性 + 当前事务缓冲数据"""
        schema = self.catalog.get_table_schema(stmt.from_table)
        if not schema:
            raise ValueError(f"表 {stmt.from_table} 不存在")

        # SERIALIZABLE: 申请读锁
        self._maybe_lock_shared(stmt.from_table)

        # 读取基线集合：取决于隔离级别
        base_records: List[Record] = []
        level = self.txn.isolation_level()
        in_txn_ctx = self.txn.in_txn() and not self.txn.autocommit()

        # REPEATABLE READ / SERIALIZABLE: 第一次读时拍快照
        if in_txn_ctx and level in ("REPEATABLE READ", "SERIALIZABLE"):
            snap = self.txn.get_rr_snapshot_for_table(stmt.from_table)
            if snap is None:
                committed_rows = self.table_manager.scan_table(stmt.from_table)
                snapshot_rows = [dict(r.data) for r in committed_rows]
                self.txn.set_rr_snapshot_for_table(stmt.from_table, snapshot_rows)
                snap = snapshot_rows
            # 基线用快照
            base_dict_rows = snap
            base_records = [Record(dict(row)) for row in base_dict_rows]
        else:
            # READ COMMITTED / READ UNCOMMITTED: 每次读最新已提交
            committed_rows = self.table_manager.scan_table(stmt.from_table)
            base_records = committed_rows

        # READ UNCOMMITTED: 本实现仅在单进程多会话下有意义；暂与 READ COMMITTED 一致

        # WHERE 过滤
        filtered_records: List[Record] = []
        for record in base_records:
            if stmt.where_clause is None or self._evaluate_condition(
                stmt.where_clause, record.data
            ):
                filtered_records.append(record)

        # 当前事务缓冲数据（仅当前会话可见）
        extra_rows: List[Record] = []
        if in_txn_ctx:
            txn_id = self.txn.current_txn_id()
            for item in self._pending_inserts.get(txn_id, []):
                if item["table"] == stmt.from_table:
                    row = item["data"]
                    if stmt.where_clause is None or self._evaluate_condition(
                        stmt.where_clause, row
                    ):
                        extra_rows.append(Record(dict(row)))

        # 选择列
        result_records = []
        for record in filtered_records + extra_rows:
            if stmt.columns == ["*"]:
                result_records.append(dict(record.data))
            else:
                selected_data = {}
                for col in stmt.columns:
                    if isinstance(col, ColumnRef):
                        col_name = col.column_name
                        if col_name in record.data:
                            selected_data[col_name] = record.data[col_name]
                        else:
                            raise ValueError(f"列 {col_name} 不存在")
                    else:
                        if col in record.data:
                            selected_data[col] = record.data[col]
                        else:
                            raise ValueError(f"列 {col} 不存在")
                result_records.append(selected_data)

        return {
            "type": "SELECT",
            "table_name": from_name,
            "rows_returned": len(result_records),
            "data": result_records,
            "success": True,
            "message": f"查询返回 {len(result_records)} 行",
        }

    def _execute_create_index(self, stmt: CreateIndexStatement) -> Dict[str, Any]:
        """执行CREATE INDEX"""
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
        """执行DROP INDEX"""
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

    def _build_index_for_existing_data(
        self, index_name: str, table_name: str, column_name: str
    ):
        """为现有数据构建索引 - 修复版"""
        btree = self.index_manager.get_index(index_name)
        if not btree:
            return

        all_records = self.table_manager.scan_table(table_name)

        for record_id, record in enumerate(all_records):
            if column_name in record.data:
                key = record.data[column_name]
                btree.insert(key, record_id)  # 存储正确的记录ID
                print(f"DEBUG: 构建索引 {index_name}: {key} -> {record_id}")

    def _try_index_scan(
        self, table_name: str, where_clause: Expression
    ) -> Optional[List[Record]]:
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

        print(f"DEBUG: 使用索引 {index_name} 查找 {column_name} {operator} {value}")

        if operator == "=":
            # 精确查找
            rid_or_list = btree.search(value)
            if rid_or_list is not None:
                if isinstance(rid_or_list, list):
                    rids = rid_or_list
                else:
                    rids = [rid_or_list]
                return self._get_records_by_ids(table_name, rids)
        elif operator in ["<", "<=", ">", ">="]:
            # 范围查询
            if operator in ["<", "<="]:
                results = btree.range_search(float("-inf"), value)
            else:
                results = btree.range_search(value, float("inf"))

            rids = [rid for _, rid in results]
            return self._get_records_by_ids(table_name, rids)

        return None

    def _analyze_where_for_index(
        self, table_name: str, where_clause: Expression
    ) -> Optional[tuple]:
        """分析WHERE条件，看是否可以使用索引"""
        if not isinstance(where_clause, BinaryOp):
            return None

        if isinstance(where_clause.left, ColumnRef) and isinstance(
            where_clause.right, Literal
        ):
            column_name = where_clause.left.column_name
            operator = where_clause.operator
            value = where_clause.right.value

            available_indexes = self.index_manager.get_table_indexes(table_name)
            for index_name in available_indexes:
                index_info = self.index_manager.indexes.get(index_name)
                if index_info and index_info.column_name == column_name:
                    return (index_name, column_name, operator, value)

        return None

    def _get_records_by_ids(
        self, table_name: str, rids: List[tuple]
    ) -> List[Record]:
        """根据RID获取记录（当前用scan匹配page_id/index，教学简化）"""
        rid_set = set(rids)
        results: List[Record] = []
        for page_id, idx, rec in self.table_manager.scan_table_with_locations(table_name):
            if (page_id, idx) in rid_set:
                results.append(rec)
        return results

    def _evaluate_expression(self, expr: Expression, context: Dict[str, Any]) -> Any:
        if isinstance(expr, Literal):
            return expr.value
        elif isinstance(expr, ColumnRef):
            if expr.table_name:
                # 优先查找带前缀
                key = f"{expr.table_name}.{expr.column_name}"
                if key in context:
                    return context[key]
            # 回退查找无前缀
            return context.get(expr.column_name)
        else:
            raise ValueError(f"不支持的表达式类型: {type(expr)}")

    def _evaluate_condition(
        self, condition: Expression, record_data: Dict[str, Any]
    ) -> bool:
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

    def _execute_update_with_undo(self, stmt: UpdateStatement) -> Dict[str, Any]:
        # SERIALIZABLE: 写锁
        self._maybe_lock_exclusive(stmt.table_name)

        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise ValueError(f"表 {stmt.table_name} 不存在")

        # 预计算 updates 值
        updates: Dict[str, Any] = {}
        for set_clause in stmt.set_clauses:
            column_name = set_clause["column"]
            value_expr = set_clause["value"]
            if isinstance(value_expr, Literal):
                updates[column_name] = value_expr.value
            else:
                raise ValueError("UPDATE暂时只支持字面量值")

        # 遍历并按位置更新 + 写入undo
        updated = 0
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
        return {"type": "UPDATE", "table_name": stmt.table_name, "rows_updated": updated, "success": True, "message": f"成功更新 {updated} 行"}

    def _execute_delete_with_undo(self, stmt: DeleteStatement) -> Dict[str, Any]:
        # SERIALIZABLE: 写锁
        self._maybe_lock_exclusive(stmt.table_name)

        schema = self.catalog.get_table_schema(stmt.table_name)
        if not schema:
            raise ValueError(f"表 {stmt.table_name} 不存在")

        deleted = 0
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
        return {"type": "DELETE", "table_name": stmt.table_name, "rows_deleted": deleted, "success": True, "message": f"成功删除 {deleted} 行"}
