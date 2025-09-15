"""
事务状态管理器 - 用于实现正确的隔离级别
"""

from typing import Dict, List, Any, Optional, Set
from threading import Lock
import time

class TransactionState:
    """单个事务的状态"""
    def __init__(self, txn_id: int, session_id: int, isolation_level: str):
        self.txn_id = txn_id
        self.session_id = session_id
        self.isolation_level = isolation_level
        self.start_time = time.time()
        self.modified_tables: Set[str] = set()
        self.pending_changes: Dict[str, List[Dict[str, Any]]] = {}  # table -> [changes]
        self.committed = False

class GlobalTransactionManager:
    """全局事务状态管理器"""
    
    _instance = None
    _lock = Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.active_transactions: Dict[int, TransactionState] = {}
            self.committed_changes: Dict[str, List[Dict[str, Any]]] = {}  # table -> [committed_changes]
            self._lock = Lock()
            self._initialized = True
    
    def register_transaction(self, txn_id: int, session_id: int, isolation_level: str):
        """注册新事务"""
        with self._lock:
            self.active_transactions[txn_id] = TransactionState(txn_id, session_id, isolation_level)
    
    def unregister_transaction(self, txn_id: int):
        """注销事务"""
        with self._lock:
            if txn_id in self.active_transactions:
                del self.active_transactions[txn_id]
    
    def record_table_modification(self, txn_id: int, table_name: str, change_type: str, old_data: Dict[str, Any] = None, new_data: Dict[str, Any] = None):
        """记录表的修改"""
        with self._lock:
            if txn_id in self.active_transactions:
                txn = self.active_transactions[txn_id]
                txn.modified_tables.add(table_name)
                if table_name not in txn.pending_changes:
                    txn.pending_changes[table_name] = []
                txn.pending_changes[table_name].append({
                    'type': change_type,
                    'old_data': old_data,
                    'new_data': new_data,
                    'timestamp': time.time()
                })
    
    def commit_transaction(self, txn_id: int):
        """提交事务"""
        with self._lock:
            if txn_id in self.active_transactions:
                txn = self.active_transactions[txn_id]
                txn.committed = True
                # 将修改应用到已提交状态
                for table_name, changes in txn.pending_changes.items():
                    if table_name not in self.committed_changes:
                        self.committed_changes[table_name] = []
                    # 应用修改到已提交状态
                    for change in changes:
                        if change['type'] == 'INSERT' and change['new_data']:
                            self.committed_changes[table_name].append(change['new_data'])
                        elif change['type'] == 'UPDATE' and change['new_data']:
                            # 更新现有记录
                            for i, record in enumerate(self.committed_changes[table_name]):
                                if self._records_match(record, change['old_data']):
                                    self.committed_changes[table_name][i] = change['new_data'].copy()
                                    break
                        elif change['type'] == 'DELETE' and change['old_data']:
                            # 删除记录
                            self.committed_changes[table_name] = [
                                r for r in self.committed_changes[table_name] 
                                if not self._records_match(r, change['old_data'])
                            ]
                del self.active_transactions[txn_id]
    
    def rollback_transaction(self, txn_id: int):
        """回滚事务"""
        with self._lock:
            if txn_id in self.active_transactions:
                del self.active_transactions[txn_id]
    
    def get_visible_data(self, table_name: str, reader_txn_id: int, reader_isolation_level: str) -> List[Dict[str, Any]]:
        """根据隔离级别获取可见数据"""
        with self._lock:
            # 获取基础数据（已提交的数据）
            base_data = self.committed_changes.get(table_name, [])
            
            if reader_isolation_level == "READ UNCOMMITTED":
                # 读未提交：可以看到所有数据，包括未提交的修改
                visible_data = base_data.copy()
                for txn_id, txn in self.active_transactions.items():
                    if txn_id != reader_txn_id and table_name in txn.pending_changes:
                        # 添加其他事务的未提交修改
                        for change in txn.pending_changes[table_name]:
                            if change['type'] == 'INSERT' and change['new_data']:
                                visible_data.append(change['new_data'])
                            elif change['type'] == 'UPDATE' and change['new_data']:
                                # 更新现有记录
                                for i, record in enumerate(visible_data):
                                    if self._records_match(record, change['old_data']):
                                        visible_data[i] = change['new_data'].copy()
                            elif change['type'] == 'DELETE' and change['old_data']:
                                # 删除记录
                                visible_data = [r for r in visible_data if not self._records_match(r, change['old_data'])]
                return visible_data
            
            elif reader_isolation_level == "READ COMMITTED":
                # 读已提交：只能看到已提交的数据，不能看到其他事务的未提交修改
                return base_data.copy()
            
            elif reader_isolation_level in ("REPEATABLE READ", "SERIALIZABLE"):
                # 可重复读和串行化：使用快照隔离，只能看到已提交的数据
                return base_data.copy()
            
            return base_data.copy()
    
    def _records_match(self, record1: Dict[str, Any], record2: Dict[str, Any]) -> bool:
        """比较两个记录是否匹配（基于主键）"""
        if not record1 or not record2:
            return False
        # 简单的主键比较（假设id是主键）
        return record1.get('id') == record2.get('id')
    
    def get_active_transactions(self) -> List[TransactionState]:
        """获取所有活跃事务"""
        with self._lock:
            return list(self.active_transactions.values())
    
    def clear_all(self):
        """清理所有状态（用于测试）"""
        with self._lock:
            self.active_transactions.clear()
            self.committed_changes.clear()

# 全局实例
global_txn_manager = GlobalTransactionManager()
