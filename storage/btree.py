# storage/btree.py
"""
B+树索引实现
"""

import struct
from typing import List, Tuple, Optional, Any
from .buffer_manager import BufferManager
from .page_manager import PageManager


class BPlusTreeNode:
    """B+树节点基类"""

    def __init__(self, page_id: int, is_leaf: bool = False):
        self.page_id = page_id
        self.is_leaf = is_leaf
        self.keys = []  # 键列表
        self.parent_id = -1  # 父节点页面ID


class InternalNode(BPlusTreeNode):
    """内部节点"""

    def __init__(self, page_id: int):
        super().__init__(page_id, False)
        self.children_ids = []  # 子节点页面ID列表


class LeafNode(BPlusTreeNode):
    """叶子节点"""

    def __init__(self, page_id: int):
        super().__init__(page_id, True)
        self.values = []  # 值列表（记录ID或记录本身）
        self.next_leaf_id = -1  # 下一个叶子节点ID（支持范围查询）


class BPlusTree:
    """B+树索引"""

    def __init__(
        self,
        buffer_manager: BufferManager,  # 缓冲区管理器，用于管理数据页的缓存
        page_manager: PageManager,  # 页面管理器，负责数据页的读写操作
        order: int = 50,  # B+树的阶数，决定每个节点能容纳的关键字数量
        root_page_id: Optional[int] = None,  # 根节点所在的页面ID，如果为None则创建新树
        is_unique: bool = False,  # 添加这个参数，用于标识索引是否唯一
    ):
        self.buffer_manager = buffer_manager  # 初始化缓冲区管理器
        self.page_manager = page_manager  # 初始化页面管理器
        self.order = order  # 设置B+树的阶数
        self.root_page_id = root_page_id
        self.root = None
        self.is_unique = is_unique  # 添加这个属性

        if root_page_id is None:
            self._create_root()
        else:
            self.root = self._load_node_from_page(root_page_id)

    def is_empty(self):
        """检查B+树是否为空"""
        return self.root is None or (
            hasattr(self.root, "keys") and len(self.root.keys) == 0
        )

    def _create_root(self) -> None:
        """创建根节点"""
        root_page = self.page_manager.allocate_page()
        self.root_page_id = root_page.page_id

        # 初始化为叶子节点
        leaf = LeafNode(self.root_page_id)
        self.root = leaf  # 设置root属性
        self._save_node_to_page(leaf)

    def insert(self, key: Any, value: Any) -> bool:
        """插入键值对，支持唯一性检查"""
        if self.root_page_id is None:
            self._create_root()

        # 如果是唯一索引，先检查键是否已存在
        if self.is_unique:
            existing_value = self.search(key)
            if existing_value is not None:
                raise ValueError(f"唯一性约束违反：键 {key} 已存在")

        # 查找插入位置
        leaf = self._find_leaf(key)

        # 在叶子节点中插入
        if self._insert_into_leaf(leaf, key, value):
            return True
        else:
            return self._handle_leaf_split(leaf, key, value)

    def search(self, key: Any) -> Optional[Any]:
        """搜索指定键的值"""
        if self.root_page_id is None:
            return None

        leaf = self._find_leaf(key)

        # 在叶子节点中查找
        for i, k in enumerate(leaf.keys):
            if k == key:
                return leaf.values[i]

        return None

    def range_search(self, start_key: Any, end_key: Any) -> List[Tuple[Any, Any]]:
        """范围查询"""
        result = []

        # 找到起始叶子节点
        current_leaf = self._find_leaf(start_key)

        while current_leaf:
            # 在当前叶子节点中查找符合条件的键值对
            for i, key in enumerate(current_leaf.keys):
                if start_key <= key <= end_key:
                    result.append((key, current_leaf.values[i]))
                elif key > end_key:
                    return result

            # 移动到下一个叶子节点
            if current_leaf.next_leaf_id != -1:
                current_leaf = self._load_node_from_page(current_leaf.next_leaf_id)
            else:
                break

        return result

    def _find_leaf(self, key: Any) -> LeafNode:
        """查找应该插入指定键的叶子节点"""
        current = self._load_node_from_page(self.root_page_id)

        while not current.is_leaf:
            # 在内部节点中查找
            child_index = self._find_child_index(current, key)
            current = self._load_node_from_page(current.children_ids[child_index])

        return current

    def _find_child_index(self, internal_node: InternalNode, key: Any) -> int:
        """在内部节点中找到子节点索引"""
        for i, node_key in enumerate(internal_node.keys):
            if key < node_key:
                return i
        return len(internal_node.keys)

    def _insert_into_leaf(self, leaf: LeafNode, key: Any, value: Any) -> bool:
        """在叶子节点中插入键值对，返回是否成功（不需要分裂）"""
        # 找到插入位置
        insert_pos = 0
        for i, k in enumerate(leaf.keys):
            if key == k:
                if self.is_unique:
                    # 唯一索引不允许重复键
                    raise ValueError(f"唯一性约束违反：键 {key} 已存在")
                else:
                    # 非唯一索引更新现有键的值
                    leaf.values[i] = value
                    self._save_node_to_page(leaf)
                    return True
            elif key < k:
                insert_pos = i
                break
            else:
                insert_pos = i + 1

        # 插入新键值对
        leaf.keys.insert(insert_pos, key)
        leaf.values.insert(insert_pos, value)

        # 检查是否需要分裂
        if len(leaf.keys) <= self.order - 1:
            self._save_node_to_page(leaf)
            return True
        else:
            return False

    def _handle_leaf_split(self, leaf: LeafNode, key: Any, value: Any) -> bool:
        """处理叶子节点分裂"""
        # 创建新的叶子节点
        new_leaf_page = self.page_manager.allocate_page()
        new_leaf = LeafNode(new_leaf_page.page_id)

        # 找到分割点
        mid_index = len(leaf.keys) // 2

        # 分割键和值
        new_leaf.keys = leaf.keys[mid_index:]
        new_leaf.values = leaf.values[mid_index:]
        new_leaf.next_leaf_id = leaf.next_leaf_id
        new_leaf.parent_id = leaf.parent_id

        leaf.keys = leaf.keys[:mid_index]
        leaf.values = leaf.values[:mid_index]
        leaf.next_leaf_id = new_leaf.page_id

        # 保存节点
        self._save_node_to_page(leaf)
        self._save_node_to_page(new_leaf)

        # 向父节点插入新键
        promote_key = new_leaf.keys[0]
        return self._insert_into_parent(leaf, promote_key, new_leaf)

    def _insert_into_parent(
        self, left_node: BPlusTreeNode, key: Any, right_node: BPlusTreeNode
    ) -> bool:
        """向父节点插入键"""
        if left_node.parent_id == -1:
            # 创建新根节点
            return self._create_new_root(left_node, key, right_node)

        # 获取父节点并插入
        parent = self._load_node_from_page(left_node.parent_id)
        return self._insert_into_internal(parent, key, right_node.page_id)

    def _create_new_root(
        self, left_node: BPlusTreeNode, key: Any, right_node: BPlusTreeNode
    ) -> bool:
        """创建新的根节点"""
        root_page = self.page_manager.allocate_page()
        new_root = InternalNode(root_page.page_id)

        new_root.keys = [key]
        new_root.children_ids = [left_node.page_id, right_node.page_id]

        # 更新子节点的父指针
        left_node.parent_id = new_root.page_id
        right_node.parent_id = new_root.page_id

        self._save_node_to_page(new_root)
        self._save_node_to_page(left_node)
        self._save_node_to_page(right_node)

        # 关键：更新根节点ID和root属性
        old_root_id = self.root_page_id
        self.root_page_id = new_root.page_id
        self.root = new_root

        return True

    def _insert_into_internal(
        self, internal_node: InternalNode, key: Any, child_page_id: int
    ) -> bool:
        """向内部节点插入键和子节点ID"""
        # 找到插入位置
        insert_pos = 0
        for i, node_key in enumerate(internal_node.keys):
            if key < node_key:
                insert_pos = i
                break
            else:
                insert_pos = i + 1

        # 插入键和子节点ID
        internal_node.keys.insert(insert_pos, key)
        internal_node.children_ids.insert(insert_pos + 1, child_page_id)

        # 检查是否需要分裂
        if len(internal_node.keys) <= self.order - 1:
            self._save_node_to_page(internal_node)
            return True
        else:
            # 需要分裂内部节点
            return self._handle_internal_split(internal_node)

    def _handle_internal_split(self, internal_node: InternalNode) -> bool:
        """处理内部节点分裂"""
        # 创建新的内部节点
        new_internal_page = self.page_manager.allocate_page()
        new_internal = InternalNode(new_internal_page.page_id)
        new_internal.parent_id = internal_node.parent_id

        # 找到分割点
        mid_index = len(internal_node.keys) // 2
        promote_key = internal_node.keys[mid_index]

        # 分割键和子节点
        new_internal.keys = internal_node.keys[mid_index + 1 :]
        new_internal.children_ids = internal_node.children_ids[mid_index + 1 :]

        internal_node.keys = internal_node.keys[:mid_index]
        internal_node.children_ids = internal_node.children_ids[: mid_index + 1]

        # 更新所有受影响的子节点的父指针
        for child_id in internal_node.children_ids:
            child_node = self._load_node_from_page(child_id)
            child_node.parent_id = internal_node.page_id
            self._save_node_to_page(child_node)

        for child_id in new_internal.children_ids:
            child_node = self._load_node_from_page(child_id)
            child_node.parent_id = new_internal.page_id
            self._save_node_to_page(child_node)

        # 保存节点
        self._save_node_to_page(internal_node)
        self._save_node_to_page(new_internal)

        # 向父节点插入提升的键
        return self._insert_into_parent(internal_node, promote_key, new_internal)

    def _load_node_from_page(self, page_id: int) -> BPlusTreeNode:
        """从页面加载节点"""
        page = self.buffer_manager.get_page(page_id)
        try:
            # 读取节点头部信息
            is_leaf = page.read_int(0) == 1
            parent_id = page.read_int(4)
            if parent_id == 0:
                parent_id = -1  # 恢复为-1表示无父节点
            key_count = page.read_int(8)

            if is_leaf:
                node = LeafNode(page_id)
                node.next_leaf_id = page.read_int(12)

                # 读取键和值
                offset = 16
                for i in range(key_count):
                    key_size = page.read_int(offset)
                    offset += 4
                    key_data = page.read_bytes(offset, key_size)
                    offset += key_size
                    key = self._deserialize_key(key_data)
                    node.keys.append(key)

                    value_size = page.read_int(offset)
                    offset += 4
                    value_data = page.read_bytes(offset, value_size)
                    offset += value_size
                    value = self._deserialize_value(value_data)
                    node.values.append(value)
            else:
                node = InternalNode(page_id)

                # 读取键和子节点ID
                offset = 12
                for i in range(key_count):
                    key_size = page.read_int(offset)
                    offset += 4
                    key_data = page.read_bytes(offset, key_size)
                    offset += key_size
                    key = self._deserialize_key(key_data)
                    node.keys.append(key)

                # 读取子节点ID（比键多一个）
                for i in range(key_count + 1):
                    child_id = page.read_int(offset)
                    offset += 4
                    node.children_ids.append(child_id)

            node.parent_id = parent_id
            return node

        finally:
            self.buffer_manager.unpin_page(page_id, False)

    def _save_node_to_page(self, node: BPlusTreeNode) -> None:
        """保存节点到页面"""
        page = self.buffer_manager.get_page(node.page_id)
        try:
            # 写入节点头部
            page.write_int(0, 1 if node.is_leaf else 0)
            # 处理parent_id
            if node.parent_id == -1:
                page.write_int(4, 0)  # 用0表示无父节点
            else:
                page.write_int(4, node.parent_id)
            page.write_int(8, len(node.keys))

            offset = 12
            if node.is_leaf:
                page.write_int(offset, node.next_leaf_id)
                offset += 4

                # 写入键和值
                for i, key in enumerate(node.keys):
                    key_data = self._serialize_key(key)
                    page.write_int(offset, len(key_data))
                    offset += 4
                    page.write_bytes(offset, key_data)
                    offset += len(key_data)

                    value_data = self._serialize_value(node.values[i])
                    page.write_int(offset, len(value_data))
                    offset += 4
                    page.write_bytes(offset, value_data)
                    offset += len(value_data)
            else:
                # 写入键
                for key in node.keys:
                    key_data = self._serialize_key(key)
                    page.write_int(offset, len(key_data))
                    offset += 4
                    page.write_bytes(offset, key_data)
                    offset += len(key_data)

                # 写入子节点ID
                for child_id in node.children_ids:
                    page.write_int(offset, child_id)
                    offset += 4

        finally:
            self.buffer_manager.unpin_page(node.page_id, True)

    def _serialize_key(self, key: Any) -> bytes:
        """序列化键"""
        import pickle

        return pickle.dumps(key)

    def _deserialize_key(self, data: bytes) -> Any:
        """反序列化键"""
        import pickle

        return pickle.loads(data)

    def _serialize_value(self, value: Any) -> bytes:
        """序列化值"""
        import pickle

        return pickle.dumps(value)

    def _deserialize_value(self, data: bytes) -> Any:
        """反序列化值"""
        import pickle

        return pickle.loads(data)
