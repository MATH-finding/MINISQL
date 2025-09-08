"""
记录管理器 - 修复版本，使用固定偏移表区域
"""

from typing import List, Dict, Any, Optional
import struct
import pickle
from .buffer_manager import BufferManager


class Record:
    """记录类"""

    def __init__(self, data: Dict[str, Any]):
        self.data = data

    def get(self, key: str, default=None):
        return self.data.get(key, default)

    def __getitem__(self, key: str):
        return self.data[key]

    def __setitem__(self, key: str, value: Any):
        self.data[key] = value

    def keys(self):
        return self.data.keys()

    def items(self):
        return self.data.items()

    def __repr__(self):
        return f"Record({self.data})"


class RecordManager:
    """记录管理器，管理页面内的记录存储"""

    # 固定偏移表大小，支持最多200条记录
    MAX_RECORDS_PER_PAGE = 200
    OFFSET_TABLE_START = 8
    OFFSET_TABLE_SIZE = MAX_RECORDS_PER_PAGE * 4
    DATA_START = OFFSET_TABLE_START + OFFSET_TABLE_SIZE  # 808

    def __init__(self, buffer_manager: BufferManager):
        self.buffer_manager = buffer_manager

    def insert_record(self, page_id: int, record: Record) -> bool:
        """在页面中插入记录"""
        page = self.buffer_manager.get_page(page_id)

        try:
            # 检查并初始化页面（如果需要）
            self._ensure_page_initialized(page)

            # 序列化记录
            record_data = pickle.dumps(record.data, protocol=pickle.HIGHEST_PROTOCOL)
            record_size = len(record_data)

            # 读取页面头部信息
            record_count = page.read_int(0)
            free_space_offset = page.read_int(4)

            # 检查记录数是否超出限制
            if record_count >= self.MAX_RECORDS_PER_PAGE:
                return False

            # 检查空间
            if free_space_offset + 4 + record_size > page.PAGE_SIZE:
                return False

            # 写入记录：长度 + 数据
            page.write_int(free_space_offset, record_size)
            page.write_bytes(free_space_offset + 4, record_data)

            # 更新偏移表：在固定区域内
            offset_pos = self.OFFSET_TABLE_START + record_count * 4
            page.write_int(offset_pos, free_space_offset)

            # 更新页面头部
            page.write_int(0, record_count + 1)
            page.write_int(4, free_space_offset + 4 + record_size)

            return True

        except Exception as e:
            print(f"插入记录时出错: {e}")
            return False
        finally:
            self.buffer_manager.unpin_page(page_id, True)

    def get_records(self, page_id: int) -> List[Record]:
        """获取页面中的所有记录"""
        page = self.buffer_manager.get_page(page_id)
        records = []

        try:
            # 检查并初始化页面（如果需要）
            self._ensure_page_initialized(page)

            record_count = page.read_int(0)

            if record_count == 0:
                return records

            # 读取每个记录
            for i in range(record_count):
                try:
                    offset_pos = self.OFFSET_TABLE_START + i * 4
                    record_offset = page.read_int(offset_pos)

                    # 跳过已删除的记录
                    if record_offset == -1:
                        continue

                    # 读取记录长度
                    record_size = page.read_int(record_offset)

                    if record_size <= 0 or record_size > page.PAGE_SIZE:
                        print(f"警告: 记录 {i} 的长度异常: {record_size}")
                        continue

                    # 验证偏移量的合理性
                    if (
                        record_offset < self.DATA_START
                        or record_offset + 4 + record_size > page.PAGE_SIZE
                    ):
                        print(
                            f"警告: 记录 {i} 的偏移量异常: offset={record_offset}, size={record_size}"
                        )
                        continue

                    # 读取记录数据
                    record_data = page.read_bytes(record_offset + 4, record_size)

                    if not record_data or len(record_data) != record_size:
                        print(
                            f"警告: 记录 {i} 数据读取异常，期望{record_size}字节，实际{len(record_data) if record_data else 0}字节"
                        )
                        continue

                    # 反序列化
                    data = pickle.loads(record_data)
                    records.append(Record(data))

                except (
                    pickle.UnpicklingError,
                    pickle.PickleError,
                    EOFError,
                    ValueError,
                ) as e:
                    print(f"反序列化记录 {i} 时出错: {e}")
                    continue
                except Exception as e:
                    print(f"处理记录 {i} 时出现未知错误: {e}")
                    continue

        except Exception as e:
            print(f"读取页面记录时出错: {e}")
        finally:
            self.buffer_manager.unpin_page(page_id, False)

        return records

    def delete_record(self, page_id: int, record_index: int) -> bool:
        """删除页面中的指定记录"""
        page = self.buffer_manager.get_page(page_id)

        try:
            record_count = page.read_int(0)

            if record_index >= record_count or record_index < 0:
                return False

            # 标记删除
            offset_pos = self.OFFSET_TABLE_START + record_index * 4
            page.write_int(offset_pos, -1)

            return True

        except Exception as e:
            print(f"删除记录时出错: {e}")
            return False
        finally:
            self.buffer_manager.unpin_page(page_id, True)

    def initialize_page(self, page_id: int):
        """初始化页面为空页面"""
        page = self.buffer_manager.get_page(page_id)
        try:
            page.write_int(0, 0)  # 记录数量 = 0
            page.write_int(4, self.DATA_START)  # 自由空间从数据区开始
        finally:
            self.buffer_manager.unpin_page(page_id, True)

    def _ensure_page_initialized(self, page):
        """确保页面已正确初始化"""
        try:
            record_count = page.read_int(0)
            free_space_offset = page.read_int(4)

            need_init = False

            if record_count < 0 or record_count > self.MAX_RECORDS_PER_PAGE:
                need_init = True
            elif (
                free_space_offset < self.DATA_START
                or free_space_offset > page.PAGE_SIZE
            ):
                need_init = True

            if need_init:
                print(f"检测到页面未初始化，正在初始化...")
                page.write_int(0, 0)
                page.write_int(4, self.DATA_START)

        except Exception as e:
            print(f"页面初始化检查失败，强制初始化: {e}")
            page.write_int(0, 0)
            page.write_int(4, self.DATA_START)
