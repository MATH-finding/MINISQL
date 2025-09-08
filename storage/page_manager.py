"""
页面管理器 - 负责磁盘页面的分配、读写和管理
"""

import os
import struct


class Page:
    """数据页类，固定4KB大小"""

    PAGE_SIZE = 4096

    def __init__(self, page_id: int, data: bytes = None):
        self.page_id = page_id
        self.data = bytearray(data) if data else bytearray(self.PAGE_SIZE)
        self.is_dirty = False
        self.pin_count = 0

    def read_bytes(self, offset: int, length: int) -> bytes:
        """从指定偏移读取字节"""
        return bytes(self.data[offset : offset + length])

    def write_bytes(self, offset: int, data: bytes):
        """向指定偏移写入字节"""
        self.data[offset : offset + len(data)] = data
        self.is_dirty = True

    def write_int(self, offset: int, value: int):
        """在指定偏移位置写入4字节有符号整数"""
        if offset < 0 or offset + 4 > self.PAGE_SIZE:
            raise ValueError(f"偏移量超出范围: {offset}")
        # 使用有符号整数格式 'i' 而不是无符号 'I'
        self.data[offset : offset + 4] = struct.pack("<i", value)
        self.is_dirty = True

    def read_int(self, offset: int) -> int:
        """从指定偏移位置读取4字节有符号整数"""
        if offset < 0 or offset + 4 > self.PAGE_SIZE:
            raise ValueError(f"偏移量超出范围: {offset}")
        # 使用有符号整数格式 'i'
        return struct.unpack("<i", self.data[offset : offset + 4])[0]


class PageManager:
    """页面管理器，负责页面的分配、读写"""

    def __init__(self, db_file: str):
        self.db_file = db_file
        self.next_page_id = 0
        self._ensure_file_exists()
        self._load_header()

    def _ensure_file_exists(self):
        """确保数据库文件存在"""
        if not os.path.exists(self.db_file):
            with open(self.db_file, "wb") as f:
                header = bytearray(Page.PAGE_SIZE)
                struct.pack_into("<I", header, 0, 1)  # next_page_id = 1
                f.write(header)

    def _load_header(self):
        """加载文件头部信息"""
        with open(self.db_file, "rb") as f:
            header = f.read(Page.PAGE_SIZE)
            self.next_page_id = struct.unpack("<I", header[:4])[0]

    def _save_header(self):
        """保存文件头部信息"""
        with open(self.db_file, "r+b") as f:
            header = bytearray(Page.PAGE_SIZE)
            struct.pack_into("<I", header, 0, self.next_page_id)
            f.write(header)

    def allocate_page(self) -> Page:  # 恢复返回 Page 对象
        """分配新页面"""
        page_id = self.next_page_id
        self.next_page_id += 1
        self._save_header()

        page = Page(page_id)
        # 关键修复：初始化页面头部
        page.write_int(0, 0)  # 记录数量 = 0
        page.write_int(4, 8)  # 自由空间起始偏移量 = 8

        self.write_page(page)
        return page  # 返回初始化好的 Page 对象

    def read_page(self, page_id: int) -> Page:
        """从磁盘读取页面"""
        with open(self.db_file, "rb") as f:
            f.seek(page_id * Page.PAGE_SIZE)
            data = f.read(Page.PAGE_SIZE)
            if len(data) < Page.PAGE_SIZE:
                raise ValueError(f"页面 {page_id} 不存在")
            return Page(page_id, data)

    def write_page(self, page: Page):
        """将页面写入磁盘"""
        with open(self.db_file, "r+b") as f:
            f.seek(page.page_id * Page.PAGE_SIZE)
            f.write(page.data)
            page.is_dirty = False

    def get_file_size(self) -> int:
        """获取文件大小（页数）"""
        return os.path.getsize(self.db_file) // Page.PAGE_SIZE

    def get_page_count(self) -> int:
        """获取页面总数"""
        return self.next_page_id

    def close(self):
        """关闭页面管理器"""
        self._save_header()
