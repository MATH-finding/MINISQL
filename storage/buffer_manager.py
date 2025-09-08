"""
缓存管理器 - 实现LRU页面缓存策略
"""

from typing import Dict, Optional
from .page_manager import Page, PageManager


class BufferManager:
    """缓存管理器，实现LRU替换策略"""

    def __init__(self, page_manager: PageManager, cache_size: int = 100):
        self.page_manager = page_manager
        self.cache_size = cache_size
        self.cache: Dict[int, Page] = {}
        self.access_order = []  # LRU访问顺序

        # 统计信息
        self.cache_hits = 0
        self.cache_misses = 0

    def get_page(self, page_id: int) -> Page:
        """获取页面（优先从缓存）"""
        if page_id in self.cache:
            # 缓存命中
            self.cache_hits += 1
            self._update_access_order(page_id)
            page = self.cache[page_id]
        else:
            # 缓存未命中
            self.cache_misses += 1
            page = self.page_manager.read_page(page_id)
            self._add_to_cache(page)

        page.pin_count += 1
        return page

    def unpin_page(self, page_id: int, is_dirty: bool):
        """取消固定页面"""
        if page_id in self.cache:
            page = self.cache[page_id]
            page.pin_count = max(0, page.pin_count - 1)
            if is_dirty:
                page.is_dirty = True

    def flush_page(self, page_id: int):
        """刷新特定页面到磁盘"""
        if page_id in self.cache:
            page = self.cache[page_id]
            if page.is_dirty:
                self.page_manager.write_page(page)

    def flush_all(self):
        """刷新所有脏页到磁盘"""
        for page in self.cache.values():
            if page.is_dirty:
                self.page_manager.write_page(page)

    def _add_to_cache(self, page: Page):
        """添加页面到缓存"""
        # 如果缓存已满，执行LRU替换
        if len(self.cache) >= self.cache_size:
            self._evict_lru_page()

        self.cache[page.page_id] = page
        self.access_order.append(page.page_id)

    def _update_access_order(self, page_id: int):
        """更新页面访问顺序"""
        self.access_order.remove(page_id)
        self.access_order.append(page_id)

    def _evict_lru_page(self):
        """淘汰最近最少使用的页面"""
        # 找到最久未访问且未被固定的页面
        for page_id in self.access_order:
            page = self.cache[page_id]
            if page.pin_count == 0:
                # 如果是脏页，先写回磁盘
                if page.is_dirty:
                    self.page_manager.write_page(page)

                # 从缓存中删除
                del self.cache[page_id]
                self.access_order.remove(page_id)
                return

        # 如果所有页面都被固定，抛出异常
        raise RuntimeError("无法淘汰页面：所有页面都被固定")

    def get_cache_stats(self) -> Dict[str, any]:
        """获取缓存统计信息"""
        total_requests = self.cache_hits + self.cache_misses
        hit_rate = self.cache_hits / total_requests if total_requests > 0 else 0

        return {
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "total_requests": total_requests,
            "hit_rate": hit_rate,
            "cached_pages": len(self.cache),
            "cache_size": self.cache_size,
        }
