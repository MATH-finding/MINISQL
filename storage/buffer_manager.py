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

        # 日志管理器 - 通过database.py的set_log_manager方法设置
        self.log_manager = None

    def set_log_manager(self, log_manager):
        """设置日志管理器"""
        self.log_manager = log_manager
        if self.log_manager:
            self.log_manager.logger.info(
                f"缓存管理器初始化，缓存大小: {self.cache_size}", "BUFFER_MANAGER"
            )

    def get_page(self, page_id: int) -> Page:
        """获取页面（优先从缓存）"""
        if page_id in self.cache:
            # 缓存命中
            self.cache_hits += 1
            self._update_access_order(page_id)
            page = self.cache[page_id]

            # 记录缓存命中日志（可选，避免过于频繁）
            if self.log_manager and self.cache_hits % 50 == 0:  # 每50次命中记录一次
                self.log_manager.logger.debug(
                    f"页面缓存命中: {page_id}", "BUFFER_MANAGER"
                )
        else:
            # 缓存未命中
            self.cache_misses += 1

            try:
                page = self.page_manager.read_page(page_id)
                self._add_to_cache(page)

                # 记录缓存未命中和页面加载
                if self.log_manager:
                    self.log_manager.log_page_operation("加载", page_id, True)

            except Exception as e:
                if self.log_manager:
                    self.log_manager.log_page_operation("加载", page_id, False)
                    self.log_manager.log_error(
                        "BUFFER_MANAGER", f"页面{page_id}加载失败", str(e)
                    )
                raise

        page.pin_count += 1
        return page

    def unpin_page(self, page_id: int, is_dirty: bool):
        """取消固定页面"""
        if page_id in self.cache:
            page = self.cache[page_id]
            page.pin_count = max(0, page.pin_count - 1)
            if is_dirty:
                page.is_dirty = True

                # 记录页面变脏
                if self.log_manager:
                    self.log_manager.logger.debug(
                        f"页面标记为脏页: {page_id}", "BUFFER_MANAGER"
                    )

    def flush_page(self, page_id: int):
        """刷新特定页面到磁盘"""
        if page_id in self.cache:
            page = self.cache[page_id]
            if page.is_dirty:
                try:
                    self.page_manager.write_page(page)
                    page.is_dirty = False

                    if self.log_manager:
                        self.log_manager.log_page_operation("刷新", page_id, True)

                except Exception as e:
                    if self.log_manager:
                        self.log_manager.log_page_operation("刷新", page_id, False)
                        self.log_manager.log_error(
                            "BUFFER_MANAGER", f"页面{page_id}刷新失败", str(e)
                        )
                    raise

    def flush_all(self) -> int:
        """刷新所有脏页到磁盘"""
        flushed_count = 0
        failed_count = 0

        for page_id, page in self.cache.items():
            if page.is_dirty:
                try:
                    self.page_manager.write_page(page)
                    page.is_dirty = False
                    flushed_count += 1

                except Exception as e:
                    failed_count += 1
                    if self.log_manager:
                        self.log_manager.log_error(
                            "BUFFER_MANAGER", f"页面{page_id}刷新失败", str(e)
                        )

        # 记录刷新结果
        if self.log_manager:
            if flushed_count > 0:
                self.log_manager.log_buffer_flush(flushed_count)
            if failed_count > 0:
                self.log_manager.log_error(
                    "BUFFER_MANAGER", f"刷新失败的页面数: {failed_count}"
                )

        return flushed_count

    def _add_to_cache(self, page: Page):
        """添加页面到缓存"""
        # 如果缓存已满，执行LRU替换
        if len(self.cache) >= self.cache_size:
            evicted_page_id = self._evict_lru_page()
            if self.log_manager and evicted_page_id:
                self.log_manager.logger.debug(
                    f"LRU淘汰页面: {evicted_page_id}", "BUFFER_MANAGER"
                )

        self.cache[page.page_id] = page
        self.access_order.append(page.page_id)

    def _update_access_order(self, page_id: int):
        """更新页面访问顺序"""
        self.access_order.remove(page_id)
        self.access_order.append(page_id)

    def _evict_lru_page(self) -> Optional[int]:
        """淘汰最近最少使用的页面"""
        # 找到最久未访问且未被固定的页面
        for page_id in self.access_order:
            page = self.cache[page_id]
            if page.pin_count == 0:
                try:
                    # 如果是脏页，先写回磁盘
                    if page.is_dirty:
                        self.page_manager.write_page(page)

                    # 从缓存中删除
                    del self.cache[page_id]
                    self.access_order.remove(page_id)
                    return page_id

                except Exception as e:
                    if self.log_manager:
                        self.log_manager.log_error(
                            "BUFFER_MANAGER", f"淘汰页面{page_id}时写回失败", str(e)
                        )
                    # 即使写回失败也要淘汰页面，避免死锁
                    del self.cache[page_id]
                    self.access_order.remove(page_id)
                    return page_id

        # 如果所有页面都被固定，记录错误并抛出异常
        error_msg = "无法淘汰页面：所有页面都被固定"
        if self.log_manager:
            self.log_manager.log_error(
                "BUFFER_MANAGER", error_msg, f"缓存中有{len(self.cache)}个页面"
            )
        raise RuntimeError(error_msg)

    def get_cache_stats(self) -> Dict[str, any]:
        """获取缓存统计信息"""
        total_requests = self.cache_hits + self.cache_misses
        hit_rate = self.cache_hits / total_requests if total_requests > 0 else 0

        stats = {
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "total_requests": total_requests,
            "hit_rate": hit_rate,
            "cached_pages": len(self.cache),
            "cache_size": self.cache_size,
        }

        # 定期记录缓存统计到日志（每100次请求记录一次）
        if self.log_manager and total_requests > 0 and total_requests % 100 == 0:
            self.log_manager.log_cache_stats(
                self.cache_hits, self.cache_misses, hit_rate, len(self.cache)
            )

        return stats

    def get_detailed_stats(self) -> Dict[str, any]:
        """获取详细的缓存统计信息（用于调试）"""
        pinned_pages = sum(1 for page in self.cache.values() if page.pin_count > 0)
        dirty_pages = sum(1 for page in self.cache.values() if page.is_dirty)

        detailed_stats = {
            **self.get_cache_stats(),
            "pinned_pages": pinned_pages,
            "dirty_pages": dirty_pages,
            "free_slots": self.cache_size - len(self.cache),
        }

        return detailed_stats

    def force_evict_all_unpinned(self) -> int:
        """强制淘汰所有未固定的页面（紧急情况使用）"""
        evicted_count = 0
        to_evict = []

        # 收集可淘汰的页面
        for page_id, page in self.cache.items():
            if page.pin_count == 0:
                to_evict.append(page_id)

        # 淘汰页面
        for page_id in to_evict:
            page = self.cache[page_id]
            try:
                if page.is_dirty:
                    self.page_manager.write_page(page)
                del self.cache[page_id]
                if page_id in self.access_order:
                    self.access_order.remove(page_id)
                evicted_count += 1
            except Exception as e:
                if self.log_manager:
                    self.log_manager.log_error(
                        "BUFFER_MANAGER", f"强制淘汰页面{page_id}失败", str(e)
                    )

        if self.log_manager and evicted_count > 0:
            self.log_manager.logger.info(
                f"强制淘汰了{evicted_count}个页面", "BUFFER_MANAGER"
            )

        return evicted_count
