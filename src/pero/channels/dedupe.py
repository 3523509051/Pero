"""有界短期去重：容量与有效期双重限制，按账号范围隔离。

只保存事件键与时间戳，不保存消息正文；进程重启后缓存清空，
因此 `dedupe_scope` 要如实标为 `bridge_process` 之类，不能声称跨重启去重。
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from typing import Tuple


class BoundedDedupeCache:
    """LRU + TTL 的去重集合。线程安全，供单进程使用。"""

    def __init__(self, *, max_entries: int, ttl_seconds: float) -> None:
        if max_entries <= 0:
            raise ValueError("max_entries 必须是正数")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds 必须是正数")
        self._max_entries = int(max_entries)
        self._ttl = float(ttl_seconds)
        self._items: "OrderedDict[Tuple[str, str], float]" = OrderedDict()
        self._lock = threading.RLock()
        self._expired = 0
        self._evicted = 0

    def observe(self, account_id: str, event_id: str) -> bool:
        """登记事件键。

        返回 ``True`` 表示首次见到（可以处理），``False`` 表示在有效期内已见过（重复）。
        """

        key = (str(account_id), str(event_id))
        now = time.monotonic()
        with self._lock:
            self._prune(now)
            seen_at = self._items.get(key)
            if seen_at is not None:
                self._items.move_to_end(key)
                return False
            self._items[key] = now
            self._items.move_to_end(key)
            while len(self._items) > self._max_entries:
                self._items.popitem(last=False)
                self._evicted += 1
            return True

    def _prune(self, now: float) -> None:
        while self._items:
            oldest_key = next(iter(self._items))
            if now - self._items[oldest_key] <= self._ttl:
                break
            self._items.popitem(last=False)
            self._expired += 1

    @property
    def size(self) -> int:
        with self._lock:
            self._prune(time.monotonic())
            return len(self._items)

    def stats(self) -> dict:
        with self._lock:
            return {
                "size": self.size,
                "max_entries": self._max_entries,
                "ttl_seconds": self._ttl,
                "expired": self._expired,
                "evicted": self._evicted,
            }
