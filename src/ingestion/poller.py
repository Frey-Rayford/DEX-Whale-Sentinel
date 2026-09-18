import asyncio
from collections import deque
from typing import Set, Deque, Optional
from src.config import settings, MonitoredPool
from src.ingestion.client import DexClient
from src.engine.detector import parse_raw_transaction, NormalizedSwap
from src.engine.window import stats_window
from src.api.websocket import ws_manager

class Deduplicator:
    def __init__(self, max_size: int = 10_000):
        self.max_size = max_size
        self.seen_set: Set[str] = set()
        self.seen_queue: Deque[str] = deque()

    def is_new(self, tx_id: str) -> bool:
        if tx_id in self.seen_set:
            return False
        self.seen_set.add(tx_id)
        self.seen_queue.append(tx_id)
        
        # Keep bounded
        if len(self.seen_queue) > self.max_size:
            evicted = self.seen_queue.popleft()
            self.seen_set.discard(evicted)
        return True

class StreamPoller:
    def __init__(self, pools: list[MonitoredPool] = settings.MONITORED_POOLS):
        self.pools = pools
        self.client: Optional[DexClient] = None
        self.dedup = Deduplicator()
        self.is_running = False
        self._task: Optional[asyncio.Task] = None
        self._metrics_task: Optional[asyncio.Task] = None

    async def start(self):
        if self.is_running:
            return
        self.is_running = True
        self.client = DexClient()
        self._task = asyncio.create_task(self._poll_loop())
        self._metrics_task = asyncio.create_task(self._metrics_broadcast_loop())

    async def stop(self):
        self.is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._metrics_task:
            self._metrics_task.cancel()
            try:
                await self._metrics_task
            except asyncio.CancelledError:
                pass
        if self.client:
            await self.client.close()

    async def _poll_pool(self, pool: MonitoredPool):
        if not self.client:
            return
        raw_txs = await self.client.fetch_pool_transactions(pool.chain, pool.pool_id, limit=15)
        # Process from oldest to newest in batch
        for raw in reversed(raw_txs):
            tx_id = raw.get("id")
            if not tx_id:
                continue
            if self.dedup.is_new(tx_id):
                swap = parse_raw_transaction(raw, pool)
                if swap:
                    stats_window.add_swap(swap)
                    await ws_manager.broadcast_swap(swap)

    async def _poll_loop(self):
        while self.is_running:
            try:
                # Concurrent fetch across pools
                tasks = [self._poll_pool(p) for p in self.pools]
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception:
                pass
            await asyncio.sleep(settings.POLL_INTERVAL_SECONDS)

    async def _metrics_broadcast_loop(self):
        while self.is_running:
            try:
                metrics = stats_window.get_metrics()
                await ws_manager.broadcast_metrics(metrics)
            except Exception:
                pass
            await asyncio.sleep(2.0)

stream_poller = StreamPoller()
