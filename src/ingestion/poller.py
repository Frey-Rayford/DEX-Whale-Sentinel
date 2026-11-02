import asyncio
import logging
from datetime import datetime, timezone
from collections import deque
from typing import Set, Deque, Optional
from src.config import settings, MonitoredPool
from src.ingestion.client import DexClient
from src.engine.detector import parse_raw_transaction, NormalizedSwap
from src.engine.window import stats_window
from src.api.websocket import ws_manager

logger = logging.getLogger(__name__)

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
        self.health = {f"{p.chain}:{p.pool_id}": {"last_success": None, "last_error": None, "consecutive_failures": 0, "errors": 0, "catchup_limit_reached": False} for p in pools}

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

    def health_snapshot(self):
        now = datetime.now(timezone.utc)
        pools = []
        for key, value in self.health.items():
            age = None if value["last_success"] is None else (now - datetime.fromisoformat(value["last_success"])).total_seconds()
            stale = age is None or age > max(30, settings.POLL_INTERVAL_SECONDS * 4)
            pools.append({"pool": key, **value, "age_seconds": age, "stale": stale})
        status = "STARTING" if all(p["last_success"] is None and p["errors"] == 0 for p in pools) else "ONLINE"
        if any(p["stale"] or p["consecutive_failures"] or p["catchup_limit_reached"] for p in pools):
            status = "DEGRADED" if status != "STARTING" else status
        return {"status": status, "pools": pools}

    async def _poll_pool(self, pool: MonitoredPool):
        if not self.client:
            return
        key = f"{pool.chain}:{pool.pool_id}"
        health = self.health[key]
        collected = []
        health["catchup_limit_reached"] = False
        try:
            for page in range(1, 6):
                transactions = await self.client.fetch_pool_transactions(pool.chain, pool.pool_id, limit=15, page=page)
                known = any(f"{key}:{raw.get('id')}" in self.dedup.seen_set for raw in transactions)
                collected.extend(transactions)
                if known or len(transactions) < 15:
                    break
            else:
                health["catchup_limit_reached"] = True
                logger.warning("Catch-up page limit reached for %s; history may be incomplete", key)
            for raw in reversed(collected):
                tx_id = raw.get("id")
                if not tx_id:
                    continue
                swap = parse_raw_transaction(raw, pool)
                if swap and self.dedup.is_new(f"{key}:{tx_id}"):
                    stats_window.add_swap(swap)
                    await ws_manager.broadcast_swap(swap)
            health.update(last_success=datetime.now(timezone.utc).isoformat(), last_error=None, consecutive_failures=0)
        except Exception as exc:
            health["errors"] += 1
            health["consecutive_failures"] += 1
            health["last_error"] = str(exc)[:250]
            logger.warning("Pool fetch failed for %s: %s", key, exc)

    async def _poll_loop(self):
        while self.is_running:
            try:
                # Concurrent fetch across pools
                tasks = [self._poll_pool(p) for p in self.pools]
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception:
                logger.exception("Background poller task failed")
            await asyncio.sleep(settings.POLL_INTERVAL_SECONDS)

    async def _metrics_broadcast_loop(self):
        while self.is_running:
            try:
                metrics = stats_window.get_metrics()
                await ws_manager.broadcast_metrics(metrics)
            except Exception:
                logger.exception("Background poller task failed")
            await asyncio.sleep(2.0)

stream_poller = StreamPoller()
