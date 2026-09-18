import time
from collections import deque
from typing import List, Dict, Any, Deque
from src.engine.detector import NormalizedSwap
from src.config import settings

class SlidingWindowStats:
    def __init__(self, window_seconds: int = 300):
        self.window_seconds = window_seconds
        # Stores tuples of (monotonic_time, swap)
        self.events: Deque[tuple[float, NormalizedSwap]] = deque()
        self.recent_swaps: Deque[NormalizedSwap] = deque(maxlen=settings.RING_BUFFER_SIZE)
        self.recent_whales: Deque[NormalizedSwap] = deque(maxlen=settings.WHALE_BUFFER_SIZE)
        
        # Wallet Leaderboard: address -> dict
        self.wallets: Dict[str, Dict[str, Any]] = {}
        
        # 1-minute time bars for Charting (stores last 30 minutes)
        # key: minute timestamp integer (timestamp // 60 * 60)
        self.minute_bars: Dict[int, Dict[str, Any]] = {}
        
        # Ingested totals
        self.total_ingested_swaps: int = 0
        self.total_ingested_volume_usd: float = 0.0
        self.start_time: float = time.time()

    def add_swap(self, swap: NormalizedSwap):
        now = time.time()
        self.events.append((now, swap))
        self.recent_swaps.appendleft(swap)
        
        if swap.tier in ("WHALE", "MEGA_WHALE"):
            self.recent_whales.appendleft(swap)
            
        self.total_ingested_swaps += 1
        self.total_ingested_volume_usd += swap.volume_usd
        
        # Update Wallet Leaderboard
        sender = swap.sender
        if sender and sender != "0x0":
            if sender not in self.wallets:
                self.wallets[sender] = {
                    "address": sender,
                    "chain": swap.chain,
                    "total_volume_usd": 0.0,
                    "buy_volume_usd": 0.0,
                    "sell_volume_usd": 0.0,
                    "tx_count": 0,
                    "last_seen": swap.created_at,
                    "tier": swap.tier,
                    "explorer_url": f"{settings.MONITORED_POOLS[0].explorer_tx_base.replace('/tx/', '/address/')}{sender}" if "0x" in sender else f"https://solscan.io/account/{sender}"
                }
            w = self.wallets[sender]
            w["total_volume_usd"] = round(w["total_volume_usd"] + swap.volume_usd, 2)
            if swap.side == "BUY":
                w["buy_volume_usd"] = round(w["buy_volume_usd"] + swap.volume_usd, 2)
            else:
                w["sell_volume_usd"] = round(w["sell_volume_usd"] + swap.volume_usd, 2)
            w["tx_count"] += 1
            w["last_seen"] = swap.created_at
            if swap.tier in ("WHALE", "MEGA_WHALE"):
                w["tier"] = swap.tier

        # Update 1-minute aggregation bar for CVD Chart
        min_key = int(now // 60 * 60)
        if min_key not in self.minute_bars:
            self.minute_bars[min_key] = {
                "timestamp": min_key,
                "time_label": time.strftime("%H:%M", time.localtime(min_key)),
                "buy_volume": 0.0,
                "sell_volume": 0.0,
                "net_cvd": 0.0,
                "tx_count": 0
            }
        bar = self.minute_bars[min_key]
        if swap.side == "BUY":
            bar["buy_volume"] = round(bar["buy_volume"] + swap.volume_usd, 2)
        else:
            bar["sell_volume"] = round(bar["sell_volume"] + swap.volume_usd, 2)
        bar["net_cvd"] = round(bar["buy_volume"] - bar["sell_volume"], 2)
        bar["tx_count"] += 1

        # Purge minute bars older than 30 minutes
        cutoff_min = min_key - (30 * 60)
        old_keys = [k for k in self.minute_bars if k < cutoff_min]
        for k in old_keys:
            del self.minute_bars[k]

        self.cleanup(now)

    def cleanup(self, now: float = None):
        if now is None:
            now = time.time()
        cutoff = now - self.window_seconds
        while self.events and self.events[0][0] < cutoff:
            self.events.popleft()

    def get_metrics(self) -> Dict[str, Any]:
        now = time.time()
        self.cleanup(now)
        
        buy_vol = 0.0
        sell_vol = 0.0
        buy_count = 0
        sell_count = 0
        whale_count = 0
        chain_volumes: Dict[str, float] = {}
        
        v_cutoff = now - 60.0
        v_tx_count = 0
        
        for ts, s in self.events:
            if s.side == "BUY":
                buy_vol += s.volume_usd
                buy_count += 1
            else:
                sell_vol += s.volume_usd
                sell_count += 1
                
            if s.tier in ("WHALE", "MEGA_WHALE"):
                whale_count += 1
                
            chain_volumes[s.chain] = chain_volumes.get(s.chain, 0.0) + s.volume_usd
            
            if ts >= v_cutoff:
                v_tx_count += 1
                
        cvd = buy_vol - sell_vol
        total_vol = buy_vol + sell_vol
        buy_ratio = (buy_vol / total_vol * 100.0) if total_vol > 0 else 50.0
        velocity = round(v_tx_count / 60.0, 2)
        uptime_seconds = int(now - self.start_time)
        
        return {
            "window_seconds": self.window_seconds,
            "total_volume_5m": round(total_vol, 2),
            "buy_volume_5m": round(buy_vol, 2),
            "sell_volume_5m": round(sell_vol, 2),
            "cvd_5m": round(cvd, 2),
            "buy_ratio": round(buy_ratio, 1),
            "buy_count_5m": buy_count,
            "sell_count_5m": sell_count,
            "whale_count_5m": whale_count,
            "tx_velocity_per_sec": velocity,
            "chain_volumes": {k: round(v, 2) for k, v in chain_volumes.items()},
            "total_ingested_swaps": self.total_ingested_swaps,
            "total_ingested_volume_usd": round(self.total_ingested_volume_usd, 2),
            "uptime_seconds": uptime_seconds
        }

    def get_recent_swaps(self, limit: int = 100) -> List[NormalizedSwap]:
        return list(self.recent_swaps)[:limit]

    def get_recent_whales(self, limit: int = 50) -> List[NormalizedSwap]:
        return list(self.recent_whales)[:limit]

    def get_top_wallets(self, limit: int = 25) -> List[Dict[str, Any]]:
        sorted_wallets = sorted(
            self.wallets.values(),
            key=lambda w: w["total_volume_usd"],
            reverse=True
        )
        return sorted_wallets[:limit]

    def get_chart_bars(self) -> List[Dict[str, Any]]:
        sorted_keys = sorted(self.minute_bars.keys())
        # Calculate running cumulative CVD
        running_cvd = 0.0
        result = []
        for k in sorted_keys:
            b = dict(self.minute_bars[k])
            running_cvd = round(running_cvd + b["net_cvd"], 2)
            b["cumulative_cvd"] = running_cvd
            result.append(b)
        return result

stats_window = SlidingWindowStats(window_seconds=settings.SLIDING_WINDOW_SECONDS)
