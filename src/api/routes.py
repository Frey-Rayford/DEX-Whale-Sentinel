import random
import time
from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel
from fastapi import APIRouter, Query
from src.config import settings
from src.ingestion.poller import stream_poller
from src.engine.window import stats_window
from src.engine.detector import NormalizedSwap, classify_tier
from src.api.websocket import ws_manager

router = APIRouter(prefix="/api")

class SettingsUpdate(BaseModel):
    whale_threshold_usd: Optional[float] = None
    notable_threshold_usd: Optional[float] = None

class WebhookTestPayload(BaseModel):
    webhook_url: str
    channel_name: Optional[str] = "Telegram / Discord"

@router.get("/status")
async def get_status():
    return {
        "status": stream_poller.health_snapshot()["status"],
        "ingestion": stream_poller.health_snapshot(),
        "app": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "monitored_pools_count": len(settings.MONITORED_POOLS),
        "active_connections": len(ws_manager.active_connections),
        "whale_threshold_usd": settings.WHALE_THRESHOLD_USD,
        "metrics": stats_window.get_metrics()
    }

@router.get("/pools")
async def get_pools():
    return [p.model_dump() for p in settings.MONITORED_POOLS]

@router.get("/metrics")
async def get_metrics():
    return stats_window.get_metrics()

@router.get("/recent")
async def get_recent_swaps(limit: int = Query(100, ge=1, le=250)):
    return [s.model_dump() for s in stats_window.get_recent_swaps(limit=limit)]

@router.get("/whales")
async def get_recent_whales(limit: int = Query(50, ge=1, le=100)):
    return [s.model_dump() for s in stats_window.get_recent_whales(limit=limit)]

@router.get("/whales/leaderboard")
async def get_whale_leaderboard(limit: int = Query(25, ge=1, le=100)):
    return stats_window.get_top_wallets(limit=limit)

@router.get("/chart/bars")
async def get_chart_bars():
    return stats_window.get_chart_bars()

@router.post("/settings")
async def update_settings(payload: SettingsUpdate):
    if payload.whale_threshold_usd is not None and payload.whale_threshold_usd > 0:
        settings.WHALE_THRESHOLD_USD = payload.whale_threshold_usd
    if payload.notable_threshold_usd is not None and payload.notable_threshold_usd > 0:
        settings.NOTABLE_THRESHOLD_USD = payload.notable_threshold_usd
    return {
        "status": "updated",
        "whale_threshold_usd": settings.WHALE_THRESHOLD_USD,
        "notable_threshold_usd": settings.NOTABLE_THRESHOLD_USD
    }

@router.post("/webhook/test")
async def test_webhook(payload: WebhookTestPayload):
    # Simulated webhook notification dispatch
    return {
        "status": "simulated",
        "delivered": False,
        "message": "Preview only; no webhook request was sent",
        "sample_alert": {
            "title": "WHALE ALERT DETECTED",
            "chain": "ethereum",
            "pair": "WETH / USDC",
            "volume_usd": "$145,200.00",
            "side": "BUY",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    }

@router.post("/simulate-whale")
async def simulate_whale():
    """Simulates a live whale swap for testing alerts & radar instantly."""
    pool = random.choice(settings.MONITORED_POOLS)
    side = random.choice(["BUY", "SELL"])
    vol = round(random.uniform(35_000, 180_000), 2)
    tier = classify_tier(vol)
    
    sim_swap = NormalizedSwap(
        id=f"sim_0x{random.randbytes(16).hex()}",
        chain=pool.chain,
        pool_id=pool.pool_id,
        dex_name=pool.dex_name,
        pair_label=pool.pair_label,
        tx_hash=f"0x{random.randbytes(32).hex()}",
        sender=f"0x{random.randbytes(20).hex()}",
        recipient=f"0x{random.randbytes(20).hex()}",
        side=side,
        base_symbol=pool.pair_label.split(" / ")[0],
        quote_symbol=pool.pair_label.split(" / ")[1],
        base_amount=round(vol / 2500.0, 4),
        quote_amount=vol,
        price_usd=2500.0,
        volume_usd=vol,
        tier=tier,
        block_number=random.randint(26000000, 27000000),
        created_at=datetime.now(timezone.utc).isoformat(),
        explorer_url=f"{pool.explorer_tx_base}0x{random.randbytes(32).hex()}"
    )
    
    stats_window.add_swap(sim_swap)
    await ws_manager.broadcast_swap(sim_swap)
    return {"status": "broadcasted", "swap": sim_swap.model_dump()}
