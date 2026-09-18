import random
import time
from datetime import datetime, timezone
from fastapi import APIRouter
from src.config import settings
from src.engine.window import stats_window
from src.engine.detector import NormalizedSwap, classify_tier
from src.api.websocket import ws_manager

router = APIRouter(prefix="/api")

@router.get("/status")
async def get_status():
    return {
        "status": "ONLINE",
        "app": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "monitored_pools_count": len(settings.MONITORED_POOLS),
        "active_connections": len(ws_manager.active_connections),
        "metrics": stats_window.get_metrics()
    }

@router.get("/pools")
async def get_pools():
    return [p.model_dump() for p in settings.MONITORED_POOLS]

@router.get("/metrics")
async def get_metrics():
    return stats_window.get_metrics()

@router.get("/recent")
async def get_recent_swaps(limit: int = 100):
    return [s.model_dump() for s in stats_window.get_recent_swaps(limit=limit)]

@router.get("/whales")
async def get_recent_whales(limit: int = 50):
    return [s.model_dump() for s in stats_window.get_recent_whales(limit=limit)]

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
