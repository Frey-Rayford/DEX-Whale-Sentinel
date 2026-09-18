from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, timezone
from src.config import settings, MonitoredPool

class NormalizedSwap(BaseModel):
    id: str
    chain: str
    pool_id: str
    dex_name: str
    pair_label: str
    tx_hash: str
    sender: str
    recipient: str
    side: str  # BUY or SELL
    base_symbol: str
    quote_symbol: str
    base_amount: float
    quote_amount: float
    price_usd: float
    volume_usd: float
    tier: str  # NORMAL, NOTABLE, WHALE, MEGA_WHALE
    block_number: Optional[int] = None
    created_at: str
    explorer_url: str

def classify_tier(volume_usd: float) -> str:
    if volume_usd >= settings.MEGA_WHALE_THRESHOLD_USD:
        return "MEGA_WHALE"
    if volume_usd >= settings.WHALE_THRESHOLD_USD:
        return "WHALE"
    if volume_usd >= settings.NOTABLE_THRESHOLD_USD:
        return "NOTABLE"
    return "NORMAL"

def parse_raw_transaction(raw: dict, pool: MonitoredPool) -> Optional[NormalizedSwap]:
    try:
        tx_id = raw.get("id", "")
        if not tx_id:
            return None
            
        token_0_sym = raw.get("token_0_symbol") or "TOKEN0"
        token_1_sym = raw.get("token_1_symbol") or "TOKEN1"
        
        vol_0 = float(raw.get("volume_0", 0.0) or 0.0)
        vol_1 = float(raw.get("volume_1", 0.0) or 0.0)
        
        price_0_usd = float(raw.get("price_0_usd", 0.0) or 0.0)
        price_1_usd = float(raw.get("price_1_usd", 0.0) or 0.0)
        
        # Determine total USD volume
        usd_vol_from_0 = abs(vol_0 * price_0_usd) if price_0_usd > 0 else 0.0
        usd_vol_from_1 = abs(vol_1 * price_1_usd) if price_1_usd > 0 else 0.0
        volume_usd = max(usd_vol_from_0, usd_vol_from_1)
        
        # If both are 0, fallback to raw volume field if available
        if volume_usd <= 0.0 and "volume_usd" in raw:
            volume_usd = float(raw.get("volume_usd", 0.0) or 0.0)
            
        # Determine Buy/Sell side relative to base token (token_0)
        # In Uniswap V3 convention:
        # amount_0 < 0 => pool sent token_0 to user (Trader bought token_0)
        # amount_0 > 0 => pool received token_0 from user (Trader sold token_0)
        amount_0 = float(raw.get("amount_0", 0.0) or 0.0)
        amount_1 = float(raw.get("amount_1", 0.0) or 0.0)
        
        if amount_0 < 0:
            side = "BUY"
        elif amount_0 > 0:
            side = "SELL"
        else:
            # Fallback if amount_0 is 0
            side = "BUY" if amount_1 > 0 else "SELL"
            
        base_price = price_0_usd if price_0_usd > 0 else price_1_usd
        
        # Format clean TX hash for explorer
        clean_tx_hash = tx_id
        explorer_link = f"{pool.explorer_tx_base}{clean_tx_hash}"
        
        tier = classify_tier(volume_usd)
        
        return NormalizedSwap(
            id=tx_id,
            chain=pool.chain,
            pool_id=pool.pool_id,
            dex_name=pool.dex_name,
            pair_label=pool.pair_label,
            tx_hash=clean_tx_hash,
            sender=raw.get("sender", "0x0"),
            recipient=raw.get("recipient", "0x0"),
            side=side,
            base_symbol=token_0_sym,
            quote_symbol=token_1_sym,
            base_amount=abs(vol_0),
            quote_amount=abs(vol_1),
            price_usd=base_price,
            volume_usd=round(volume_usd, 2),
            tier=tier,
            block_number=raw.get("created_at_block_number"),
            created_at=raw.get("created_at", datetime.now(timezone.utc).isoformat()),
            explorer_url=explorer_link
        )
    except Exception:
        return None
