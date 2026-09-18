from pydantic import BaseModel
from typing import List, Dict, Any

class MonitoredPool(BaseModel):
    chain: str
    pool_id: str
    dex_name: str
    pair_label: str
    explorer_tx_base: str

class Settings:
    APP_NAME: str = "DEX-Whale-Sentinel"
    APP_VERSION: str = "1.0.0"
    DEXPAPRIKA_BASE_URL: str = "https://api.dexpaprika.com"
    
    # Whale Thresholds
    NOTABLE_THRESHOLD_USD: float = 10_000.0
    WHALE_THRESHOLD_USD: float = 25_000.0
    MEGA_WHALE_THRESHOLD_USD: float = 100_000.0
    
    # Buffer & Window sizes
    RING_BUFFER_SIZE: int = 250
    WHALE_BUFFER_SIZE: int = 100
    SLIDING_WINDOW_SECONDS: int = 300  # 5 minutes
    
    # Ingestion polling frequency
    POLL_INTERVAL_SECONDS: float = 2.5
    HTTP_TIMEOUT_SECONDS: float = 8.0
    
    # Target Pools
    MONITORED_POOLS: List[MonitoredPool] = [
        MonitoredPool(
            chain="ethereum",
            pool_id="0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
            dex_name="Uniswap V3",
            pair_label="WETH / USDC",
            explorer_tx_base="https://etherscan.io/tx/"
        ),
        MonitoredPool(
            chain="ethereum",
            pool_id="0x11b815efb8f581194ae79006d24e0d814b7697f6",
            dex_name="Uniswap V3",
            pair_label="WETH / USDT",
            explorer_tx_base="https://etherscan.io/tx/"
        ),
        MonitoredPool(
            chain="ethereum",
            pool_id="0x2b21c65d9a7dc6926ee330a1c6e5a8037fd81774f3dc066536f800128e39f634",
            dex_name="Uniswap V4",
            pair_label="USDC / USDT",
            explorer_tx_base="https://etherscan.io/tx/"
        ),
        MonitoredPool(
            chain="base",
            pool_id="0x6c561b446416e1a00e8e93e221854d6ea4171372",
            dex_name="Uniswap V3",
            pair_label="WETH / USDC",
            explorer_tx_base="https://basescan.org/tx/"
        ),
        MonitoredPool(
            chain="base",
            pool_id="0x3fe04a59ebd38cf06080a6f60a98d124eb59392a",
            dex_name="Aerodrome",
            pair_label="WETH / USDC",
            explorer_tx_base="https://basescan.org/tx/"
        ),
        MonitoredPool(
            chain="arbitrum",
            pool_id="0xc6962004f452be9203591991d15f6b388e09e8d0",
            dex_name="Uniswap V3",
            pair_label="WETH / USDC",
            explorer_tx_base="https://arbiscan.io/tx/"
        ),
        MonitoredPool(
            chain="arbitrum",
            pool_id="0x2f5e87c9312fa29aed5c179e456625d79015299c",
            dex_name="Uniswap V3",
            pair_label="WBTC / WETH",
            explorer_tx_base="https://arbiscan.io/tx/"
        ),
        MonitoredPool(
            chain="solana",
            pool_id="Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtLG4crryfu44zE",
            dex_name="Orca",
            pair_label="SOL / USDC",
            explorer_tx_base="https://solscan.io/tx/"
        )
    ]

settings = Settings()
