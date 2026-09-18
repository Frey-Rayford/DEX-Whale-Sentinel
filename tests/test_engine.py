import pytest
import time
from src.config import MonitoredPool, settings
from src.engine.detector import classify_tier, parse_raw_transaction, NormalizedSwap
from src.ingestion.poller import Deduplicator
from src.engine.window import SlidingWindowStats

def test_deduplicator():
    dedup = Deduplicator(max_size=3)
    assert dedup.is_new("tx_1") is True
    assert dedup.is_new("tx_1") is False
    assert dedup.is_new("tx_2") is True
    assert dedup.is_new("tx_3") is True
    assert len(dedup.seen_set) == 3
    
    # Adding 4th should evict tx_1
    assert dedup.is_new("tx_4") is True
    assert len(dedup.seen_set) == 3
    assert "tx_1" not in dedup.seen_set
    assert dedup.is_new("tx_1") is True

def test_classify_tier():
    assert classify_tier(500.0) == "NORMAL"
    assert classify_tier(12_000.0) == "NOTABLE"
    assert classify_tier(35_000.0) == "WHALE"
    assert classify_tier(150_000.0) == "MEGA_WHALE"

def test_parse_raw_transaction():
    pool = MonitoredPool(
        chain="ethereum",
        pool_id="0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
        dex_name="Uniswap V3",
        pair_label="WETH / USDC",
        explorer_tx_base="https://etherscan.io/tx/"
    )
    raw = {
        "id": "0xabc123",
        "token_0_symbol": "WETH",
        "token_1_symbol": "USDC",
        "amount_0": -10.0,  # Negative means BUY base token
        "amount_1": 25000.0,
        "volume_0": 10.0,
        "volume_1": 25000.0,
        "price_0_usd": 2500.0,
        "price_1_usd": 1.0,
        "sender": "0xsender",
        "recipient": "0xrecipient",
        "created_at": "2026-09-18T08:00:00Z",
        "created_at_block_number": 26001000
    }
    swap = parse_raw_transaction(raw, pool)
    assert swap is not None
    assert swap.id == "0xabc123"
    assert swap.side == "BUY"
    assert swap.volume_usd == 25000.0
    assert swap.tier == "WHALE"
    assert swap.base_symbol == "WETH"
    assert swap.quote_symbol == "USDC"
    assert swap.explorer_url == "https://etherscan.io/tx/0xabc123"

def test_sliding_window_cvd():
    window = SlidingWindowStats(window_seconds=10)
    
    # 1 BUY of 30,000 USD
    s1 = NormalizedSwap(
        id="tx_buy",
        chain="ethereum",
        pool_id="0x1",
        dex_name="Uniswap V3",
        pair_label="WETH / USDC",
        tx_hash="0x1",
        sender="0xa",
        recipient="0xb",
        side="BUY",
        base_symbol="WETH",
        quote_symbol="USDC",
        base_amount=12.0,
        quote_amount=30000.0,
        price_usd=2500.0,
        volume_usd=30000.0,
        tier="WHALE",
        created_at="2026-09-18T08:00:00Z",
        explorer_url="https://etherscan.io/tx/0x1"
    )
    
    # 1 SELL of 10,000 USD
    s2 = NormalizedSwap(
        id="tx_sell",
        chain="base",
        pool_id="0x2",
        dex_name="Aerodrome",
        pair_label="WETH / USDC",
        tx_hash="0x2",
        sender="0xc",
        recipient="0xd",
        side="SELL",
        base_symbol="WETH",
        quote_symbol="USDC",
        base_amount=4.0,
        quote_amount=10000.0,
        price_usd=2500.0,
        volume_usd=10000.0,
        tier="NOTABLE",
        created_at="2026-09-18T08:00:01Z",
        explorer_url="https://basescan.org/tx/0x2"
    )
    
    window.add_swap(s1)
    window.add_swap(s2)
    
    metrics = window.get_metrics()
    assert metrics["buy_volume_5m"] == 30000.0
    assert metrics["sell_volume_5m"] == 10000.0
    assert metrics["cvd_5m"] == 20000.0  # 30k - 10k
    assert metrics["buy_ratio"] == 75.0   # 30k / 40k
    assert metrics["whale_count_5m"] == 1
    assert metrics["chain_volumes"]["ethereum"] == 30000.0
    assert metrics["chain_volumes"]["base"] == 10000.0
