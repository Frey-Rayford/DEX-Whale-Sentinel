# DEX-Whale-Sentinel

[Tiếng Việt](#tiếng-việt) • [English](#english)

---

<a name="tiếng-việt"></a>
# TIẾNG VIỆT

Hệ thống thu thập, xử lý và giám sát luồng giao dịch Crypto DEX thời gian thực (Real-time Stream Pipeline) với độ trễ thấp, kết nối trực tiếp dữ liệu on-chain từ **DexPaprika API** (hỗ trợ Ethereum, Solana, Base, Arbitrum), phát hiện các lệnh cá voi (Whale Alert) và tính toán chỉ số chênh lệch khối lượng mua/bán (Cumulative Volume Delta - CVD) trên cửa sổ trượt (Sliding Window).

Dự án sở hữu giao diện **Trading Terminal (Bloomberg / Linear Lite)** tối giản, độ đặc thông tin cao, sử dụng font monospaced chuẩn số học tài chính và **hoàn toàn không sử dụng phong cách đồ họa AI slop**.

![Trading Terminal Preview](https://raw.githubusercontent.com/PhongPro69/DEX-Whale-Sentinel/main/docs/terminal_preview.png)

---

## 1. Kiến Trúc Hệ Thống (Architecture)

```text
DexPaprika API (REST / SSE)
[Ethereum, Solana, Base, Arbitrum]
               │
               ▼
┌──────────────────────────────────────────────────────────┐
│ Ingestion Layer (AsyncIO Multi-Chain Poller)             │
│ - Async HTTP Client (httpx với connection pooling)       │
│ - Hash-based Deduplication Buffer (O(1) LRU bounded set) │
│ - Đảm bảo mỗi lệnh swap chỉ xử lý đúng một lần           │
└──────────────────────────────┬───────────────────────────┘
                               │ Normalized Swap Stream
                               ▼
┌──────────────────────────────────────────────────────────┐
│ Processing & Analytics Engine                            │
│ - Whale Tier Categorization (>= $10k, $25k, $100k)       │
│ - 5-min Sliding Window (CVD, Buy Ratio, Velocity)        │
│ - In-memory Ring Buffer (lưu trữ 250 swap gần nhất)      │
└──────────────────────────────┬───────────────────────────┘
                               │ Filtered Swaps & Metrics
                               ▼
┌──────────────────────────────────────────────────────────┐
│ FastAPI WebSocket Hub & REST API                         │
│ - WS /ws/stream: Push swaps & metrics với độ trễ < 5ms   │
│ - REST /api/status, /api/metrics, /api/whales            │
│ - REST /api/simulate-whale: Test radar theo yêu cầu      │
└──────────────────────────────┬───────────────────────────┘
                               │ WebSocket
                               ▼
┌──────────────────────────────────────────────────────────┐
│ Terminal-Grade Dashboard (Dark Zinc / Slate)             │
│ - Live Order Flow Table (Flash micro-animation Buy/Sell) │
│ - Whale Radar Alert Feed (kèm link Etherscan/Solscan)    │
│ - CVD Pressure Bar & Phân bổ Volume theo Chain           │
│ - Web Audio API Synthesizer (Cảnh báo âm thanh cá voi)   │
└──────────────────────────────────────────────────────────┘
```

---

## 2. Các Tính Năng Kỹ Thuật Nổi Bật

### A. Cơ Chế Khử Trùng Lặp Giao Dịch (Deduplication Engine)
Trong môi trường mạng phân tán, việc kéo dữ liệu từ nhiều node/API có thể trả về các giao dịch trùng nhau giữa các đợt fetch. Lớp `Deduplicator` kết hợp giữa `set` và `deque` giới hạn kích thước (bounded capacity):
- Tra cứu $O(1)$ để kiểm tra mã giao dịch (`tx_id`) đã xuất hiện hay chưa.
- Tự động xả (evict) các hash cũ nhất khi bộ nhớ vượt ngưỡng, giữ dung lượng RAM ổn định ở mức vài MB.

### B. Phân Loại Giao Dịch & Ngưỡng Cá Voi (Whale Tiers)
Mỗi giao dịch được chuẩn hóa và phân loại theo quy mô tài chính:
- **`NORMAL` (< $10,000):** Giao dịch bán lẻ thông thường.
- **`NOTABLE` ($10,000 - $25,000):** Giao dịch đáng chú ý.
- **`WHALE` ($25,000 - $100,000):** Lệnh cá voi, kích hoạt radar và âm thanh cảnh báo.
- **`MEGA_WHALE` (≥ $100,000):** Lệnh tổ chức / cá mập, đánh dấu nổi bật trên toàn bộ terminal.

### C. Cửa Sổ Trượt Tính Toán CVD (Cumulative Volume Delta)
Chỉ số CVD đo lường áp lực mua ròng hoặc bán ròng trong 5 phút gần nhất:
$$\text{CVD} = \text{Buy Volume} - \text{Sell Volume}$$
- Nếu $\text{CVD} > 0$: Thị trường đang có áp lực mua chủ động chiếm ưu thế.
- Nếu $\text{CVD} < 0$: Phe bán chủ động đang xả hàng.
- Kết hợp tính toán tốc độ giao dịch tức thời ($\text{tx/sec}$) trong cửa sổ 60 giây.

---

## 3. Cấu Trúc Thư Mục

```text
dex-whale-sentinel/
├── src/
│   ├── config.py             # Cấu hình pools, thresholds, timeouts
│   ├── ingestion/
│   │   ├── client.py         # Async DexPaprika HTTP Client
│   │   └── poller.py         # Multi-chain background poller & deduplicator
│   ├── engine/
│   │   ├── detector.py       # Model NormalizedSwap & phân loại whale
│   │   └── window.py         # Sliding window stats (CVD, velocity)
│   ├── api/
│   │   ├── routes.py         # REST endpoints (/api/status, /api/metrics...)
│   │   └── websocket.py      # Non-blocking WebSocket Manager
│   ├── static/
│   │   └── index.html        # Giao diện Trading Terminal cao cấp
│   └── main.py               # FastAPI entrypoint & Lifespan manager
├── tests/
│   └── test_engine.py        # 4 unit tests (Deduplication, Tiers, CVD...)
├── pyproject.toml            # Cấu hình pytest
├── requirements.txt          # Danh sách thư viện
└── README.md
```

---

## 4. Hướng Dẫn Cài Đặt & Chạy Dự Án

### Bước 1: Khởi tạo môi trường ảo Python
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### Bước 2: Cài đặt thư viện phụ thuộc
```powershell
pip install -r requirements.txt
```

### Bước 3: Chạy bộ kiểm thử tự động
```powershell
pytest -v
```

### Bước 4: Khởi động server
```powershell
python -m uvicorn src.main:app --port 8000
```
Truy cập trình duyệt tại địa chỉ: `http://localhost:8000`

---
---

<a name="english"></a>
# ENGLISH

**DEX-Whale-Sentinel** is a high-throughput, low-latency real-time cryptocurrency DEX stream ingestion and anomaly detection pipeline built with **Python (FastAPI, AsyncIO, WebSockets)**. It connects directly to live on-chain decentralized exchange data via the **DexPaprika API** (covering Ethereum, Solana, Base, and Arbitrum).

The project features a **Bloomberg / Linear-style dark Trading Terminal** with high information density, tabular monospaced numbers, micro-flash transaction updates, and zero artificial intelligence aesthetic clutter (no neon blurs, no cartoon emojis).

---

## 1. System Architecture

```text
DexPaprika API (REST / SSE)
[Ethereum, Solana, Base, Arbitrum]
               │
               ▼
┌──────────────────────────────────────────────────────────┐
│ Ingestion Layer (AsyncIO Multi-Chain Poller)             │
│ - Async HTTP Client (httpx connection pooling)           │
│ - Hash-based Deduplication Buffer (O(1) LRU set)         │
│ - Guarantees exactly-once processing per swap            │
└──────────────────────────────┬───────────────────────────┘
                               │ Normalized Swap Stream
                               ▼
┌──────────────────────────────────────────────────────────┐
│ Processing & Analytics Engine                            │
│ - Whale Tier Categorization (>= $10k, $25k, $100k)       │
│ - 5-min Sliding Window (CVD, Buy Ratio, Velocity)        │
│ - In-memory Ring Buffer (250 recent transactions)        │
└──────────────────────────────┬───────────────────────────┘
                               │ Filtered Swaps & Metrics
                               ▼
┌──────────────────────────────────────────────────────────┐
│ FastAPI WebSocket Hub & REST API                         │
│ - WS /ws/stream: Sub-5ms broadcast to connected clients  │
│ - REST /api/status, /api/metrics, /api/whales            │
│ - REST /api/simulate-whale: Instant trigger for QA       │
└──────────────────────────────┬───────────────────────────┘
                               │ WebSocket
                               ▼
┌──────────────────────────────────────────────────────────┐
│ Institutional Trading Terminal (Dark Zinc / Slate)       │
│ - Live Order Flow Table (directional micro-animations)   │
│ - Whale Radar Alert Feed (with direct block explorer links)│
│ - Real-Time CVD Pressure Gauge & Chain Volume Breakdown  │
│ - Web Audio API Synthesizer (Auditory alerts)            │
└──────────────────────────────────────────────────────────┘
```

---

## 2. Core Engineering Highlights

- **$O(1)$ Hash Deduplication:** Combines a hash set with a bounded deque to eliminate redundant swap ingestions across concurrent polling cycles without memory leaks.
- **Sliding Window Analytics:** Computes 5-minute Cumulative Volume Delta ($\text{CVD} = V_{\text{buy}} - V_{\text{sell}}$) and 60-second transaction velocity ($\text{tx/s}$) in memory.
- **Zero-Latency WebSocket Hub:** Dispatches normalized JSON swap events and telemetry metrics asynchronously to web clients with sub-5ms broadcast overhead.
- **Institutional Design Philosophy:** Strict dark zinc/slate theme, monospaced tabular figures (`tabular-nums`), audio ping synthesized via Web Audio API, and full inspection drawer for raw transaction payloads.

---

## 3. Quickstart Guide

```powershell
# 1. Activate environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run test suite
pytest -v

# 4. Start terminal server
python -m uvicorn src.main:app --port 8000
```
Open `http://localhost:8000` in your web browser.
