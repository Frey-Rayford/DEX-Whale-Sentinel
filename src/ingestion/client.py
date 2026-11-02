import asyncio
import httpx
from typing import List, Dict, Any, Optional
from src.config import settings

class DexClient:
    def __init__(self, base_url: str = settings.DEXPAPRIKA_BASE_URL, timeout: float = settings.HTTP_TIMEOUT_SECONDS):
        self.base_url = base_url.rstrip("/")
        self.client = httpx.AsyncClient(
            timeout=timeout,
            headers={
                "User-Agent": "DEX-Whale-Sentinel/1.0 (Realtime Analytics Stream)",
                "Accept": "application/json"
            }
        )

    async def fetch_pool_transactions(self, chain: str, pool_id: str, limit: int = 15, page: int = 1):
        url = f"{self.base_url}/networks/{chain}/pools/{pool_id}/transactions"
        for attempt in range(3):
            try:
                response = await self.client.get(url, params={"limit": limit, "page": page})
                response.raise_for_status()
                transactions = response.json().get("transactions")
                if not isinstance(transactions, list):
                    raise ValueError("Upstream response is missing the transactions list")
                return transactions
            except (httpx.TransportError, httpx.HTTPStatusError) as exc:
                if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code != 429 and exc.response.status_code < 500:
                    raise
                if attempt == 2:
                    raise
                await asyncio.sleep(.5 * 2**attempt)

    async def fetch_networks(self) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/networks"
        try:
            resp = await self.client.get(url)
            if resp.status_code == 200:
                return resp.json()
            return []
        except Exception:
            return []

    async def close(self):
        await self.client.aclose()
