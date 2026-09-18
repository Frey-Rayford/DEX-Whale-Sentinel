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

    async def fetch_pool_transactions(self, chain: str, pool_id: str, limit: int = 15) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/networks/{chain}/pools/{pool_id}/transactions"
        params = {"limit": limit, "page": 1}
        try:
            resp = await self.client.get(url, params=params)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("transactions", [])
            return []
        except Exception:
            return []

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
