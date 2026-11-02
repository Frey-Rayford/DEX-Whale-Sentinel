import asyncio
import unittest
from unittest.mock import AsyncMock, patch
import httpx
from src.config import settings
from src.ingestion.client import DexClient
from src.ingestion.poller import StreamPoller


class IngestionTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_poll_is_degraded_and_recovers(self):
        pool = settings.MONITORED_POOLS[0]
        poller = StreamPoller([pool])
        poller.client = AsyncMock()
        poller.client.fetch_pool_transactions.side_effect = RuntimeError('upstream down')
        await poller._poll_pool(pool)
        self.assertEqual(poller.health_snapshot()['status'], 'DEGRADED')
        self.assertEqual(poller.health_snapshot()['pools'][0]['errors'], 1)
        poller.client.fetch_pool_transactions.side_effect = None
        poller.client.fetch_pool_transactions.return_value = []
        await poller._poll_pool(pool)
        self.assertEqual(poller.health_snapshot()['status'], 'ONLINE')

    async def test_retry_then_success(self):
        count = 0
        def respond(request):
            nonlocal count
            count += 1
            return httpx.Response(503) if count == 1 else httpx.Response(200, json={'transactions': []})
        client = DexClient()
        await client.client.aclose()
        client.client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        try:
            with patch('src.ingestion.client.asyncio.sleep', new=AsyncMock()):
                self.assertEqual(await client.fetch_pool_transactions('ethereum', 'pool'), [])
            self.assertEqual(count, 2)
        finally:
            await client.close()

    async def test_bounded_catchup_is_visible(self):
        pool = settings.MONITORED_POOLS[0]
        poller = StreamPoller([pool])
        poller.client = AsyncMock()
        poller.client.fetch_pool_transactions.return_value = [{} for _ in range(15)]
        await poller._poll_pool(pool)
        self.assertEqual(poller.client.fetch_pool_transactions.await_count, 5)
        self.assertTrue(poller.health_snapshot()['pools'][0]['catchup_limit_reached'])


if __name__ == '__main__':
    unittest.main()
