import asyncio
import json
from typing import Set, Dict, Any
from fastapi import WebSocket
from src.engine.detector import NormalizedSwap

class WebSocketManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self._lock:
            self.active_connections.add(websocket)

    async def disconnect(self, websocket: WebSocket):
        async with self._lock:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)

    async def broadcast_json(self, payload: Dict[str, Any]):
        if not self.active_connections:
            return
            
        message = json.dumps(payload)
        dead_connections = set()
        
        async with self._lock:
            connections = list(self.active_connections)
            
        for ws in connections:
            try:
                await ws.send_text(message)
            except Exception:
                dead_connections.add(ws)
                
        if dead_connections:
            async with self._lock:
                for ws in dead_connections:
                    self.active_connections.discard(ws)

    async def broadcast_swap(self, swap: NormalizedSwap):
        payload = {
            "type": "SWAP",
            "data": swap.model_dump()
        }
        await self.broadcast_json(payload)

    async def broadcast_metrics(self, metrics: Dict[str, Any]):
        payload = {
            "type": "METRICS",
            "data": metrics
        }
        await self.broadcast_json(payload)

ws_manager = WebSocketManager()
