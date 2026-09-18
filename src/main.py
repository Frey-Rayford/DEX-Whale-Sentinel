from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from src.config import settings
from src.ingestion.poller import stream_poller
from src.api.websocket import ws_manager
from src.api.routes import router as api_router
from src.engine.window import stats_window
import os

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: start background ingestion poller
    await stream_poller.start()
    yield
    # Shutdown: clean up poller
    await stream_poller.stop()

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    lifespan=lifespan
)

# API routes
app.include_router(api_router)

# WebSocket streaming endpoint
@app.websocket("/ws/stream")
async def websocket_stream(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        # Send initial snapshot
        initial_payload = {
            "type": "SNAPSHOT",
            "recent_swaps": [s.model_dump() for s in stats_window.get_recent_swaps(limit=50)],
            "recent_whales": [s.model_dump() for s in stats_window.get_recent_whales(limit=20)],
            "metrics": stats_window.get_metrics(),
            "pools": [p.model_dump() for p in settings.MONITORED_POOLS]
        }
        await websocket.send_json(initial_payload)
        
        # Keep connection open for incoming pings or client filter updates
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        await ws_manager.disconnect(websocket)
    except Exception:
        await ws_manager.disconnect(websocket)

# Serve static dashboard
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

@app.get("/")
async def serve_dashboard():
    index_file = os.path.join(static_dir, "index.html")
    if os.path.exists(index_file):
        return FileResponse(index_file)
    return {"message": "DEX-Whale-Sentinel API is running. index.html not found."}
