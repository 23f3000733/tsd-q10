import base64
import threading
import time
from collections import deque
from typing import Optional

from fastapi import FastAPI, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

TOTAL_ORDERS = 50          # T
RATE_LIMIT = 18            # R requests
WINDOW_SECONDS = 10        # per 10s

app = FastAPI(title="Orders API")

# ---------- Rate limiting (per X-Client-Id, sliding window) ----------
_hits: dict[str, deque] = {}
_hits_lock = threading.Lock()


@app.middleware("http")
async def rate_limit(request: Request, call_next):
    client_id = request.headers.get("x-client-id")
    if client_id and request.method != "OPTIONS":
        now = time.monotonic()
        with _hits_lock:
            q = _hits.setdefault(client_id, deque())
            while q and now - q[0] >= WINDOW_SECONDS:
                q.popleft()
            if len(q) >= RATE_LIMIT:
                retry_after = max(1, int(WINDOW_SECONDS - (now - q[0])) + 1)
                return JSONResponse(
                    status_code=429,
                    content={"detail": "Rate limit exceeded"},
                    headers={
                        "Retry-After": str(retry_after),
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Expose-Headers": "Retry-After",
                    },
                )
            q.append(now)
    return await call_next(request)


# CORS added after the rate limiter so it wraps it (outermost) and
# preflight requests / headers are always handled.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After", "Idempotency-Key"],
)

# ---------- Data ----------
CATALOG = [
    {"id": i, "item": f"item-{i}", "quantity": 1, "status": "created"}
    for i in range(1, TOTAL_ORDERS + 1)
]

_created: dict[str, dict] = {}       # idempotency key -> order
_next_id = 1000                      # created orders live outside 1..T catalog
_orders_lock = threading.Lock()


# ---------- Idempotent POST ----------
@app.post("/orders", status_code=201)
async def create_order(
    request: Request,
    idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
):
    global _next_id
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
    except Exception:
        body = {}

    with _orders_lock:
        if idempotency_key and idempotency_key in _created:
            return JSONResponse(status_code=201, content=_created[idempotency_key])
        _next_id += 1
        order = {"id": str(_next_id), "status": "created", **{k: v for k, v in body.items() if k != "id"}}
        order["id"] = str(_next_id)
        if idempotency_key:
            _created[idempotency_key] = order
        return JSONResponse(status_code=201, content=order)


# ---------- Cursor pagination ----------
def _encode(offset: int) -> str:
    return base64.urlsafe_b64encode(f"o:{offset}".encode()).decode()


def _decode(cursor: str) -> int:
    raw = base64.urlsafe_b64decode(cursor.encode()).decode()
    if not raw.startswith("o:"):
        raise ValueError
    off = int(raw[2:])
    if off < 0:
        raise ValueError
    return off


@app.get("/orders")
def list_orders(limit: int = 10, cursor: Optional[str] = None):
    limit = max(1, min(limit, 100))
    offset = 0
    if cursor:
        try:
            offset = _decode(cursor)
        except Exception:
            return JSONResponse(status_code=400, content={"detail": "Invalid cursor"})
    items = CATALOG[offset: offset + limit]
    end = offset + len(items)
    next_cursor = _encode(end) if end < len(CATALOG) else None
    return {"items": items, "next_cursor": next_cursor}


@app.get("/")
def root():
    return {"status": "ok"}
