"""
Simple in-memory rate limiter (token bucket per IP).
No Redis needed at Phase 2 scale.

Limits:
  - 30 requests/minute per IP (burst up to 10)
  - Returns 429 with Retry-After header when exceeded
"""
import time
from collections import defaultdict
from threading import Lock

from fastapi import Request
from fastapi.responses import JSONResponse

# Token bucket config
RATE_LIMIT_RPS        = 0.5   # 30 req/min = 0.5 req/sec sustained
RATE_LIMIT_BURST      = 10    # allow short bursts up to 10 requests
CLEANUP_INTERVAL_SECS = 300   # purge idle buckets every 5 min

_buckets: dict[str, dict] = defaultdict(lambda: {"tokens": RATE_LIMIT_BURST, "last": time.monotonic()})
_lock = Lock()
_last_cleanup = time.monotonic()


def _get_ip(request: Request) -> str:
    # Respect Cloudflare's CF-Connecting-IP header
    cf_ip = request.headers.get("CF-Connecting-IP")
    if cf_ip:
        return cf_ip
    xff = request.headers.get("X-Forwarded-For")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def check_rate_limit(request: Request) -> JSONResponse | None:
    """
    Call at the top of each paid endpoint.
    Returns None if allowed, JSONResponse(429) if rate-limited.
    """
    global _last_cleanup
    ip = _get_ip(request)
    now = time.monotonic()

    with _lock:
        # Periodic cleanup of idle buckets
        if now - _last_cleanup > CLEANUP_INTERVAL_SECS:
            idle_cutoff = now - 600  # 10 min idle
            stale = [k for k, v in _buckets.items() if v["last"] < idle_cutoff]
            for k in stale:
                del _buckets[k]
            _last_cleanup = now

        bucket = _buckets[ip]
        elapsed = now - bucket["last"]
        # Refill tokens based on elapsed time
        bucket["tokens"] = min(
            RATE_LIMIT_BURST,
            bucket["tokens"] + elapsed * RATE_LIMIT_RPS
        )
        bucket["last"] = now

        if bucket["tokens"] >= 1:
            bucket["tokens"] -= 1
            return None  # allowed

    # Rate limited
    retry_after = int((1 - bucket["tokens"]) / RATE_LIMIT_RPS) + 1
    return JSONResponse(
        status_code=429,
        content={"detail": "Rate limit exceeded. Please slow down."},
        headers={"Retry-After": str(retry_after)},
    )
