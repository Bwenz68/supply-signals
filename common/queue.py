from __future__ import annotations
import json, os, time
from typing import Callable
import redis

def _client():
    url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    return redis.Redis.from_url(url, decode_responses=True)

def publish(stream: str, payload: dict) -> str:
    r = _client()
    return r.xadd(stream, {"data": json.dumps(payload)})

def consume_forever(stream: str, group: str, consumer: str,
                    handler: Callable[[dict], None], block_ms: int = 5000):
    r = _client()
    # create group if missing
    try:
        r.xgroup_create(stream, group, id="$", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
    while True:
        resp = r.xreadgroup(group, consumer, {stream: ">"}, count=10, block=block_ms)
        if not resp:
            continue
        for _s, msgs in resp:
            for msg_id, fields in msgs:
                raw = fields.get("data")
                try:
                    payload = json.loads(raw) if raw else {}
                    handler(payload)
                except Exception as e:
                    print(f"[{stream}] handler error: {e}")
                finally:
                    r.xack(stream, group, msg_id)
        time.sleep(0.05)
