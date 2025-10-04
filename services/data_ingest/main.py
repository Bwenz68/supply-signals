from __future__ import annotations
import time
from datetime import datetime, timezone
from common.logging import get_logger
from common.queue import publish
from common.schemas import RawEvent

log = get_logger("data_ingest")
STREAM_OUT = "events.raw"

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def main():
    log.info("data_ingest starting (Phase 0 heartbeat)...")
    while True:
        evt = RawEvent(
            source="heartbeat",
            ts_utc=now_iso(),
            headline="pipeline heartbeat",
            body_text="test RawEvent flowing through the system"
        ).model_dump()
        msg_id = publish(STREAM_OUT, evt)
        log.info(f"published RawEvent id={msg_id}")
        time.sleep(5)

if __name__ == "__main__":
    main()
