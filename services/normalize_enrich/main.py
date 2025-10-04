from __future__ import annotations
import uuid
from common.logging import get_logger
from common.queue import consume_forever, publish
from common.schemas import RawEvent, Fact

log = get_logger("normalize_enrich")
STREAM_IN = "events.raw"
STREAM_OUT = "facts.normalized"
GROUP = "normalize_group"
CONSUMER = "normalize_1"

def handle(payload: dict):
    # Validate input matches RawEvent/1 contract (will raise if not)
    _ = RawEvent.model_validate(payload)

    # Minimal normalization for Phase 0
    fact = Fact(
        event_id=str(uuid.uuid4()),
        companies=[],
        entities=[],
        metrics={},
        tags=["heartbeat"],
        confidence=0.99,
    ).model_dump()

    publish(STREAM_OUT, fact)
    log.info("emitted Fact event_id=%s", fact["event_id"])

def main():
    log.info("normalize_enrich starting...")
    consume_forever(STREAM_IN, GROUP, CONSUMER, handle)

if __name__ == "__main__":
    main()
