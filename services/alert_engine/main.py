from __future__ import annotations
from common.logging import get_logger
from common.queue import consume_forever
from common.schemas import Signal

log = get_logger("alert_engine")
STREAM_IN = "signals.detected"
GROUP = "alert_group"
CONSUMER = "alert_1"

def handle(payload: dict):
    sig = Signal.model_validate(payload)
    log.info(
        "ALERT ▷ tier=%s score=%.2f reason=%s provenance=%s",
        sig.tier, sig.score_total, sig.explanation, ",".join(sig.provenance_event_ids)
    )

def main():
    log.info("alert_engine starting...")
    consume_forever(STREAM_IN, GROUP, CONSUMER, handle)

if __name__ == "__main__":
    main()
