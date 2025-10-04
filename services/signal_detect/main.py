from __future__ import annotations
from common.logging import get_logger
from common.queue import consume_forever, publish
from common.schemas import Fact, Signal

log = get_logger("signal_detect")
STREAM_IN = "facts.normalized"
STREAM_OUT = "signals.detected"
GROUP = "signal_group"
CONSUMER = "signal_1"

def score(payload: dict):
    fact = Fact.model_validate(payload)

    # Phase 0 placeholder scoring
    comps = {"A": 0.10, "B": 0.10, "C": 0.05, "D": 0.05}
    total = sum(comps.values())
    tier = "T2" if total >= 0.25 else "T3"

    sig = Signal(
        ticker=None,
        score_components=comps,
        score_total=total,
        tier=tier,
        explanation="heartbeat flow through pipeline",
        provenance_event_ids=[fact.event_id],
        links=[]
    ).model_dump()

    publish(STREAM_OUT, sig)
    log.info("emitted Signal score=%.2f tier=%s", total, tier)

def main():
    log.info("signal_detect starting...")
    consume_forever(STREAM_IN, GROUP, CONSUMER, score)

if __name__ == "__main__":
    main()
