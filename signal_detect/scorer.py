from typing import List, Optional

# Simple weights per tag; adjust as you learn.
TAG_WEIGHTS = {
    "buyback": 3,
    "dividend": 2,
    "guidance_up": 3,
    "guidance_down": 2,
    "cfo_resign": 3,
    "ceo_resign": 4,
}

def score_hits(hits: List[str], event_kind: Optional[str], subtype: Optional[str]) -> int:
    s = 0
    for h in hits:
        s += TAG_WEIGHTS.get(h, 1)

    # Mild priors: SEC and 8-K/6-K get a small bump.
    if (event_kind or "").upper() == "SEC":
        s += 1
    if (subtype or "").upper() in {"8-K", "6-K"}:
        s += 1
    return s
