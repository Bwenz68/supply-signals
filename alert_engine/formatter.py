def one_line(sig: dict) -> str:
    """
    Render a single alert line from a signal.
    Safe for missing fields; keeps Phase-0 compatibility.
    """
    e = sig.get("event", {}) or {}
    tkr   = e.get("canonical_ticker") or "?"
    comp  = e.get("canonical_company") or "?"
    kind  = e.get("event_kind") or "?"
    sub   = e.get("event_subtype") or "?"
    score = sig.get("score", "?")
    title = (e.get("title") or "").strip()

    # Keep it compact and human-readable
    return f"[{tkr}] {comp} | {kind}/{sub} | score={score} | {title}"
