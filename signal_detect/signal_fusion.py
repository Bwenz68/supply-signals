#!/usr/bin/env python3
"""
Multi-signal fusion - Combine SEC, insider, and Reddit signals for conviction scoring.

Fusion logic:
- Group signals by ticker + time window
- Weight by source reliability and signal strength
- Detect alignment vs conflict
- Output conviction scores (0-100)
"""
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any
from collections import defaultdict


def parse_datetime(dt_str: str) -> datetime:
    """Parse ISO datetime string to datetime object."""
    try:
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except Exception:
        return datetime.now(timezone.utc)


def score_signal(signal: Dict[str, Any]) -> Dict[str, float]:
    """
    Score a signal based on its type and attributes.
    
    Returns dict with:
        - base_score: 0-100
        - weight: importance multiplier (0.5-2.0)
        - sentiment: -1 (bearish) to +1 (bullish)
    """
    signal_type = signal.get("signal_type")
    source = signal.get("source")
    
    # Default neutral
    base_score = 50
    weight = 1.0
    sentiment = 0
    
    # Insider clustering signals
    if signal_type == "insider_cluster":
        cluster_sentiment = signal.get("sentiment", "MIXED")
        num_insiders = signal.get("num_insiders", 0)
        
        if cluster_sentiment == "STRONG_BULLISH":
            base_score = 85
            sentiment = 1.0
            weight = 2.0  # High weight for strong insider buying
        elif cluster_sentiment == "BULLISH":
            base_score = 70
            sentiment = 0.7
            weight = 1.5
        elif cluster_sentiment == "BEARISH":
            base_score = 30
            sentiment = -0.7
            weight = 1.5
        else:  # MIXED
            base_score = 50
            sentiment = 0
            weight = 1.0
        
        # Boost for more insiders
        weight *= (1 + min(num_insiders - 3, 5) * 0.1)
    
    # Reddit sentiment signals
    elif source == "reddit" and signal.get("event_kind") == "social_sentiment":
        buzz = signal.get("buzz_score", 0)
        sentiment_score = signal.get("sentiment_score", 0)
        
        # Base score from buzz
        base_score = min(40 + buzz // 2, 80)
        
        # Sentiment direction
        if sentiment_score > 20:
            sentiment = 0.6
        elif sentiment_score < -20:
            sentiment = -0.6
        else:
            sentiment = 0
        
        # Lower weight - Reddit is noisy
        weight = 0.8
        
        # Boost for very high buzz
        if buzz > 90:
            weight = 1.2
    
    # SEC filing signals (from existing signal_detect rules)
    elif source in ["sec_edgar", "SEC"]:
        # These come from rules_sec_pr.py scoring
        signal_score = signal.get("score", 0)
        
        if signal_score >= 8:
            base_score = 80
            sentiment = 0.8
            weight = 1.8
        elif signal_score >= 5:
            base_score = 65
            sentiment = 0.5
            weight = 1.3
        elif signal_score >= 3:
            base_score = 55
            sentiment = 0.3
            weight = 1.0
        else:
            base_score = 45
            sentiment = 0
            weight = 0.8
    
    return {
        "base_score": base_score,
        "weight": weight,
        "sentiment": sentiment,
    }


def fuse_signals(signals: List[Dict[str, Any]], window_hours: int = 48) -> List[Dict[str, Any]]:
    """
    Group signals by ticker and time window, then fuse into conviction scores.
    
    Returns list of fused signal events.
    """
    if not signals:
        return []
    
    # Group by ticker
    by_ticker = defaultdict(list)
    for sig in signals:
        ticker = sig.get("ticker") or sig.get("issuer_ticker")
        if ticker:
            by_ticker[ticker].append(sig)
    
    fused = []
    
    for ticker, ticker_signals in by_ticker.items():
        # Sort by time
        ticker_signals = sorted(
            ticker_signals,
            key=lambda s: parse_datetime(s.get("event_datetime") or s.get("event_datetime_utc") or s.get("cluster_start_date") or "")
        )
        
        # Sliding window fusion
        i = 0
        while i < len(ticker_signals):
            anchor = ticker_signals[i]
            anchor_time = parse_datetime(
                anchor.get("event_datetime") or 
                anchor.get("event_datetime_utc") or 
                anchor.get("cluster_start_date") or ""
            )
            window_end = anchor_time + timedelta(hours=window_hours)
            
            # Collect signals in window
            window_signals = []
            for sig in ticker_signals[i:]:
                sig_time = parse_datetime(
                    sig.get("event_datetime") or 
                    sig.get("event_datetime_utc") or 
                    sig.get("cluster_start_date") or ""
                )
                if sig_time <= window_end:
                    window_signals.append(sig)
                else:
                    break
            
            # Fuse if multiple signals or single high-value signal
            if len(window_signals) >= 1:
                fusion = fuse_window(ticker, window_signals, anchor_time, window_end)
                if fusion:
                    fused.append(fusion)
            
            # Move to next unique time
            i += len(window_signals)
            if i >= len(ticker_signals):
                break
    
    return fused


def fuse_window(ticker: str, signals: List[Dict[str, Any]], start_time: datetime, end_time: datetime) -> Dict[str, Any]:
    """
    Fuse all signals in a time window for a ticker.
    
    Returns fused signal with conviction score.
    """
    # Score each signal
    scored = []
    for sig in signals:
        scores = score_signal(sig)
        scored.append({
            "signal": sig,
            "base_score": scores["base_score"],
            "weight": scores["weight"],
            "sentiment": scores["sentiment"],
        })
    
    # Calculate weighted conviction
    total_weight = sum(s["weight"] for s in scored)
    weighted_score = sum(s["base_score"] * s["weight"] for s in scored) / total_weight if total_weight > 0 else 50
    
    # Calculate net sentiment
    net_sentiment = sum(s["sentiment"] * s["weight"] for s in scored) / total_weight if total_weight > 0 else 0
    
    # Detect alignment vs conflict
    sentiments = [s["sentiment"] for s in scored]
    alignment = "aligned" if all(s >= 0 for s in sentiments) or all(s <= 0 for s in sentiments) else "conflicted"
    
    # Boost for alignment
    if alignment == "aligned" and len(signals) > 1:
        weighted_score = min(weighted_score * 1.2, 100)
    
    # Penalize for conflict
    if alignment == "conflicted":
        weighted_score *= 0.85
    
    # Determine conviction level
    if weighted_score >= 80:
        conviction = "HIGH"
    elif weighted_score >= 65:
        conviction = "MEDIUM"
    elif weighted_score >= 50:
        conviction = "LOW"
    else:
        conviction = "NEUTRAL"
    
    # Build fused signal
    fusion = {
        "signal_type": "fused_conviction",
        "ticker": ticker,
        "conviction_score": round(weighted_score, 1),
        "conviction_level": conviction,
        "net_sentiment": round(net_sentiment, 2),
        "alignment": alignment,
        "num_signals": len(signals),
        "window_start": start_time.isoformat(),
        "window_end": end_time.isoformat(),
        "event_datetime": datetime.now(timezone.utc).isoformat(),
        "component_signals": [
            {
                "source": sig["signal"].get("source"),
                "signal_type": sig["signal"].get("signal_type") or sig["signal"].get("event_kind"),
                "base_score": sig["base_score"],
                "weight": sig["weight"],
                "sentiment": sig["sentiment"],
            }
            for sig in scored
        ],
    }
    
    return fusion


def main():
    parser = argparse.ArgumentParser(
        description="Fuse multi-source signals into conviction scores"
    )
    parser.add_argument(
        "--signals-dir",
        type=Path,
        default=Path("queue/signals"),
        help="Directory containing signal files"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("queue/fused_signals"),
        help="Output directory for fused signals"
    )
    parser.add_argument(
        "--window-hours",
        type=int,
        default=48,
        help="Time window for signal fusion (hours)"
    )
    
    args = parser.parse_args()
    
    # Load all signals
    all_signals = []
    
    signal_files = list(args.signals_dir.glob("*.signals.jsonl")) + list(args.signals_dir.glob("insider_clusters_*.jsonl"))
    
    if not signal_files:
        print("[FUSION] No signal files found")
        return 0
    
    print(f"[FUSION] Loading signals from {len(signal_files)} file(s)...")
    
    for filepath in signal_files:
        try:
            with filepath.open("r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        all_signals.append(json.loads(line))
        except Exception as e:
            print(f"[FUSION] Error reading {filepath.name}: {e}")
    
    if not all_signals:
        print("[FUSION] No signals loaded")
        return 0
    
    print(f"[FUSION] Loaded {len(all_signals)} signals")
    print(f"[FUSION] Fusing signals (window={args.window_hours}h)...")
    
    # Fuse signals
    fused = fuse_signals(all_signals, args.window_hours)
    
    if not fused:
        print("[FUSION] No fused signals generated")
        return 0
    
    # Write output
    args.output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_path = args.output_dir / f"fused_{timestamp}.jsonl"
    
    with output_path.open("w", encoding="utf-8") as f:
        for fusion in fused:
            f.write(json.dumps(fusion, ensure_ascii=False) + "\n")
    
    print(f"[FUSION] Wrote {len(fused)} fused signals to {output_path.name}")
    
    # Display summary
    print("\n📊 Fusion Summary:")
    fused.sort(key=lambda x: x["conviction_score"], reverse=True)
    
    for fs in fused[:10]:
        sentiment_emoji = "🟢" if fs["net_sentiment"] > 0.3 else "🔴" if fs["net_sentiment"] < -0.3 else "⚪"
        align_emoji = "✓" if fs["alignment"] == "aligned" else "⚠"
        
        print(f"  {sentiment_emoji} {align_emoji} {fs['ticker']}: "
              f"{fs['conviction_level']} ({fs['conviction_score']:.1f}) "
              f"- {fs['num_signals']} signals, sentiment={fs['net_sentiment']:.2f}")
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
