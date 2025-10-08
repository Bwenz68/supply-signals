#!/usr/bin/env python3
"""
Reddit sentiment scraper - Extract ticker mentions from investing subreddits.

Scrapes WSB, stocks, investing subreddits for ticker mentions and basic sentiment.
"""
import os
import re
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Set, List, Dict, Any
from collections import Counter

import praw


# Common tickers to track (loaded from universe if available)
def load_universe_tickers(universe_path: Path) -> Set[str]:
    """Load tickers from universe.tsv"""
    tickers = set()
    if not universe_path.exists():
        return tickers
    
    import csv
    with universe_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            ticker = row.get("ticker", "").strip().upper()
            if ticker:
                tickers.add(ticker)
    
    return tickers


# Ticker extraction regex (1-5 uppercase letters, common stock pattern)
TICKER_PATTERN = re.compile(r'\b([A-Z]{1,5})\b')

# Exclude common words that look like tickers
EXCLUDE_WORDS = {
    'A', 'I', 'DD', 'CEO', 'CFO', 'IPO', 'ATH', 'ATL', 'YTD', 'EOD', 'AH', 'PM',
    'EPS', 'PE', 'EV', 'IV', 'US', 'USA', 'EU', 'UK', 'IMO', 'IMHO', 'TLDR',
    'YOLO', 'FD', 'WSB', 'TA', 'DD', 'FOMO', 'FUD', 'HOD', 'HODL', 'ETF', 'NYSE',
    'NASDAQ', 'SEC', 'FDA', 'GDP', 'CPI', 'API', 'FOR', 'THE', 'AND', 'ARE', 'NOT',
}


def extract_tickers(text: str, valid_tickers: Set[str] = None) -> List[str]:
    """
    Extract ticker mentions from text.
    If valid_tickers provided, only return those.
    """
    matches = TICKER_PATTERN.findall(text.upper())
    
    # Filter out excluded words
    tickers = [t for t in matches if t not in EXCLUDE_WORDS]
    
    # If we have a universe, filter to only those tickers
    if valid_tickers:
        tickers = [t for t in tickers if t in valid_tickers]
    
    return tickers


def simple_sentiment(text: str) -> str:
    """
    Very basic sentiment analysis based on keywords.
    Returns: bullish, bearish, or neutral
    """
    text_lower = text.lower()
    
    bullish_words = ['moon', 'calls', 'buy', 'long', 'pump', 'rocket', '🚀', 'bull', 
                     'green', 'tendies', 'gains', 'breakout', 'rally']
    bearish_words = ['puts', 'short', 'crash', 'dump', 'bear', 'red', 'drill',
                     'sell', 'overvalued', 'bubble', 'drop', 'fall']
    
    bullish_count = sum(1 for word in bullish_words if word in text_lower)
    bearish_count = sum(1 for word in bearish_words if word in text_lower)
    
    if bullish_count > bearish_count + 1:
        return "bullish"
    elif bearish_count > bullish_count + 1:
        return "bearish"
    else:
        return "neutral"


def scrape_subreddit(reddit, subreddit_name: str, limit: int, valid_tickers: Set[str]) -> List[Dict[str, Any]]:
    """
    Scrape a subreddit for ticker mentions.
    
    Returns list of mention events.
    """
    mentions = []
    
    try:
        subreddit = reddit.subreddit(subreddit_name)
        
        for submission in subreddit.hot(limit=limit):
            # Check title + selftext
            full_text = f"{submission.title} {submission.selftext}"
            
            tickers = extract_tickers(full_text, valid_tickers)
            if not tickers:
                continue
            
            sentiment = simple_sentiment(full_text)
            
            # Count mentions
            ticker_counts = Counter(tickers)
            
            # Create event for each unique ticker mentioned
            for ticker, count in ticker_counts.items():
                mentions.append({
                    "source": "reddit",
                    "event_kind": "social_mention",
                    "subreddit": subreddit_name,
                    "ticker": ticker,
                    "mention_count": count,
                    "sentiment": sentiment,
                    "post_title": submission.title,
                    "post_url": f"https://reddit.com{submission.permalink}",
                    "post_score": submission.score,
                    "post_comments": submission.num_comments,
                    "post_created_utc": datetime.fromtimestamp(submission.created_utc, tz=timezone.utc).isoformat(),
                    "event_datetime": datetime.now(timezone.utc).isoformat(),
                })
    
    except Exception as e:
        print(f"[REDDIT] Error scraping r/{subreddit_name}: {e}")
    
    return mentions


def aggregate_mentions(mentions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Aggregate mentions by ticker and calculate scores.
    
    Returns list of aggregated ticker signals.
    """
    # Group by ticker
    by_ticker = {}
    
    for mention in mentions:
        ticker = mention["ticker"]
        if ticker not in by_ticker:
            by_ticker[ticker] = {
                "ticker": ticker,
                "total_mentions": 0,
                "total_score": 0,
                "total_comments": 0,
                "bullish_count": 0,
                "bearish_count": 0,
                "neutral_count": 0,
                "subreddits": set(),
                "top_posts": [],
            }
        
        by_ticker[ticker]["total_mentions"] += mention["mention_count"]
        by_ticker[ticker]["total_score"] += mention["post_score"]
        by_ticker[ticker]["total_comments"] += mention["post_comments"]
        by_ticker[ticker]["subreddits"].add(mention["subreddit"])
        
        if mention["sentiment"] == "bullish":
            by_ticker[ticker]["bullish_count"] += 1
        elif mention["sentiment"] == "bearish":
            by_ticker[ticker]["bearish_count"] += 1
        else:
            by_ticker[ticker]["neutral_count"] += 1
        
        # Keep top 3 posts
        if len(by_ticker[ticker]["top_posts"]) < 3:
            by_ticker[ticker]["top_posts"].append({
                "title": mention["post_title"],
                "url": mention["post_url"],
                "score": mention["post_score"],
            })
    
    # Convert to signals
    signals = []
    for ticker, data in by_ticker.items():
        # Calculate sentiment score (-100 to +100)
        sentiment_score = (data["bullish_count"] - data["bearish_count"]) * 10
        
        # Calculate buzz score (volume-based)
        buzz_score = min(data["total_mentions"] * 2 + data["total_score"] // 10, 100)
        
        signal = {
            "source": "reddit",
            "event_kind": "social_sentiment",
            "ticker": ticker,
            "total_mentions": data["total_mentions"],
            "total_upvotes": data["total_score"],
            "total_comments": data["total_comments"],
            "subreddits": list(data["subreddits"]),
            "sentiment_bullish": data["bullish_count"],
            "sentiment_bearish": data["bearish_count"],
            "sentiment_neutral": data["neutral_count"],
            "sentiment_score": sentiment_score,
            "buzz_score": buzz_score,
            "top_posts": data["top_posts"],
            "event_datetime": datetime.now(timezone.utc).isoformat(),
        }
        
        signals.append(signal)
    
    return signals


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Reddit for ticker mentions and sentiment"
    )
    parser.add_argument(
        "--subreddits",
        default="wallstreetbets,stocks,investing",
        help="Comma-separated subreddits to scrape"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Number of posts to fetch per subreddit"
    )
    parser.add_argument(
        "--universe",
        type=Path,
        default=Path("ref/universe.tsv"),
        help="Path to universe.tsv (filter to these tickers)"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("queue/raw_events"),
        help="Output directory for mention events"
    )
    
    args = parser.parse_args()
    
    # Check credentials
    client_id = os.environ.get("REDDIT_CLIENT_ID")
    client_secret = os.environ.get("REDDIT_CLIENT_SECRET")
    user_agent = os.environ.get("REDDIT_USER_AGENT", "supply-signals/1.0")
    
    if not (client_id and client_secret):
        print("ERROR: REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET required")
        return 1
    
    # Load valid tickers from universe
    valid_tickers = load_universe_tickers(args.universe)
    if valid_tickers:
        print(f"[REDDIT] Loaded {len(valid_tickers)} tickers from universe")
    else:
        print("[REDDIT] No universe file found, will extract all tickers")
    
    # Connect to Reddit
    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent,
    )
    
    print(f"[REDDIT] Connected to Reddit API")
    
    # Scrape subreddits
    subreddit_list = [s.strip() for s in args.subreddits.split(",")]
    all_mentions = []
    
    for sub in subreddit_list:
        print(f"[REDDIT] Scraping r/{sub} (limit={args.limit})...")
        mentions = scrape_subreddit(reddit, sub, args.limit, valid_tickers)
        all_mentions.extend(mentions)
        print(f"[REDDIT]   Found {len(mentions)} ticker mentions")
    
    if not all_mentions:
        print("[REDDIT] No ticker mentions found")
        return 0
    
    # Aggregate by ticker
    signals = aggregate_mentions(all_mentions)
    
    # Write output
    args.output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_path = args.output_dir / f"reddit_sentiment_{timestamp}.jsonl"
    
    with output_path.open("w", encoding="utf-8") as f:
        for signal in signals:
            f.write(json.dumps(signal, ensure_ascii=False) + "\n")
    
    print(f"[REDDIT] Wrote {len(signals)} ticker signals to {output_path.name}")
    
    # Show top mentions
    signals.sort(key=lambda x: x["buzz_score"], reverse=True)
    print("\n📊 Top Mentions:")
    for sig in signals[:10]:
        sentiment = "🟢" if sig["sentiment_score"] > 20 else "🔴" if sig["sentiment_score"] < -20 else "⚪"
        print(f"  {sentiment} {sig['ticker']}: {sig['total_mentions']} mentions, buzz={sig['buzz_score']}, sentiment={sig['sentiment_score']}")
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
