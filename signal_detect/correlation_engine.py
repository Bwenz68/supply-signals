#!/usr/bin/env python3
"""
Cross-correlation engine - Detect price relationships between stocks.

Analyzes:
- Correlation coefficients (which stocks move together)
- Recent divergences (when correlated pairs break pattern)
- Leading indicators (which stocks predict others)
"""
import json
import argparse
import csv
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Set
import warnings

import yfinance as yf
import pandas as pd
import numpy as np

warnings.filterwarnings('ignore')


def load_universe_tickers(universe_path: Path) -> List[Dict[str, str]]:
    """Load tickers from universe.tsv with metadata."""
    tickers = []
    
    if not universe_path.exists():
        return tickers
    
    with universe_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            ticker = row.get("ticker", "").strip().upper()
            if ticker:
                tickers.append({
                    "ticker": ticker,
                    "name": row.get("name", ""),
                    "sector": row.get("sector", ""),
                    "industry": row.get("industry", ""),
                })
    
    return tickers


def fetch_price_data(tickers: List[str], days: int = 90) -> pd.DataFrame:
    """
    Fetch historical price data for multiple tickers.
    
    Returns DataFrame with tickers as columns, dates as index.
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    print(f"[CORRELATION] Fetching {days} days of price data for {len(tickers)} tickers...")
    
    price_data = {}
    
    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(start=start_date, end=end_date)
            
            if len(hist) > 0:
                # Use close price, forward-fill missing data
                price_data[ticker] = hist['Close']
                print(f"[CORRELATION]   {ticker}: {len(hist)} days")
            else:
                print(f"[CORRELATION]   {ticker}: No data")
        except Exception as e:
            print(f"[CORRELATION]   {ticker}: Error - {e}")
    
    if not price_data:
        return pd.DataFrame()
    
    # Combine into single DataFrame
    df = pd.DataFrame(price_data)
    
    # Forward-fill missing values (for holidays/weekends)
    df = df.fillna(method='ffill')
    
    return df


def calculate_returns(prices: pd.DataFrame) -> pd.DataFrame:
    """Calculate daily percentage returns."""
    return prices.pct_change().dropna()


def calculate_correlation_matrix(returns: pd.DataFrame) -> pd.DataFrame:
    """Calculate Pearson correlation matrix."""
    return returns.corr()


def find_strong_correlations(corr_matrix: pd.DataFrame, threshold: float = 0.7) -> List[Dict]:
    """
    Find ticker pairs with strong correlation (positive or negative).
    
    Returns list of correlation pairs.
    """
    correlations = []
    
    tickers = corr_matrix.columns.tolist()
    
    for i, ticker1 in enumerate(tickers):
        for ticker2 in tickers[i+1:]:  # Avoid duplicates
            corr = corr_matrix.loc[ticker1, ticker2]
            
            # Strong positive or negative correlation
            if abs(corr) >= threshold:
                correlations.append({
                    "ticker1": ticker1,
                    "ticker2": ticker2,
                    "correlation": round(corr, 3),
                    "type": "positive" if corr > 0 else "negative",
                    "strength": "very_strong" if abs(corr) >= 0.85 else "strong",
                })
    
    return correlations


def detect_divergences(prices: pd.DataFrame, correlations: List[Dict], 
                       lookback_days: int = 10, divergence_threshold: float = 0.05) -> List[Dict]:
    """
    Detect recent divergences in correlated pairs.
    
    When two highly correlated stocks diverge, it may signal:
    - Mean reversion opportunity
    - Leading indicator (one leads, other follows)
    - Fundamental change
    """
    divergences = []
    
    # Calculate recent returns
    recent_returns = prices.pct_change(lookback_days).iloc[-1]
    
    for pair in correlations:
        ticker1 = pair["ticker1"]
        ticker2 = pair["ticker2"]
        
        if ticker1 not in recent_returns or ticker2 not in recent_returns:
            continue
        
        ret1 = recent_returns[ticker1]
        ret2 = recent_returns[ticker2]
        
        # Skip if data is missing
        if pd.isna(ret1) or pd.isna(ret2):
            continue
        
        # Calculate divergence
        if pair["type"] == "positive":
            # For positive correlation, expect similar returns
            divergence = abs(ret1 - ret2)
        else:
            # For negative correlation, expect opposite returns
            divergence = abs(ret1 + ret2)
        
        # Significant divergence?
        if divergence >= divergence_threshold:
            divergences.append({
                "ticker1": ticker1,
                "ticker2": ticker2,
                "correlation": pair["correlation"],
                "ticker1_return": round(ret1 * 100, 2),
                "ticker2_return": round(ret2 * 100, 2),
                "divergence_magnitude": round(divergence * 100, 2),
                "signal": "mean_reversion_opportunity",
                "lookback_days": lookback_days,
            })
    
    return divergences


def calculate_lead_lag(prices: pd.DataFrame, ticker1: str, ticker2: str, max_lag: int = 5) -> Dict:
    """
    Determine if one ticker leads another (useful for predictive signals).
    
    Returns dict with lead/lag relationship.
    """
    if ticker1 not in prices.columns or ticker2 not in prices.columns:
        return {}
    
    returns1 = prices[ticker1].pct_change().dropna()
    returns2 = prices[ticker2].pct_change().dropna()
    
    # Calculate correlation at different lags
    correlations = []
    for lag in range(-max_lag, max_lag + 1):
        if lag == 0:
            corr = returns1.corr(returns2)
        elif lag > 0:
            # ticker1 leads ticker2 by 'lag' days
            corr = returns1.shift(lag).corr(returns2)
        else:
            # ticker2 leads ticker1 by 'lag' days
            corr = returns1.corr(returns2.shift(-lag))
        
        correlations.append((lag, corr))
    
    # Find strongest correlation
    best_lag, best_corr = max(correlations, key=lambda x: abs(x[1]) if not pd.isna(x[1]) else 0)
    
    if pd.isna(best_corr):
        return {}
    
    if best_lag > 0:
        leader = ticker1
        follower = ticker2
        lag_days = best_lag
    elif best_lag < 0:
        leader = ticker2
        follower = ticker1
        lag_days = abs(best_lag)
    else:
        return {"concurrent": True, "correlation": round(best_corr, 3)}
    
    return {
        "leader": leader,
        "follower": follower,
        "lag_days": lag_days,
        "correlation": round(best_corr, 3),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Calculate cross-correlations and detect divergences"
    )
    parser.add_argument(
        "--universe",
        type=Path,
        default=Path("ref/universe.tsv"),
        help="Path to universe.tsv"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Number of days of price history"
    )
    parser.add_argument(
        "--correlation-threshold",
        type=float,
        default=0.7,
        help="Minimum correlation to report"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("queue/signals"),
        help="Output directory for correlation signals"
    )
    
    args = parser.parse_args()
    
    # Load universe
    universe = load_universe_tickers(args.universe)
    if not universe:
        print("[CORRELATION] No tickers found in universe")
        return 1
    
    tickers = [u["ticker"] for u in universe]
    
    # Fetch price data
    prices = fetch_price_data(tickers, args.days)
    
    if prices.empty:
        print("[CORRELATION] No price data fetched")
        return 1
    
    print(f"[CORRELATION] Got price data for {len(prices.columns)} tickers")
    
    # Calculate returns and correlations
    returns = calculate_returns(prices)
    corr_matrix = calculate_correlation_matrix(returns)
    
    # Find strong correlations
    print(f"[CORRELATION] Calculating correlations (threshold={args.correlation_threshold})...")
    strong_corrs = find_strong_correlations(corr_matrix, args.correlation_threshold)
    
    print(f"[CORRELATION] Found {len(strong_corrs)} strong correlation pairs")
    
    # Detect divergences
    print("[CORRELATION] Detecting divergences...")
    divergences = detect_divergences(prices, strong_corrs, lookback_days=10)
    
    print(f"[CORRELATION] Found {len(divergences)} divergence signals")
    
    # Write signals
    args.output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    
    # Write correlations
    corr_path = args.output_dir / f"correlations_{timestamp}.jsonl"
    with corr_path.open("w", encoding="utf-8") as f:
        for corr in strong_corrs:
            signal = {
                "source": "correlation_engine",
                "signal_type": "correlation_pair",
                "event_datetime": datetime.now().isoformat(),
                **corr
            }
            f.write(json.dumps(signal, ensure_ascii=False) + "\n")
    
    print(f"[CORRELATION] Wrote {len(strong_corrs)} correlations to {corr_path.name}")
    
    # Write divergences
    if divergences:
        div_path = args.output_dir / f"divergences_{timestamp}.jsonl"
        with div_path.open("w", encoding="utf-8") as f:
            for div in divergences:
                signal = {
                    "source": "correlation_engine",
                    "signal_type": "divergence_alert",
                    "event_datetime": datetime.now().isoformat(),
                    **div
                }
                f.write(json.dumps(signal, ensure_ascii=False) + "\n")
        
        print(f"[CORRELATION] Wrote {len(divergences)} divergences to {div_path.name}")
    
    # Display summary
    print("\n📊 Correlation Summary:")
    print(f"  Strong correlations: {len(strong_corrs)}")
    
    for corr in strong_corrs[:10]:
        emoji = "📈" if corr["type"] == "positive" else "📉"
        print(f"  {emoji} {corr['ticker1']} ↔ {corr['ticker2']}: {corr['correlation']:.3f} ({corr['strength']})")
    
    if divergences:
        print("\n⚠️  Recent Divergences:")
        for div in divergences[:5]:
            print(f"  {div['ticker1']} ({div['ticker1_return']:+.2f}%) vs {div['ticker2']} ({div['ticker2_return']:+.2f}%)")
            print(f"    Divergence: {div['divergence_magnitude']:.2f}% over {div['lookback_days']} days")
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
