#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "== SUPPLY-SIGNALS Phase-1 (local) =="
date

# Optional: clean CSV to avoid dupes each run
rm -f queue/alerts/alerts.csv || true

echo "-- Ingest: SEC"
python3 -m data_ingest sec

echo "-- Ingest: PR"
python3 -m data_ingest pr

echo "-- Normalize"
python3 -m normalize_enrich --once

echo "-- Signals (threshold=3)"
python3 -m signal_detect --threshold 3

echo "-- Alerts (console)"
python3 -m alert_engine

echo "-- Alerts (csv)"
python3 -m alert_engine --csv

echo "== DONE =="
