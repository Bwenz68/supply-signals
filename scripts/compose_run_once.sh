#!/usr/bin/env bash
# scripts/compose_run_once.sh
# Run the full file-queue pipeline using Docker Compose (offline fixtures)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Note: UID/GID are already set by bash, no need to export
echo "================================================"
echo "SUPPLY-SIGNALS Docker Pipeline (File-Queue)"
echo "================================================"
date
echo

# Optional: clean alerts CSV to avoid duplicates
rm -f queue/alerts/alerts.csv 2>/dev/null || true

echo "-- Ingest: PR (single fixture)"
docker compose run --rm \
  -e PR_FEED_URL="file:///app/tests/fixtures/pr_sample.xml" \
  -e PR_ISSUER_NAME="Contoso Energy" \
  ingest_pr_single

echo
echo "-- Ingest: PR (multi fixture)"
docker compose run --rm \
  -e PR_FEEDS_FILE="tests/fixtures/pr_multi_feeds.txt" \
  ingest_pr_multi || true

echo
echo "-- Ingest: SEC (fixture)"
docker compose run --rm \
  -e SEC_URL="file:///app/tests/fixtures/sec_atom_sample.xml" \
  -e SEC_CIK="9876543" \
  -e SEC_ISSUER_NAME="Contoso Energy" \
  ingest_sec

echo
echo "-- Normalize & Enrich"
docker compose run --rm normalize_enrich

echo
echo "-- Signal Detection (threshold=3)"
docker compose run --rm \
  -e SIGNAL_THRESHOLD="3" \
  signal_detect

echo
echo "-- Alerts: Console"
docker compose run --rm alert_console

echo
echo "-- Alerts: CSV"
docker compose run --rm alert_csv

echo
echo "================================================"
echo "Pipeline complete!"
echo "================================================"
echo "Check outputs:"
echo "  - Signals: queue/signals/*.signals.jsonl"
echo "  - Alerts CSV: queue/alerts/alerts.csv"
echo
