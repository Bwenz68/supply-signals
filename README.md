# SUPPLY-SIGNALS (Hidden Alpha Machine)

Local, modular, explainable system for detecting hidden investment signals across direct filings, ecosystem clues, market behavior, and cross-sector links. Designed for Linux + RTX 3060 eGPU.

## Quick start (Phase 0)
```bash
docker compose up -d redis
docker compose build
docker compose up -d data_ingest normalize_enrich signal_detect alert_engine
docker compose logs -f alert_engine
