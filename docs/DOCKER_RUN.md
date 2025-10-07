# Docker Pipeline - File-Queue Mode

## Overview

One-shot file-queue pipeline services. Each service runs once and exits.

**Services:**
- `ingest_pr_single` - Single PR feed
- `ingest_pr_multi` - Multiple PR feeds from file
- `ingest_sec` - SEC EDGAR Atom feed
- `normalize_enrich` - Normalize raw events
- `signal_detect` - Detect signals
- `alert_console` - Print alerts to console
- `alert_csv` - Write alerts to CSV
- `alert_slack` - Send to Slack (DRY-RUN by default)
- `alert_smtp` - Send via SMTP (DRY-RUN by default)

## Prerequisites
```bash
# Set user/group IDs for file permissions
export UID=$(id -u)
export GID=$(id -g)

# Copy environment template (first time only)
cp .env.sample .env
nano .env  # Edit with your settings
