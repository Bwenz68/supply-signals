SHELL := /bin/bash
.DEFAULT_GOAL := help

help:
	@echo "Targets:"
	@echo "  up         - start core services"
	@echo "  down       - stop all"
	@echo "  build      - build service images"
	@echo "  logs S=svc - tail logs for service"
	@echo "  test       - (reserved for Phase 1+)"
	@echo "  snapshot   - save redis/db snapshots to .snapshots/"
	@echo "  tag        - create a git tag TAG=v0.1.0-phase0"

up:
	docker compose up -d redis
	docker compose build
	docker compose up -d data_ingest normalize_enrich signal_detect alert_engine

down:
	docker compose down

build:
	docker compose build

logs:
	@if [ -z "$(S)" ]; then echo "Usage: make logs S=alert_engine"; exit 1; fi
	docker compose logs -f $(S)

snapshot:
	mkdir -p .snapshots && date +"%Y%m%d_%H%M%S" > .snapshots/ts.txt
	@echo "Snapshot timestamp: " && cat .snapshots/ts.txt

tag:
	@if [ -z "$(TAG)" ]; then echo "Usage: make tag TAG=v0.1.0-phase0"; exit 1; fi
	git add .
	git commit -m "Tag $(TAG)"
	git tag -a $(TAG) -m "Stable baseline $(TAG)"
