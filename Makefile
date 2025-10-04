.PHONY: phase1-local clean-queues

phase1-local:
	@bash scripts/phase1_local.sh

clean-queues:
	@rm -f queue/raw_events/*.jsonl 2>/dev/null || true
	@rm -f queue/normalized_events/*.norm.jsonl 2>/dev/null || true
	@rm -f queue/signals/*.signals.jsonl 2>/dev/null || true
	@rm -f queue/alerts/alerts.csv 2>/dev/null || true
	@echo "Queues cleaned."
