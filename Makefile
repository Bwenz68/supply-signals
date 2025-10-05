.PHONY: phase1-local clean-queues

phase1-local:
	@bash scripts/phase1_local.sh

clean-queues:
	@rm -f queue/raw_events/*.jsonl 2>/dev/null || true
	@rm -f queue/normalized_events/*.norm.jsonl 2>/dev/null || true
	@rm -f queue/signals/*.signals.jsonl 2>/dev/null || true
	@rm -f queue/alerts/alerts.csv 2>/dev/null || true
	@echo "Queues cleaned."

.PHONY: test-docker
test-docker:
	@docker run --rm \
	  --user "$(id -u):$(id -g)" \
	  -e HOME=/tmp \
	  -e PYTHONUSERBASE=/tmp/.local \
	  -e PATH="/tmp/.local/bin:$$PATH" \
	  -v "$$PWD":/app -w /app python:3.10 bash -lc '\
	    python -m pip install --user pytest >/dev/null && \
	    pytest -q \
	  '
