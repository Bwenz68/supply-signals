# alert_engine/sinks/slack.py
from __future__ import annotations

import json
import os
import random
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional

from .base import BaseSink, Alert


class SlackSink(BaseSink):
    """
    Slack sink.

    Step B.1: DRY-RUN only.
    Step B.3: Live POST when dry_run=False, with retries and optional rate limiting.
    """
    name = "slack"

    def __init__(
        self,
        *,
        webhook_url: Optional[str],
        mention: Optional[str] = None,
        timeout_secs: float = 5.0,
        rate_per_sec: float = 0.0,
        dry_run: bool = True,
    ) -> None:
        super().__init__(dry_run=dry_run)
        self.webhook_url = webhook_url
        self.mention = mention
        self.timeout_secs = timeout_secs
        self.rate_per_sec = rate_per_sec

        # Rate limiter state
        self._min_interval = 1.0 / rate_per_sec if rate_per_sec and rate_per_sec > 0 else 0.0
        self._next_post_time = 0.0

    @staticmethod
    def from_args_env(args) -> "SlackSink":
        # Accept args first, then env fallbacks.
        webhook = getattr(args, "slack_webhook", None) or os.getenv("SLACK_WEBHOOK_URL")
        timeout = float((getattr(args, "slack_timeout", None) or os.getenv("SLACK_TIMEOUT_SECS") or "5"))
        rate = float((getattr(args, "slack_rate_per_sec", None) or os.getenv("SLACK_RATE_PER_SEC") or "0"))
        mention = getattr(args, "slack_mention", None) or os.getenv("SLACK_MENTION")
        # Default to DRY-RUN unless --sinks-live was provided on the CLI owning 'args'
        dry_run = not bool(getattr(args, "sinks_live", False))
        return SlackSink(
            webhook_url=webhook,
            mention=mention,
            timeout_secs=timeout,
            rate_per_sec=rate,
            dry_run=dry_run,
        )

    # --- Formatting helpers ---

    def _format_preview(self, alert: Alert) -> str:
        issuer = alert.get("issuer_name") or alert.get("company") or "Unknown issuer"
        title = alert.get("title") or "(no title)"
        url = alert.get("first_url") or alert.get("url") or "(no url)"
        kind = alert.get("event_kind") or alert.get("kind") or "event"
        ts = alert.get("event_datetime_utc") or alert.get("ts") or "unknown time"
        score = alert.get("score")
        score_s = f" score={score}" if score is not None else ""
        mention_s = f" mention={self.mention}" if self.mention else ""
        return f"{issuer} — {kind}{score_s}\n  {title}\n  {url}\n  {ts}{mention_s}"

    def _build_payload(self, alert: Alert) -> Dict[str, Any]:
        issuer = alert.get("issuer_name") or alert.get("company") or "Unknown issuer"
        kind = alert.get("event_kind") or alert.get("kind") or "event"
        title = alert.get("title") or "(no title)"
        url = alert.get("first_url") or alert.get("url") or ""
        ts = alert.get("event_datetime_utc") or alert.get("ts") or ""
        score = alert.get("score")
        score_s = f" (score {score})" if score is not None else ""
        mention = f"\n{self.mention}" if self.mention else ""

        # Simple, broadly compatible blocks + fallback text
        text = f"{issuer} — {kind}{score_s}\n*{title}*\n{url}\n{ts}{mention}"
        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": f"{issuer} — {kind}"}},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{title}*\n<{url}|open link>\n{ts}{mention}",
                },
            },
        ]
        rh = alert.get("rule_hits")
        if isinstance(rh, list) and rh:
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": "rule_hits: " + ", ".join(map(str, rh))}],
            })

        return {"text": text, "blocks": blocks}

    # --- HTTP utilities ---

    def _rate_sleep_if_needed(self) -> None:
        if self._min_interval <= 0:
            return
        now = time.monotonic()
        if now < self._next_post_time:
            time.sleep(self._next_post_time - now)
        self._next_post_time = max(now, self._next_post_time) + self._min_interval

    def _post_json(self, url: str, payload: Dict[str, Any]) -> tuple[int, str]:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=self.timeout_secs) as resp:
            body = resp.read().decode("utf-8", "ignore")
            return resp.getcode() or 0, body

    # --- Main ---

    def emit(self, alert: Alert) -> bool:
        self._on_attempt()

        if not self.webhook_url:
            # Config missing -> treat as skip (preflight in live mode should catch this)
            self._on_skip()
            print("[Slack]" + ("[DRY-RUN]" if self.dry_run else "") + " SKIP (no webhook configured)")
            return False

        if self.dry_run:
            preview = self._format_preview(alert)
            print(f"[Slack][DRY-RUN] Would POST to {self.webhook_url}:\n{preview}\n")
            self._on_sent()
            return True

        # LIVE mode
        payload = self._build_payload(alert)
        attempts = 3
        base = 0.5

        for i in range(1, attempts + 1):
            try:
                self._rate_sleep_if_needed()
                status, body = self._post_json(self.webhook_url, payload)
                if 200 <= status < 300:
                    # Slack webhooks typically return "ok" in the body
                    self._on_sent()
                    return True
                else:
                    # Retry on 5xx; do not retry on other 4xx
                    if 500 <= status < 600:
                        raise RuntimeError(f"HTTP {status}: {body}")
                    else:
                        print(f"[Slack] ERROR non-retriable HTTP {status}: {body}")
                        self._on_error()
                        return False
            except urllib.error.HTTPError as e:
                if e.code >= 500 and i < attempts:
                    backoff = base * (2 ** (i - 1)) + random.uniform(0, 0.2)
                    print(f"[Slack] WARN HTTP {e.code}; retry {i}/{attempts-1} in {backoff:.2f}s")
                    time.sleep(backoff)
                    continue
                print(f"[Slack] ERROR HTTP {e.code}: {getattr(e, 'reason', '')}")
                self._on_error()
                return False
            except (urllib.error.URLError, RuntimeError, Exception) as e:
                # Treat as transient unless last attempt
                if i < attempts:
                    backoff = base * (2 ** (i - 1)) + random.uniform(0, 0.2)
                    print(f"[Slack] WARN {e}; retry {i}/{attempts-1} in {backoff:.2f}s")
                    time.sleep(backoff)
                    continue
                print(f"[Slack] ERROR {e}")
                self._on_error()
                return False
