# alert_engine/sinks/smtp.py
from __future__ import annotations

import os
import random
import smtplib
import ssl
import time
from email.message import EmailMessage
from typing import Optional, Dict, Any, List

from .base import BaseSink, Alert


class SMTPSink(BaseSink):
    """
    SMTP sink.

    Step B.1: DRY-RUN only.
    Step B.3: Live email send when dry_run=False (SSL or STARTTLS), retries on transient errors.
    """
    name = "smtp"

    def __init__(
        self,
        *,
        host: Optional[str],
        port: Optional[int],
        user: Optional[str],
        password: Optional[str],
        from_addr: Optional[str],
        to_addr: Optional[str],
        subject_prefix: Optional[str] = None,
        timeout_secs: float = 10.0,
        use_ssl: Optional[bool] = None,
        use_starttls: Optional[bool] = None,
        dry_run: bool = True,
    ) -> None:
        super().__init__(dry_run=dry_run)
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.from_addr = from_addr
        self.to_addr = to_addr
        self.subject_prefix = subject_prefix or ""
        self.timeout_secs = timeout_secs
        self.use_ssl = use_ssl
        self.use_starttls = use_starttls

    @staticmethod
    def from_args_env(args) -> "SMTPSink":
        # Helper for env fallback
        def env_or_arg(arg_val, env_key, default=None):
            if arg_val not in (None, ""):
                return arg_val
            return os.getenv(env_key, default)

        host = env_or_arg(getattr(args, "smtp_host", None), "SMTP_HOST")
        port_s = env_or_arg(getattr(args, "smtp_port", None), "SMTP_PORT")
        port = int(port_s) if port_s else None
        user = env_or_arg(getattr(args, "smtp_user", None), "SMTP_USER")
        password = env_or_arg(getattr(args, "smtp_pass", None), "SMTP_PASS")
        from_addr = env_or_arg(getattr(args, "smtp_from", None), "SMTP_FROM")
        to_addr = env_or_arg(getattr(args, "smtp_to", None), "SMTP_TO")
        subject_prefix = env_or_arg(getattr(args, "smtp_subject_prefix", None), "SMTP_SUBJECT_PREFIX", "")
        timeout = float(env_or_arg(getattr(args, "smtp_timeout", None), "SMTP_TIMEOUT_SECS", "10"))
        use_ssl = env_or_arg(getattr(args, "smtp_use_ssl", None), "SMTP_USE_SSL")
        use_starttls = env_or_arg(getattr(args, "smtp_use_starttls", None), "SMTP_USE_STARTTLS")
        dry_run = not bool(getattr(args, "sinks_live", False))

        def to_bool(v):
            if v is None:
                return None
            s = str(v).strip().lower()
            return s in ("1", "true", "yes", "on")

        return SMTPSink(
            host=host,
            port=port,
            user=user,
            password=password,
            from_addr=from_addr,
            to_addr=to_addr,
            subject_prefix=subject_prefix,
            timeout_secs=timeout,
            use_ssl=to_bool(use_ssl),
            use_starttls=to_bool(use_starttls),
            dry_run=dry_run,
        )

    # --- Formatting helpers ---

    def _format_subject(self, alert: Alert) -> str:
        issuer = alert.get("issuer_name") or alert.get("company") or "Unknown issuer"
        kind = alert.get("event_kind") or alert.get("kind") or "event"
        title = alert.get("title") or ""
        prefix = (self.subject_prefix + " ").strip()
        return f"{prefix}{issuer} — {kind} — {title[:80]}"

    def _format_body(self, alert: Alert) -> str:
        parts = []
        def add(k):
            v = alert.get(k)
            if v is not None:
                parts.append(f"{k}: {v}")
        add("issuer_name")
        add("ticker")
        add("cik")
        add("event_kind")
        add("score")
        add("event_datetime_utc")
        add("title")
        add("first_url")
        rule_hits = alert.get("rule_hits")
        if rule_hits:
            parts.append("rule_hits: " + ", ".join(map(str, rule_hits)))
        return "\n".join(parts)

    # --- Utilities ---

    def _derive_port(self) -> int:
        if self.port:
            return int(self.port)
        # Reasonable defaults if not provided
        if self.use_ssl:
            return 465
        if self.use_starttls:
            return 587
        return 25

    @staticmethod
    def _parse_recipients(to_addr: Optional[str]) -> List[str]:
        if not to_addr:
            return []
        raw = to_addr.replace(";", ",")
        return [s.strip() for s in raw.split(",") if s.strip()]

    # --- Main ---

    def emit(self, alert: Alert) -> bool:
        self._on_attempt()

        if not self.to_addr or not self.from_addr:
            self._on_skip()
            print("[SMTP]" + ("[DRY-RUN]" if self.dry_run else "") + " SKIP (missing SMTP_FROM/SMTP_TO)")
            return False

        subject = self._format_subject(alert)
        body = self._format_body(alert)

        if self.dry_run:
            print(
                "[SMTP][DRY-RUN] Would send email\n"
                f"  host={self.host}:{self.port} ssl={self.use_ssl} starttls={self.use_starttls}\n"
                f"  from={self.from_addr}\n"
                f"  to={self.to_addr}\n"
                f"  subject={subject}\n"
                f"  --- body ---\n{body}\n"
            )
            self._on_sent()
            return True

        # LIVE mode
        if not self.host:
            # Preflight should block this, but keep a guard
            print("[SMTP] ERROR: SMTP_HOST is required in live mode")
            self._on_error()
            return False

        recipients = self._parse_recipients(self.to_addr)
        if not recipients:
            print("[SMTP] ERROR: no valid recipients parsed from SMTP_TO")
            self._on_error()
            return False

        port = self._derive_port()
        attempts = 3
        base = 0.5

        for i in range(1, attempts + 1):
            try:
                if self.use_ssl:
                    server = smtplib.SMTP_SSL(self.host, port, timeout=self.timeout_secs)
                else:
                    server = smtplib.SMTP(self.host, port, timeout=self.timeout_secs)
                try:
                    server.ehlo()
                    if (self.use_starttls is True) and not self.use_ssl:
                        context = ssl.create_default_context()
                        server.starttls(context=context)
                        server.ehlo()
                    if self.user and self.password:
                        server.login(self.user, self.password)

                    msg = EmailMessage()
                    msg["From"] = self.from_addr
                    msg["To"] = ", ".join(recipients)
                    msg["Subject"] = subject
                    msg.set_content(body)

                    server.send_message(msg)
                    try:
                        server.quit()
                    except Exception:
                        pass

                    self._on_sent()
                    return True
                finally:
                    try:
                        server.close()
                    except Exception:
                        pass
            except smtplib.SMTPAuthenticationError as e:
                print(f"[SMTP] AUTH ERROR: {e}")
                self._on_error()
                return False
            except (smtplib.SMTPException, OSError) as e:
                if i < attempts:
                    backoff = base * (2 ** (i - 1)) + random.uniform(0, 0.2)
                    print(f"[SMTP] WARN {e}; retry {i}/{attempts-1} in {backoff:.2f}s")
                    time.sleep(backoff)
                    continue
                print(f"[SMTP] ERROR {e}")
                self._on_error()
                return False
