# alert_engine/sinks/__init__.py
from .base import Alert, AlertSink, BaseSink, SinkMetrics  # re-export
from .slack import SlackSink
from .smtp import SMTPSink

__all__ = [
    "Alert",
    "AlertSink",
    "BaseSink",
    "SinkMetrics",
    "SlackSink",
    "SMTPSink",
]
