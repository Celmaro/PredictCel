"""Alerting module for PredictCel cycle failures.

Sends notifications to Slack/Discord/webhooks when cycle execution fails.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)


@dataclass
class AlertPayload:
    """Structured alert payload for webhook notifications."""

    severity: str
    title: str
    message: str
    cycle_id: str | None
    timestamp: str
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}


class SlackFormatter:
    """Formats alerts as Slack Block Kit messages."""

    SEVERITY_COLORS = {"critical": "#FF0000", "warning": "#FFA500", "info": "#36A64F"}

    @classmethod
    def format(cls, payload: AlertPayload) -> dict[str, Any]:
        color = cls.SEVERITY_COLORS.get(payload.severity, "#808080")
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"🔔 {payload.title}", "emoji": True},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": payload.message},
            },
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f"*Severity:* {payload.severity.upper()} | *Time:* {payload.timestamp}"}
                ],
            },
        ]
        if payload.metadata:
            fields = [
                {"type": "mrkdwn", "text": f"*{k}*\n{v}"}
                for k, v in list(payload.metadata.items())[:5]
            ]
            blocks.append({"type": "section", "fields": fields})
        if payload.cycle_id:
            blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": f"*Cycle ID:* `{payload.cycle_id}`"}]})
        return {"attachments": [{"color": color, "blocks": blocks}]}


class AlertManager:
    """Centralized alerting manager for PredictCel."""

    def __init__(self):
        self.slack_url = os.environ.get("SLACK_WEBHOOK_URL") or os.environ.get("SLACK_WEBHOOK")
        self.discord_url = os.environ.get("DISCORD_WEBHOOK_URL") or os.environ.get("DISCORD_WEBHOOK")
        self.generic_url = os.environ.get("ALERT_WEBHOOK_URL")
        self._setup_logging()

    def _setup_logging(self):
        if self.slack_url:
            logger.info("Slack alerting enabled")
        if self.discord_url:
            logger.info("Discord alerting enabled")
        if self.generic_url:
            logger.info("Generic webhook alerting enabled")

    @property
    def is_enabled(self) -> bool:
        return bool(self.slack_url or self.discord_url or self.generic_url)

    def _send_webhook(self, url: str, payload: dict[str, Any]) -> bool:
        try:
            body = json.dumps(payload).encode("utf-8")
            request = Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
            with urlopen(request, timeout=10) as resp:
                return 200 <= resp.status < 300
        except Exception as e:
            logger.error(f"Webhook send failed: {e}")
            return False

    def send(
        self,
        severity: str,
        title: str,
        message: str,
        cycle_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        """Send alert to all configured channels."""
        if not self.is_enabled:
            return False

        payload = AlertPayload(
            severity=severity,
            title=title,
            message=message,
            cycle_id=cycle_id,
            timestamp=datetime.now(UTC).isoformat(),
            metadata=metadata,
        )

        sent = False
        if self.slack_url:
            slack_data = SlackFormatter.format(payload)
            if self._send_webhook(self.slack_url, slack_data):
                logger.info(f"Slack alert sent: {title}")
                sent = True

        if self.generic_url:
            if self._send_webhook(self.generic_url, payload.to_dict()):
                sent = True

        return sent

    def alert_critical(self, title: str, message: str, **kwargs) -> bool:
        return self.send("critical", title, message, **kwargs)

    def alert_warning(self, title: str, message: str, **kwargs) -> bool:
        return self.send("warning", title, message, **kwargs)

    def alert_info(self, title: str, message: str, **kwargs) -> bool:
        return self.send("info", title, message, **kwargs)

    def alert_cycle_failure(self, cycle_id: str, stage: str, error: str, **kwargs) -> bool:
        meta = kwargs.get("metadata", {})
        meta.update({"stage": stage, "error_type": "cycle_failure"})
        return self.alert_critical(
            title=f"PredictCel Cycle Failed: {stage}",
            message=f"Cycle `{cycle_id}` failed at `{stage}` stage.\n\n*Error:* {error}",
            cycle_id=cycle_id,
            metadata=meta,
        )

    def alert_api_error(self, endpoint: str, error: str, retry_count: int = 0) -> bool:
        return self.alert_warning(
            title="Polymarket API Error",
            message=f"API endpoint `{endpoint}` failed after {retry_count} retries.\n\n*Error:* {error}",
            metadata={"endpoint": endpoint, "retry_count": retry_count},
        )

    def alert_no_signals(self, cycle_id: str, reason: str = "No signals generated") -> bool:
        return self.alert_warning(
            title="No Signals Generated",
            message=f"Cycle `{cycle_id}` produced no trading signals.\n\n*Reason:* {reason}",
            cycle_id=cycle_id,
            metadata={"reason": reason},
        )

    def alert_cycle_success(self, cycle_id: str, signals_count: int, metadata: dict[str, Any] | None = None) -> bool:
        meta = metadata or {}
        meta["signals_count"] = signals_count
        return self.alert_info(
            title="PredictCel Cycle Complete",
            message=f"Cycle `{cycle_id}` completed successfully.\n\n*Signals generated:* {signals_count}",
            cycle_id=cycle_id,
            metadata=meta,
        )


# Global singleton
_alert_manager: AlertManager | None = None


def get_alert_manager() -> AlertManager:
    global _alert_manager
    if _alert_manager is None:
        _alert_manager = AlertManager()
    return _alert_manager


__all__ = ["AlertManager", "AlertPayload", "get_alert_manager", "SlackFormatter"]
