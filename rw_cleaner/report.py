"""Структурированные логи (key=value) и CSV-отчёт запуска rw-cleaner.

В лог и отчёт попадают только несекретные поля снимка панели и фактов БД:
ссылка подписки, ключи и токены сюда не передаются никогда.
"""

from __future__ import annotations

import csv
import dataclasses
from datetime import datetime
from pathlib import Path
from typing import Optional

from rw_cleaner.panel import PanelSnapshot
from rw_cleaner.rules import DbFacts
from rw_cleaner.rules import Decision

REPORT_COLUMNS = (
    "username",
    "panel_uuid",
    "user_id",
    "mode",
    "tier",
    "decision",
    "reason",
    "action",
    "archive_id",
    "panel_status",
    "panel_expire_at",
    "db_expire_at",
    "expire_diff_hours",
    "first_connected_at",
    "online_at",
    "lifetime_traffic_bytes",
    "ever_paid",
    "last_payment_at",
    "free_switched_at",
    "notes",
    # Двухфазное удаление (free_inactive): итог обратного включения
    # подписки, отключённой в фазе 1 (reenabled, not_disabled, gone,
    # kept_disabled_blocked, …); пусто — не отключалась или удалена.
    "reenable",
)


def _fmt(value) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else f"{value:.0f}"
    if isinstance(value, (tuple, list, frozenset, set)):
        return ";".join(str(v) for v in value)
    return str(value)


def kv(**fields) -> str:
    """key=value через пробел; значения с пробелами — в кавычках."""
    parts = []
    for key, value in fields.items():
        text = _fmt(value) or "-"
        if any(ch.isspace() for ch in text) or '"' in text:
            text = '"' + text.replace('"', "'") + '"'
        parts.append(f"{key}={text}")
    return " ".join(parts)


@dataclasses.dataclass
class ReportRow:
    panel: PanelSnapshot
    facts: DbFacts
    decision: Decision
    mode: str
    tier: str
    action: str = ""
    archive_id: Optional[int] = None
    reenable: str = ""

    def fields(self) -> dict:
        diff = None
        if self.panel.expire_at and self.facts.db_expire_at:
            seconds = abs(
                (self.panel.expire_at - self.facts.db_expire_at).total_seconds()
            )
            diff = f"{seconds / 3600:.1f}"
        return {
            "username": self.panel.username,
            "panel_uuid": self.panel.uuid,
            "user_id": self.facts.user_id,
            "mode": self.mode,
            "tier": self.tier,
            "decision": "candidate" if self.decision.candidate else "skip",
            "reason": self.decision.reason,
            "action": self.action,
            "archive_id": self.archive_id,
            "panel_status": self.panel.status,
            "panel_expire_at": self.panel.expire_at,
            "db_expire_at": self.facts.db_expire_at,
            "expire_diff_hours": diff,
            "first_connected_at": self.panel.first_connected_at,
            "online_at": self.panel.online_at,
            "lifetime_traffic_bytes": self.panel.lifetime_traffic_bytes,
            "ever_paid": self.facts.ever_paid if not self.facts.orphan else None,
            "last_payment_at": self.facts.last_payment_at,
            "free_switched_at": self.facts.free_switched_at,
            "notes": self.decision.notes,
            "reenable": self.reenable,
        }

    def log_line(self) -> str:
        return kv(**self.fields())


def write_report(path: Path, rows: list[ReportRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _fmt(value) for key, value in row.fields().items()})
