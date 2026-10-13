"""Чтение подписок из панели (через RWMS) для rw-cleaner.

Снимок PanelSnapshot содержит только несекретные поля: ссылка (short_uuid),
ключи (vless_uuid, trojan_password, ss_password) и subscription_url в снимок
не попадают и никогда не логируются. Для архива берётся сырой ответ панели
(proto.UserResponse) строгого перечитывания прямо перед удалением.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime
from datetime import timezone
from typing import AsyncIterator
from typing import Optional

import proto.rwmanager_pb2 as proto
from common.subscription_archive import panel_user_restorable


class PanelScanError(Exception):
    """Перебор панели не удалось довести до конца (RWMS вернул ошибку или
    курсор повёл себя неожиданно). Решения по неполному перебору не
    принимаются — запуск прерывается без удалений."""


@dataclasses.dataclass(frozen=True)
class PanelSnapshot:
    uuid: str
    username: str
    status: str
    expire_at: Optional[datetime]
    first_connected_at: Optional[datetime] = None
    online_at: Optional[datetime] = None
    last_traffic_reset_at: Optional[datetime] = None
    lifetime_traffic_bytes: float = 0.0
    # Трафик текущего периода и последнее открытие ссылки подписки клиентом —
    # только для сравнения «до/после» в двухфазном удалении (free_inactive).
    used_traffic_bytes: float = 0.0
    sub_last_opened_at: Optional[datetime] = None
    traffic_limit_bytes: Optional[int] = None
    hwid_device_limit: Optional[int] = None
    internal_squads: tuple[str, ...] = ()
    external_squad_uuid: Optional[str] = None
    tag: Optional[str] = None
    email: Optional[str] = None
    telegram_id: Optional[int] = None
    restorable: bool = False

    @property
    def never_connected(self) -> bool:
        """Ни разу не подключался: нет first_connected / online_at и ноль
        трафика за всё время."""
        return (
            self.first_connected_at is None
            and self.online_at is None
            and not self.lifetime_traffic_bytes
        )

    def activity_fingerprint(self) -> tuple:
        """Всё, что меняется, когда пользователь подключается или открывает
        подписку: отличие между снимком фазы 1 (перед DISABLED) и фазы 2
        (после паузы) — признак активности, удалять нельзя."""
        return (
            self.online_at,
            self.first_connected_at,
            self.lifetime_traffic_bytes,
            self.used_traffic_bytes,
            self.sub_last_opened_at,
            self.last_traffic_reset_at,
        )


def _has(message, field: str) -> bool:
    try:
        return message.HasField(field)
    except ValueError:
        return getattr(message, field, None) not in (None, "", 0)


def timestamp_to_utc(value) -> Optional[datetime]:
    """protobuf Timestamp → aware UTC; нулевой/битый — None."""
    if value is None:
        return None
    try:
        if value.seconds == 0 and value.nanos == 0:
            return None
        return value.ToDatetime().replace(tzinfo=timezone.utc)
    except (AttributeError, ValueError, OverflowError):
        return None


def _optional_ts(user, field: str) -> Optional[datetime]:
    return timestamp_to_utc(getattr(user, field)) if _has(user, field) else None


def snapshot_from_proto(user: proto.UserResponse) -> PanelSnapshot:
    return PanelSnapshot(
        uuid=user.uuid,
        username=user.username,
        status=proto.UserStatus.Name(user.status) if _has(user, "status") else "",
        expire_at=_optional_ts(user, "expire_at"),
        first_connected_at=_optional_ts(user, "first_connected"),
        online_at=_optional_ts(user, "online_at"),
        last_traffic_reset_at=_optional_ts(user, "last_traffic_reset_at"),
        lifetime_traffic_bytes=float(user.lifetime_used_traffic_bytes or 0.0),
        used_traffic_bytes=float(user.used_traffic_bytes or 0.0),
        sub_last_opened_at=_optional_ts(user, "sub_last_opened_at"),
        traffic_limit_bytes=(
            user.traffic_limit_bytes if _has(user, "traffic_limit_bytes") else None
        ),
        hwid_device_limit=(
            user.hwid_device_limit if _has(user, "hwid_device_limit") else None
        ),
        internal_squads=tuple(squad.uuid for squad in user.active_internal_squads),
        external_squad_uuid=(
            user.external_squad_uuid if _has(user, "external_squad_uuid") else None
        ),
        tag=user.tag if _has(user, "tag") else None,
        email=user.email if _has(user, "email") else None,
        telegram_id=user.telegram_id if _has(user, "telegram_id") else None,
        restorable=panel_user_restorable(user),
    )


def same_credentials(user: proto.UserResponse, archive) -> bool:
    """Ссылка и ключи в панели совпадают с открытым архивом (значения не
    логируются — только результат сравнения)."""
    return (
        user.short_uuid == archive.short_uuid
        and user.vless_uuid == archive.vless_uuid
        and user.trojan_password == archive.trojan_password
        and user.ss_password == archive.ss_password
    )


async def iter_panel_users(
    rwms,
    *,
    page_size: int,
    timeout: float,
    status: Optional[int] = None,
    external_squad_uuid: Optional[str] = None,
) -> AsyncIterator[proto.UserResponse]:
    """Пользователи панели по курсору (StreamUsers) с фильтром на стороне
    панели (статус / внешний отряд; по тегу не фильтруем — тег принадлежит
    владельцу). Ошибка RWMS или нестандартный курсор — PanelScanError."""
    cursor = None
    seen_cursors: set[int] = set()
    while True:
        reply = await rwms.stream_users(
            size=page_size,
            cursor=cursor,
            status=status,
            external_squad_uuid=external_squad_uuid,
            timeout=timeout,
        )
        if reply is None:
            raise PanelScanError("stream_users failed")
        for user in reply.users:
            yield user
        if not reply.has_more:
            return
        if not _has(reply, "next_cursor"):
            raise PanelScanError("stream_users has_more without next_cursor")
        next_cursor = reply.next_cursor
        if next_cursor in seen_cursors or next_cursor == cursor:
            raise PanelScanError(f"stream_users cursor did not advance: {next_cursor}")
        seen_cursors.add(next_cursor)
        cursor = next_cursor
