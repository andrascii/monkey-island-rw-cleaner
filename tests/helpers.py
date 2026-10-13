"""Помощники тестов rw-cleaner: поддельный RWMS (панель) с курсорным
StreamUsers, сборка ответов панели и строк БД."""

import copy
import itertools
import uuid as uuid_lib
from datetime import datetime
from datetime import timedelta
from datetime import timezone

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles

import proto.rwmanager_pb2 as proto
from common.models.db import User
from common.rwms_client import RwmsUnavailableError


@compiles(JSONB, "sqlite")
def _jsonb_on_sqlite(element, compiler, **kw):
    # Тестовые схемы на SQLite: JSONB хранится как JSON (как в common/tests).
    return "JSON"


NOW = datetime(2026, 10, 11, 12, 0, tzinfo=timezone.utc)


def naive(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def days_ago(days: float) -> datetime:
    return NOW - timedelta(days=days)


def credentials(username: str) -> dict:
    return {
        "short_uuid": f"Sh{username}abcd",
        "vless_uuid": str(uuid_lib.uuid5(uuid_lib.NAMESPACE_DNS, username)),
        "trojan_password": f"trojan-{username}-pw",
        "ss_password": f"ss-{username}-secret",
        "subscription_url": f"https://sub.example/{username}-subscription-url",
    }


def secrets_of(username: str) -> list[str]:
    values = credentials(username)
    return [
        values["short_uuid"],
        values["vless_uuid"],
        values["trojan_password"],
        values["ss_password"],
        values["subscription_url"],
    ]


def panel_user(
    username: str,
    *,
    uuid: str | None = None,
    status=proto.UserStatus.EXPIRED,
    expire_at: datetime | None = None,
    first_connected: datetime | None = None,
    online_at: datetime | None = None,
    lifetime_traffic: float = 0.0,
    tag: str | None = None,
    traffic_limit_bytes: int | None = None,
    hwid_device_limit: int | None = None,
    squads: tuple[str, ...] = ("squad-default",),
    telegram_id: int | None = None,
    email: str | None = None,
    **overrides,
) -> proto.UserResponse:
    values = credentials(username)
    values.update(overrides)
    user = proto.UserResponse(
        uuid=uuid or f"uuid-{username}",
        username=username,
        status=status,
        lifetime_used_traffic_bytes=lifetime_traffic,
        **values,
    )
    user.expire_at.FromDatetime(naive(expire_at or days_ago(120)))
    user.created_at.FromDatetime(naive(days_ago(400)))
    if first_connected:
        user.first_connected.FromDatetime(naive(first_connected))
    if online_at:
        user.online_at.FromDatetime(naive(online_at))
    if tag is not None:
        user.tag = tag
    if traffic_limit_bytes is not None:
        user.traffic_limit_bytes = traffic_limit_bytes
    if hwid_device_limit is not None:
        user.hwid_device_limit = hwid_device_limit
    if telegram_id is not None:
        user.telegram_id = telegram_id
    if email is not None:
        user.email = email
    for squad in squads:
        user.active_internal_squads.add(uuid=squad, name=squad)
    return user


class FakeRwms:
    """Поддельный RWMS: StreamUsers по курсору (индекс в отсортированном
    списке), строгое чтение по username, DeleteUser. Хуки позволяют менять
    состояние между фазами запуска."""

    def __init__(self, users=()):
        self.users = {u.username: u for u in users}
        self.stream_calls = []
        self.strict_calls = []
        self.delete_calls = []
        self.deleted = []
        self.fail_stream_on_call = None  # номер вызова (1..), на котором вернуть None
        self.stuck_cursor = False
        self.strict_errors = set()
        # username -> None (ошибка RPC, удаления нет) | False (is_deleted=False)
        # | "lost" (панель удалила, ответ потерян — None)
        # | ("late", n) (ответ None, удаление «доходит» до панели на n-м
        #   строгом перечитывании после вызова)
        self.delete_results = {}
        self.update_calls = []
        # username -> None (ошибка RPC, не применено) | "lost" (применено,
        # ответ потерян — None) | "raise" (исключение, не применено)
        self.update_results = {}
        self.on_strict = None
        self.on_delete = None
        self.on_update = None
        self._late_deletes = {}  # username -> оставшиеся перечитывания

    async def stream_users(
        self,
        size=1000,
        cursor=None,
        status=None,
        tag=None,
        external_squad_uuid=None,
        *,
        timeout=None,
    ):
        self.stream_calls.append(
            {
                "size": size,
                "cursor": cursor,
                "status": status,
                "tag": tag,
                "external_squad_uuid": external_squad_uuid,
                "timeout": timeout,
            }
        )
        if self.fail_stream_on_call == len(self.stream_calls):
            return None
        items = sorted(self.users.values(), key=lambda u: u.username)
        if status is not None:
            items = [u for u in items if u.HasField("status") and u.status == status]
        if tag is not None:
            items = [u for u in items if u.HasField("tag") and u.tag == tag]
        if external_squad_uuid is not None:
            items = [
                u
                for u in items
                if u.HasField("external_squad_uuid")
                and u.external_squad_uuid.lower() == external_squad_uuid.lower()
            ]
        start = cursor or 0
        page = items[start : start + size]
        end = start + len(page)
        reply = proto.StreamUsersReply(users=page, has_more=end < len(items))
        if reply.has_more:
            reply.next_cursor = start if self.stuck_cursor and cursor else end
        return reply

    async def get_user_by_username_strict(self, username, *, timeout=None):
        self.strict_calls.append({"username": username, "timeout": timeout})
        if self.on_strict:
            self.on_strict(username)
        if username in self._late_deletes:
            self._late_deletes[username] -= 1
            if self._late_deletes[username] <= 0:
                self._late_deletes.pop(username)
                self.users.pop(username, None)
                self.deleted.append(username)
        if username in self.strict_errors:
            raise RwmsUnavailableError(username, "UNAVAILABLE", "down")
        user = self.users.get(username)
        return copy.deepcopy(user) if user is not None else None

    async def delete_user(self, uuid, *, timeout=None):
        username = next(
            (name for name, u in self.users.items() if u.uuid == uuid), None
        )
        self.delete_calls.append(
            {"username": username, "uuid": uuid, "timeout": timeout}
        )
        if self.on_delete:
            self.on_delete(username)
        result = self.delete_results.get(username, True)
        if result is None:
            return None
        if result == "lost":
            self.users.pop(username, None)
            self.deleted.append(username)
            return None
        if isinstance(result, tuple) and result[0] == "late":
            self._late_deletes[username] = result[1]
            return None
        if result is False:
            return proto.DeleteUserResponse(is_deleted=False)
        self.users.pop(username, None)
        self.deleted.append(username)
        return proto.DeleteUserResponse(is_deleted=True)


    async def update_user(self, request, *, timeout=None):
        username = next(
            (name for name, u in self.users.items() if u.uuid == request.uuid), None
        )
        self.update_calls.append(
            {
                "username": username,
                "uuid": request.uuid,
                "status": (
                    proto.UserStatus.Name(request.status)
                    if request.HasField("status")
                    else None
                ),
                "fields": [f.name for f, _ in request.ListFields()],
                "timeout": timeout,
            }
        )
        if self.on_update:
            self.on_update(username, request)
        if username is None:
            return None
        result = self.update_results.get(username, True)
        if result == "raise":
            raise RuntimeError("grpc exploded")
        if result is None:
            return None
        user = self.users[username]
        if request.HasField("status"):
            user.status = request.status
        if result == "lost":
            return None
        return copy.deepcopy(user)


_user_ids = itertools.count(1)


def add_user(session, username, *, expire_at=None, user_id=None, **kw) -> User:
    user = User(
        id=user_id or next(_user_ids),
        username=username,
        expire_at=naive(expire_at) if expire_at else None,
        **kw,
    )
    session.add(user)
    session.flush()
    return user
