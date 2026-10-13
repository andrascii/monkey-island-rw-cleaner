"""Файл состояния двухфазного удаления (режим free_inactive).

Фаза 1 переводит подписку в DISABLED, фаза 2 (после паузы) удаляет её или
включает обратно. Если процесс упал между фазами, подписка осталась бы
DISABLED навсегда — поэтому ДО вызова UpdateUser(DISABLED) сюда пишется
упреждающая запись (pending, см. ниже), а подтверждённый DISABLE
превращается в запись об отключении (entries); после удаления/обратного
включения она убирается. Все записи — атомарно: временный файл + fsync +
os.replace + fsync каталога. Каждый запуск с --execute первым делом
включает обратно всё, что осталось в entries от упавшего запуска. Запись
entries появляется только ПОСЛЕ подтверждённого DISABLE, поэтому ACTIVE при
восстановлении значит «включил кто-то другой» и запись можно убрать.

Там же — журнал упреждающей записи (write-ahead) изменений панели. ДО
отправки DeleteUser / UpdateUser(DISABLED) сюда пишется (fsync) запись со
state="pending"; не записалось — запрос не отправляется. После
ОКОНЧАТЕЛЬНО подтверждённого исхода запись переходит: DELETE подтверждён —
убирается; DISABLE подтверждён (DISABLED) — одной атомарной записью
превращается в обычную запись об отключении (фаза 2). Исход не подтвердился
(таймаут, исключение, пустой ответ, is_deleted=False, перечитывания ничего не
прояснили, отмена задачи) — state="uncertain". Запись, оставшаяся "pending"
к старту запуска (процесс убит/отменён во время запроса), — то же, что
"uncertain". Remnawave не умеет отменить уже принятый запрос, поэтому пока
такая запись есть, запуск с --execute ничего нового не отключает и не
удаляет — только перепроверяет записи (rw_cleaner/cleanup.py). Запись
снимается по истечении карантина или вручную (--clear-uncertain).

Схема БД не меняется: файл лежит рядом с отчётом (по умолчанию
report/rw-cleaner-free-disabled.json) и должен быть на постоянном томе.
Один запуск на файл — эксклюзивный flock на соседнем <файл>.lock (поверх
pg_try_advisory_lock на базу). В файле только несекретные поля: uuid
подписки, username, user_id, id архива и время.

Формат: {"version": 3, "entries": [...], "uncertain": [...]}; у записи
uncertain поле "state" ("pending" | "uncertain"). Файлы версии 1 (без
"uncertain") и 2 (без "state" — значит "uncertain") читаются как есть.
"""

from __future__ import annotations

import dataclasses
import json
import os
from pathlib import Path
from typing import Optional

try:  # pragma: no cover - на Linux/macOS есть всегда
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None

STATE_VERSION = 3
DEFAULT_STATE_PATH = "report/rw-cleaner-free-disabled.json"


class StateError(Exception):
    """Файл состояния не читается/не пишется: что отключено прошлым
    запуском — неизвестно, новые отключения запрещены."""


@dataclasses.dataclass(frozen=True)
class DisabledEntry:
    panel_uuid: str
    username: str
    user_id: int
    disabled_at: str  # ISO-8601 UTC

    @classmethod
    def from_json(cls, raw) -> "DisabledEntry":
        try:
            entry = cls(
                panel_uuid=str(raw["panel_uuid"]),
                username=str(raw["username"]),
                user_id=int(raw["user_id"]),
                disabled_at=str(raw.get("disabled_at") or ""),
            )
        except (KeyError, TypeError, ValueError) as e:
            raise StateError(f"bad state entry: {type(e).__name__}") from e
        if not entry.panel_uuid or not entry.username:
            raise StateError("bad state entry: empty panel_uuid/username")
        return entry


OP_DELETE = "delete"
OP_DISABLE = "disable"
UNCERTAIN_OPS = (OP_DELETE, OP_DISABLE)

# Состояние записи журнала: запрос вот-вот уйдёт / ушёл, исход ещё не
# известен (write-ahead) | исход так и не подтвердился.
STATE_PENDING = "pending"
STATE_UNCERTAIN = "uncertain"
RECORD_STATES = (STATE_PENDING, STATE_UNCERTAIN)


@dataclasses.dataclass(frozen=True)
class UncertainEntry:
    """DELETE/DISABLE, исход которого не подтверждён: запоздавший запрос ещё
    может дойти до панели. state=pending — запись упреждающая (сделана ДО
    запроса); если она пережила запуск, это то же, что uncertain."""

    op: str  # OP_DELETE | OP_DISABLE
    panel_uuid: str
    username: str
    user_id: int
    started_at: str  # ISO-8601 UTC, когда отправлен запрос
    last_checked_at: str = ""  # ISO-8601 UTC последнего перечитывания панели
    # Что панель показала при последнем перечитывании: ACTIVE, DISABLED,
    # EXPIRED, LIMITED, GONE (подписки нет), UUID_CHANGED, UNREADABLE.
    last_observed: str = ""
    archive_id: Optional[int] = None  # только delete: архив подписки
    state: str = STATE_UNCERTAIN  # STATE_PENDING | STATE_UNCERTAIN

    @property
    def pending(self) -> bool:
        return self.state == STATE_PENDING

    @classmethod
    def from_json(cls, raw) -> "UncertainEntry":
        try:
            archive_id = raw.get("archive_id")
            # Нет поля (файл версии 2) — запись uncertain.
            state = raw.get("state")
            state = STATE_UNCERTAIN if state is None else str(state)
            entry = cls(
                op=str(raw["op"]),
                panel_uuid=str(raw["panel_uuid"]),
                username=str(raw["username"]),
                user_id=int(raw["user_id"]),
                started_at=str(raw.get("started_at") or ""),
                last_checked_at=str(raw.get("last_checked_at") or ""),
                last_observed=str(raw.get("last_observed") or ""),
                archive_id=int(archive_id) if archive_id is not None else None,
                state=state,
            )
        except (KeyError, TypeError, ValueError, AttributeError) as e:
            raise StateError(f"bad uncertain entry: {type(e).__name__}") from e
        if entry.op not in UNCERTAIN_OPS:
            raise StateError("bad uncertain entry: unknown op")
        if entry.state not in RECORD_STATES:
            raise StateError("bad uncertain entry: unknown state")
        if not entry.panel_uuid or not entry.username:
            raise StateError("bad uncertain entry: empty panel_uuid/username")
        return entry


class DisabledState:
    """Подписки, которые rw-cleaner перевёл в DISABLED и ещё не довёл до
    конца (удаление или обратное включение)."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self._lock_path = self.path.with_name(self.path.name + ".lock")
        self._lock_fd: Optional[int] = None
        self._entries: dict[str, DisabledEntry] = {}
        self._uncertain: dict[str, UncertainEntry] = {}

    # --- блокировка файла --------------------------------------------------

    def acquire(self) -> bool:
        """Эксклюзивный неблокирующий flock. False — файл держит другой
        процесс."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self._lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        if fcntl is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                os.close(fd)
                return False
        self._lock_fd = fd
        return True

    def release(self) -> None:
        if self._lock_fd is None:
            return
        try:
            if fcntl is not None:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
        finally:
            os.close(self._lock_fd)
            self._lock_fd = None

    # --- чтение/запись -----------------------------------------------------

    def load(self) -> list[DisabledEntry]:
        """Записи из файла (нет файла — пусто). Битый файл — StateError."""
        if not self.path.exists():
            self._entries = {}
            self._uncertain = {}
            return []
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as e:
            raise StateError(f"state file unreadable: {type(e).__name__}") from e
        if not isinstance(raw, dict) or not isinstance(raw.get("entries"), list):
            raise StateError("state file has unexpected structure")
        uncertain_raw = raw.get("uncertain", [])
        if not isinstance(uncertain_raw, list):
            raise StateError("state file has unexpected structure")
        entries = [DisabledEntry.from_json(item) for item in raw["entries"]]
        uncertain = [UncertainEntry.from_json(item) for item in uncertain_raw]
        self._entries = {entry.panel_uuid: entry for entry in entries}
        self._uncertain = {entry.panel_uuid: entry for entry in uncertain}
        return list(self._entries.values())

    @property
    def entries(self) -> list[DisabledEntry]:
        return list(self._entries.values())

    @property
    def uncertain(self) -> list[UncertainEntry]:
        return list(self._uncertain.values())

    def add(self, entry: DisabledEntry) -> None:
        """Записать подписку, отключённую этим инструментом (DISABLE
        подтверждён). Ошибка — OSError."""
        entries = dict(self._entries)
        entries[entry.panel_uuid] = entry
        self._write(entries, self._uncertain)
        self._entries = entries

    def remove(self, panel_uuid: str) -> None:
        if panel_uuid not in self._entries:
            return
        entries = dict(self._entries)
        entries.pop(panel_uuid)
        self._write(entries, self._uncertain)
        self._entries = entries

    def put_uncertain(
        self, entry: UncertainEntry, *, drop_disabled: bool = False
    ) -> None:
        """Записать/обновить запись uncertain одной атомарной записью файла.
        drop_disabled — заодно убрать запись об отключении той же подписки
        (неопределённый DISABLE: дальше её ведёт запись uncertain). Ошибка —
        OSError."""
        entries = dict(self._entries)
        if drop_disabled:
            entries.pop(entry.panel_uuid, None)
        uncertain = dict(self._uncertain)
        uncertain[entry.panel_uuid] = entry
        self._write(entries, uncertain)
        self._entries, self._uncertain = entries, uncertain

    def confirm_disable(self, entry: DisabledEntry) -> None:
        """DISABLE окончательно подтверждён: одной атомарной записью файла
        убрать упреждающую запись (pending/uncertain) этой подписки и
        добавить обычную запись об отключении (её ведёт фаза 2 и
        восстановление). Ошибка — OSError, файл не изменён."""
        entries = dict(self._entries)
        entries[entry.panel_uuid] = entry
        uncertain = dict(self._uncertain)
        uncertain.pop(entry.panel_uuid, None)
        self._write(entries, uncertain)
        self._entries, self._uncertain = entries, uncertain

    def remove_uncertain(self, panel_uuid: str) -> Optional[UncertainEntry]:
        """Убрать запись uncertain; нет такой — None. Ошибка — OSError."""
        if panel_uuid not in self._uncertain:
            return None
        uncertain = dict(self._uncertain)
        removed = uncertain.pop(panel_uuid)
        self._write(self._entries, uncertain)
        self._uncertain = uncertain
        return removed

    def _write(
        self,
        entries: dict[str, DisabledEntry],
        uncertain: dict[str, UncertainEntry],
    ) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": STATE_VERSION,
            "entries": [dataclasses.asdict(e) for e in entries.values()],
            "uncertain": [dataclasses.asdict(e) for e in uncertain.values()],
        }
        tmp = self.path.with_name(self.path.name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=1)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, self.path)
        try:
            dir_fd = os.open(self.path.parent, os.O_RDONLY)
        except OSError:  # pragma: no cover - не все ФС дают открыть каталог
            return
        try:
            os.fsync(dir_fd)
        except OSError:  # pragma: no cover
            pass
        finally:
            os.close(dir_fd)
