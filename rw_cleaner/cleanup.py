"""rw-cleaner: архивирование и удаление из панели старых истёкших подписок
(режим expired) и неактивных бесплатных (режим free_inactive).

Порядок для каждого кандидата: строгое перечитывание подписки из панели →
повторное решение по свежим данным панели и БД → запись в
subscription_archives и COMMIT → транзакция с SELECT ... FOR UPDATE NOWAIT
строки users → повторное перечитывание панели и фактов БД под блокировкой и
повторное решение → DeleteUser, пока блокировка держится → COMMIT. Продление
(платёж, промо, админ), начавшееся до блокировки, видно под ней и отменяет
удаление; начавшееся после — ждёт COMMIT очистителя и уже видит NOT_FOUND →
восстанавливает подписку из архива. Если удаление прервано ДО DeleteUser
(повторная проверка под блокировкой отменила его, строка занята, сбой), а
архив создан ЭТИМ запуском, архив закрывается отдельной транзакцией
(restored_at=now, restored_by="rw-cleaner:aborted") — открытый архив рядом с
живой подпиской не остаётся. Подписку из архива
любой путь «подписки нет в панели — создать» восстанавливает с той же ссылкой
и ключами (common/subscription_archive.py).

Журнал упреждающей записи (write-ahead): ДО отправки DeleteUser или
UpdateUser(DISABLED) в файл состояния пишется (fsync) запись state=pending;
не записалась — запрос не отправляется, запуск останавливается. Исход
окончательно подтверждён — запись переходит (DELETE: убирается; DISABLE:
атомарно становится обычной записью об отключении для фазы 2). Исход
неизвестен (таймаут, UNAVAILABLE, пустой ответ, is_deleted=False,
исключение) — НЕ «ошибка, повторим потом»: запись становится uncertain,
блокировка users не снимается, панель строго перечитывается не дольше
UNCERTAIN_CONFIRM_SECONDS (не меньше 2×RWMS_TIMEOUT_SECONDS). Исход
подтвердился (подписки нет / она DISABLED) — запись переходит как выше; нет —
CRITICAL и остановка всего запуска (код 6). Отмена задачи
(asyncio.CancelledError), KeyboardInterrupt/SystemExit во время запроса —
запись помечается uncertain и исключение пробрасывается дальше; жёсткое
падение процесса оставляет запись pending — следующий запуск считает её
uncertain. Remnawave не умеет отменить принятый запрос, поэтому пока есть
хоть одна запись pending/uncertain, запуск с --execute ничего нового не
отключает и не удаляет: только перепроверяет записи (запоздавший DISABLE —
включает подписку обратно) и выходит с кодом 6. Запись снимается после
карантина UNCERTAIN_QUARANTINE_HOURS (и только если последнее перечитывание
это допускает) или вручную: --clear-uncertain <uuid> --execute.

Режим free_inactive — двухфазный (у Remnawave нет условного удаления, а
online_at/трафик панель обновляет с задержкой): фаза 1 под блокировкой users
переводит подписки партии в DISABLED (только статус) и записывает их в файл
состояния; пауза FREE_SETTLE_SECONDS (один раз на партию); фаза 2 под
блокировкой перечитывает БД и панель: активность изменилась, статус уже не
DISABLED, правило не выполняется или любой сбой — подписка включается
обратно (ACTIVE), иначе — архив и DeleteUser. Всё, что осталось DISABLED
после упавшего запуска, включает обратно следующий запуск с --execute
(rw_cleaner/state.py).

Сироты (подписка в панели без строки users) не удаляются никогда.

По умолчанию — dry-run: ни записи в БД, ни изменений в панели. Реальное
удаление — только флаг командной строки --execute (переменная окружения его
не включает). БД используется только на чтение, кроме таблицы
subscription_archives. Строки users и связанных таблиц не удаляются никогда.
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import dataclasses
import getpass
import logging
import math
import os
import time
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Callable
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy import exists
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

import proto.rwmanager_pb2 as proto
from common.free_tier import FREE_TIER_SETTING_KEYS
from common.free_tier import free_tier_settings_from
from common.models.db import SubscriptionArchive
from common.models.db import SystemSetting
from common.models.db import User
from common.models.db import UserBlock
from common.models.settings import BOT_INTERNAL_SQUADS_UUIDS_SETTING
from common.rwms_client import RwmsClient
from common.rwms_client import RwmsUnavailableError
from common.setup_logger import setup_logger
from common.subscription_archive import ARCHIVE_REASON_EXPIRED
from common.subscription_archive import ARCHIVE_REASON_FREE_INACTIVE
from common.subscription_archive import build_archive
from common.subscription_archive import open_archive_query
from rw_cleaner.panel import PanelScanError
from rw_cleaner.panel import PanelSnapshot
from rw_cleaner.panel import iter_panel_users
from rw_cleaner.panel import same_credentials
from rw_cleaner.panel import snapshot_from_proto
from rw_cleaner.report import ReportRow
from rw_cleaner.report import kv
from rw_cleaner.report import write_report
from rw_cleaner.rules import MIN_FREE_INACTIVE_DAYS
from rw_cleaner.rules import MODE_EXPIRED
from rw_cleaner.rules import MODE_FREE_INACTIVE
from rw_cleaner.rules import MODES
from rw_cleaner.rules import TIER_NEVER_CONNECTED
from rw_cleaner.rules import TIERS
from rw_cleaner.rules import Decision
from rw_cleaner.rules import RuleConfig
from rw_cleaner.rules import decide_expired
from rw_cleaner.rules import decide_free_inactive
from rw_cleaner.rules import effective_free_inactive_days
from rw_cleaner.rules import load_db_facts
from rw_cleaner.rules import naive_utc
from rw_cleaner.rules import payment_schema_problems
from rw_cleaner.state import DEFAULT_STATE_PATH
from rw_cleaner.state import OP_DELETE
from rw_cleaner.state import OP_DISABLE
from rw_cleaner.state import STATE_PENDING
from rw_cleaner.state import STATE_UNCERTAIN
from rw_cleaner.state import DisabledEntry
from rw_cleaner.state import DisabledState
from rw_cleaner.state import StateError
from rw_cleaner.state import UncertainEntry

logger = logging.getLogger(__name__)

EXIT_OK = 0
# Часть кандидатов не обработана (архив остаётся открытым — повтор в
# следующий запуск).
EXIT_FAILURES = 1
EXIT_CONFIG = 2  # ошибка настроек (argparse)
EXIT_LOCKED = 3  # уже идёт другой запуск
EXIT_AUTO_STOP = 4  # предохранитель: доля кандидатов/сирот, серия ошибок
EXIT_PANEL_SCAN_FAILED = 5  # перебор панели не завершён — ничего не удалялось
# Нужен человек: DeleteUser/UpdateUser(DISABLED) с неизвестным исходом
# (delete_uncertain / disable_uncertain) или в файле состояния есть записи
# uncertain (запуск ничего нового не делал), подписку, отключённую этим
# инструментом, не удалось включить обратно, или файл состояния не читается.
EXIT_UNCERTAIN = 6

# Ключ pg_try_advisory_lock: один запуск rw-cleaner на базу ("mi_rw_cl").
ADVISORY_LOCK_KEY = 0x6D695F72775F636C

MIN_GRACE_DAYS = 45  # всегда позже шага 4 win-back (день 30 + оффер 48 ч)
HARD_MAX_DELETIONS = 1000
MAX_PAGE_SIZE = 1000  # предел StreamUsers

# Пауза между фазами free_inactive: не меньше интервала, с которым панель
# обновляет online_at / трафик (иначе подключение прямо перед DISABLED ещё
# не видно в фазе 2).
DEFAULT_FREE_SETTLE_SECONDS = 180.0
MIN_FREE_SETTLE_SECONDS = 120.0

# Подтверждение DeleteUser / UpdateUser(DISABLED) с неизвестным исходом:
# строгие перечитывания с этим шагом, всего не дольше
# UNCERTAIN_CONFIRM_SECONDS, но не меньше DELETE_CONFIRM_FACTOR ×
# RWMS_TIMEOUT_SECONDS (блокировка users всё это время держится).
DELETE_CONFIRM_POLL_SECONDS = 2.0
DELETE_CONFIRM_FACTOR = 2
DEFAULT_UNCERTAIN_CONFIRM_SECONDS = 180.0
# Карантин записи uncertain: раньше этого срока с момента запроса запись не
# снимается автоматически (только --clear-uncertain).
DEFAULT_UNCERTAIN_QUARANTINE_HOURS = 24.0
MIN_UNCERTAIN_QUARANTINE_HOURS = 1.0

# Обратное включение: строку users может держать платёж — несколько коротких
# попыток взять блокировку NOWAIT.
REENABLE_LOCK_ATTEMPTS = 5
REENABLE_LOCK_RETRY_SECONDS = 1.0

PANEL_STATUS_DISABLED = "DISABLED"
PANEL_STATUS_ACTIVE = "ACTIVE"

# Итоги обратного включения, после которых запись убирается из файла
# состояния: включили; подписки нет; под этим username уже другая подписка;
# статус уже не DISABLED (включил кто-то другой — платёж, админ); админская
# блокировка (user_blocks) — статусом теперь владеет она.
REENABLE_RESOLVED = frozenset(
    {"reenabled", "gone", "panel_uuid_changed", "not_disabled", "kept_disabled_blocked"}
)

# restored_by архива, закрытого после прерванного (до DeleteUser) удаления.
ABORTED_ARCHIVE_RESTORED_BY = "rw-cleaner:aborted"
# Итоги повторной проверки под блокировкой, при которых архив НЕ закрывается:
# подписки в панели уже нет — архив остаётся единственным способом вернуть
# ту же ссылку и ключи.
KEEP_ARCHIVE_OPEN_REASONS = frozenset({"gone_from_panel"})

# SQLSTATE lock_not_available: строку users прямо сейчас держит другая
# транзакция (платёж, продление) — FOR UPDATE NOWAIT падает сразу.
PG_LOCK_NOT_AVAILABLE = "55P03"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime(ISO_FORMAT)


def parse_iso_utc(value: str) -> Optional[datetime]:
    try:
        return datetime.strptime(value, ISO_FORMAT).replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def operator_name() -> str:
    """Кто снимает запись uncertain вручную (для лога)."""
    sudo_user = os.getenv("SUDO_USER")
    try:
        user = getpass.getuser()
    except Exception:  # noqa: BLE001 - нет имени в окружении
        user = "unknown"
    return f"{user}(sudo:{sudo_user})" if sudo_user else user


@dataclasses.dataclass(frozen=True)
class CleanerConfig:
    database_url: str
    rwms_host: str
    rwms_port: int
    mode: str = MODE_EXPIRED
    tier: str = TIER_NEVER_CONNECTED
    grace_days: int = 90
    expire_tolerance_hours: int = 24
    recent_payment_days: int = 14
    page_size: int = 500
    max_deletions: int = 25
    delete_pause_seconds: float = 1.0
    max_consecutive_failures: int = 3
    max_candidate_ratio: float = 0.6
    # Режим free_inactive: неактивными могут быть почти все бесплатные
    # подписки, поэтому свой (более высокий) порог доли кандидатов.
    max_free_candidate_ratio: float = 0.9
    max_orphan_ratio: float = 0.05
    ratio_min_sample: int = 200
    rwms_timeout_seconds: float = 30.0
    stream_timeout_seconds: float = 60.0
    # Пауза между фазой 1 (DISABLED) и фазой 2 (удаление) режима
    # free_inactive, один раз на партию.
    free_settle_seconds: float = DEFAULT_FREE_SETTLE_SECONDS
    execute: bool = False
    full_scan: bool = False
    username_allowlist: frozenset[str] = frozenset()
    report_path: Optional[str] = None
    # Файл состояния двухфазного удаления (подписки, отключённые и ещё не
    # доведённые до конца) и записей uncertain. None — DEFAULT_STATE_PATH.
    state_path: Optional[str] = None
    # Сколько ждать подтверждения DeleteUser/UpdateUser(DISABLED) с
    # неизвестным исходом под блокировкой users (фактически — не меньше
    # DELETE_CONFIRM_FACTOR × rwms_timeout_seconds, см. confirm_bound).
    uncertain_confirm_seconds: float = DEFAULT_UNCERTAIN_CONFIRM_SECONDS
    uncertain_quarantine_hours: float = DEFAULT_UNCERTAIN_QUARANTINE_HOURS
    # --clear-uncertain: снять эти записи uncertain (uuid подписки) после
    # ручного разбора и выйти, ничего больше не делая. Только с --execute.
    clear_uncertain: tuple[str, ...] = ()

    @property
    def confirm_bound(self) -> float:
        return max(
            self.uncertain_confirm_seconds,
            DELETE_CONFIRM_FACTOR * self.rwms_timeout_seconds,
        )

    def rule_config(self) -> RuleConfig:
        return RuleConfig(
            grace_days=self.grace_days,
            expire_tolerance_hours=self.expire_tolerance_hours,
            recent_payment_days=self.recent_payment_days,
            tier=self.tier,
            username_allowlist=self.username_allowlist,
        )


@dataclasses.dataclass
class CleanupSummary:
    scanned: int = 0
    candidates: int = 0
    planned: int = 0
    over_cap: int = 0
    orphans: int = 0
    dry_run: int = 0
    archived: int = 0
    archive_reused: int = 0
    deleted: int = 0
    skipped_at_execution: int = 0
    # Открытый архив не совпал с подпиской в панели — пропуск (не ошибка),
    # нужен ручной разбор; входит и в skipped_at_execution.
    archive_mismatch: int = 0
    # Архив, созданный этим запуском, закрыт после прерванного до DeleteUser
    # удаления (restored_by="rw-cleaner:aborted") / закрыть не удалось.
    archive_closed_aborted: int = 0
    archive_close_failed: int = 0
    failed: int = 0
    # DeleteUser / UpdateUser(DISABLED) с неизвестным исходом, подтвердить
    # не удалось — запись uncertain, запуск остановлен.
    delete_uncertain: int = 0
    disable_uncertain: int = 0
    # Записи uncertain: осталось после перепроверки этим запуском; снято
    # после карантина; запоздавших DISABLE, включённых обратно; снято
    # вручную (--clear-uncertain).
    uncertain_pending: int = 0
    uncertain_dropped: int = 0
    uncertain_reenabled: int = 0
    uncertain_cleared: int = 0
    # Двухфазное удаление (free_inactive): отключено в фазе 1; включено
    # обратно; оставлено DISABLED из-за админской блокировки; включить не
    # удалось (подписка, возможно, остаётся DISABLED — нужен человек);
    # включено обратно при старте после упавшего запуска.
    disabled: int = 0
    reenabled: int = 0
    kept_disabled_blocked: int = 0
    reenable_failed: int = 0
    recovered: int = 0
    skip_reasons: collections.Counter = dataclasses.field(
        default_factory=collections.Counter
    )
    stop_reason: Optional[str] = None
    exit_code: int = EXIT_OK
    report_path: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class Phase1:
    """Итог фазы 1 для одной подписки: запись в файле состояния и снимок
    активности, снятый под блокировкой прямо перед DISABLED."""

    entry: DisabledEntry
    fingerprint: tuple


class ArchiveMismatchError(Exception):
    """Открытый архив username есть, но не совпадает с подпиской в панели
    (другие ключи или другой user_id) — состояние неоднозначно."""


def lock_user_query(user_id: int):
    """Блокировка строки users на время повторной проверки и DeleteUser.
    NOWAIT: если строку держит другая транзакция, ждать не будем —
    состояние пользователя меняется прямо сейчас, кандидат пропускается."""
    return (
        select(User.id, User.username)
        .where(User.id == user_id)
        .with_for_update(nowait=True)
    )


def _lock_not_available(error: OperationalError) -> bool:
    orig = getattr(error, "orig", None)
    code = getattr(orig, "pgcode", None) or getattr(orig, "sqlstate", None)
    return code == PG_LOCK_NOT_AVAILABLE


def _csv_set(raw) -> frozenset[str]:
    if not raw:
        return frozenset()
    return frozenset(part.strip() for part in str(raw).split(",") if part.strip())


def _ratio(value: float) -> str:
    """Доля для логов строкой: kv/_fmt округляет float до целого (он для
    байтов трафика), и 0.6 превращалось бы в «1»."""
    return f"{value:.4g}"


def _status_name(user) -> str:
    if user is None or not user.HasField("status"):
        return ""
    return proto.UserStatus.Name(user.status)


def load_system_settings(session, keys) -> dict:
    rows = session.execute(
        select(SystemSetting.key, SystemSetting.value).where(
            SystemSetting.key.in_(list(keys))
        )
    ).all()
    return {key: value for key, value in rows}


class AdvisoryLock:
    """Один запуск на базу: сессионный pg_try_advisory_lock на отдельном
    соединении, которое держится весь запуск. __enter__ → False — занято."""

    def __init__(self, engine, key: int = ADVISORY_LOCK_KEY):
        self._engine = engine
        self._key = key
        self._conn = None
        self._acquired = False

    def __enter__(self) -> bool:
        if self._engine.dialect.name != "postgresql":
            raise RuntimeError("rw-cleaner lock requires PostgreSQL")
        self._conn = self._engine.connect()
        self._acquired = bool(
            self._conn.execute(select(func.pg_try_advisory_lock(self._key))).scalar()
        )
        self._conn.commit()
        return self._acquired

    def __exit__(self, *exc) -> None:
        try:
            if self._acquired:
                self._conn.execute(select(func.pg_advisory_unlock(self._key)))
                self._conn.commit()
        finally:
            self._conn.close()


class Cleaner:
    def __init__(
        self,
        config: CleanerConfig,
        *,
        session_factory,
        rwms,
        sleep=asyncio.sleep,
        clock: Callable[[], datetime] = utc_now,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        self.config = config
        self._session_factory = session_factory
        self._rwms = rwms
        self._sleep = sleep
        self._clock = clock
        self._monotonic = monotonic
        self._rules = config.rule_config()
        self.summary = CleanupSummary()
        self.rows: list[ReportRow] = []
        self._default_squads: frozenset[str] = frozenset()
        self._inactive_days: Optional[int] = None
        # Внешний отряд FREE (free_tier_external_squad_uuid) — признак
        # бесплатного тарифа в панели (тег подписки принадлежит владельцу).
        self._free_squad_uuid: Optional[str] = None
        self._state = DisabledState(Path(config.state_path or DEFAULT_STATE_PATH))
        self._state_locked = False
        # Двухфазное удаление: uuid → итог фазы 1; строки отчёта по uuid;
        # uuid, которые включить обратно не удалось (пока не удалось).
        self._phase1: dict[str, Phase1] = {}
        self._rows_by_uuid: dict[str, ReportRow] = {}
        self._unresolved: set[str] = set()
        self._last_reenable_result: Optional[str] = None

    # --- решение -----------------------------------------------------------

    def _decide(self, panel, facts, now: datetime) -> Decision:
        if self.config.mode == MODE_FREE_INACTIVE:
            return decide_free_inactive(
                panel,
                facts,
                self._rules,
                now,
                self._inactive_days,
                self._default_squads,
                self._free_squad_uuid,
            )
        return decide_expired(
            panel,
            facts,
            self._rules,
            now,
            self._default_squads,
            self._free_squad_uuid,
        )

    @staticmethod
    def _phase2_view(
        panel: PanelSnapshot, phase1: Optional[Phase1]
    ) -> tuple[Optional[str], PanelSnapshot]:
        """Фаза 2: подписка обязана быть DISABLED (иначе её включил кто-то
        другой — платёж, админ) и без следов активности с момента фазы 1.
        Решение принимается по снимку со статусом ACTIVE — каким он был до
        нашего DISABLED. Возвращает (причина пропуска | None, снимок для
        решения)."""
        if phase1 is None:
            return None, panel
        if panel.status != PANEL_STATUS_DISABLED:
            return "status_changed_since_disable", panel
        if panel.activity_fingerprint() != phase1.fingerprint:
            return "activity_since_disable", panel
        return None, dataclasses.replace(panel, status=PANEL_STATUS_ACTIVE)

    @property
    def _archive_reason(self) -> str:
        if self.config.mode == MODE_FREE_INACTIVE:
            return ARCHIVE_REASON_FREE_INACTIVE
        return ARCHIVE_REASON_EXPIRED

    @property
    def _tier_label(self) -> str:
        return self.config.tier if self.config.mode == MODE_EXPIRED else "-"

    # --- запуск ------------------------------------------------------------

    def _load_settings(self) -> bool:
        """Runtime-настройки из system_settings. False — режим не готов.

        free_inactive без внешнего отряда FREE (free_tier_external_squad_uuid)
        отказывается работать: ERROR в лог, exit code EXIT_CONFIG — отбирать
        бесплатные подписки не по чему (по тегу нельзя: он принадлежит
        владельцу). free_tier_inactive_days меньше MIN_FREE_INACTIVE_DAYS
        поднимается до него (WARNING в лог)."""
        keys = (BOT_INTERNAL_SQUADS_UUIDS_SETTING,) + tuple(FREE_TIER_SETTING_KEYS)
        with self._session_factory() as session:
            values = load_system_settings(session, keys)
        self._default_squads = _csv_set(values.get(BOT_INTERNAL_SQUADS_UUIDS_SETTING))
        settings = free_tier_settings_from(values)
        self._free_squad_uuid = settings.external_squad_uuid
        if self.config.mode != MODE_FREE_INACTIVE:
            return True
        if not settings.external_squad_uuid:
            logger.error(
                "free_inactive mode refused: the FREE external squad "
                "(system_settings.free_tier_external_squad_uuid) is not configured, "
                "free subscriptions cannot be selected; nothing scanned or deleted"
            )
            self.summary.stop_reason = "free_squad_not_configured"
            self.summary.exit_code = EXIT_CONFIG
            return False
        if not settings.ready:
            logger.warning(
                "free_inactive mode skipped: free tier is not enabled/configured"
            )
            return False
        effective = effective_free_inactive_days(settings.inactive_days)
        if effective != settings.inactive_days:
            logger.warning(
                "free_tier_inactive_days is below the hard floor, clamped %s",
                kv(
                    configured=settings.inactive_days,
                    effective=effective,
                    min_free_inactive_days=MIN_FREE_INACTIVE_DAYS,
                ),
            )
        self._inactive_days = effective
        return True

    def _payment_schema_ready(self) -> bool:
        """Все таблицы платёжных фактов читаются (rules.PAYMENT_FACT_MODELS).
        Иначе платёжное состояние неоднозначно (например, миграция
        cp_payments / cp_recurrent_payments ещё не применена: оплаты и
        автоплатежи CloudPayments не видны) — ни перебора, ни удалений, ни
        отключений в обоих режимах и в dry-run; exit code EXIT_AUTO_STOP."""
        with self._session_factory() as session:
            problems = payment_schema_problems(session)
        if not problems:
            return True
        logger.critical(
            "AUTO-STOP: payment state is ambiguous, payment tables are not "
            "readable (migration not applied?); nothing scanned, disabled or "
            "deleted %s",
            kv(tables=",".join(problems)),
        )
        self.summary.stop_reason = "payment_schema_unreadable"
        self.summary.exit_code = EXIT_AUTO_STOP
        return False

    def _report_path(self, started_at: datetime) -> Path:
        if self.config.report_path:
            return Path(self.config.report_path)
        stamp = started_at.strftime("%Y%m%dT%H%M%SZ")
        return Path("report") / f"rw-cleaner-{self.config.mode}-{stamp}.csv"

    async def run(self) -> CleanupSummary:
        started_at = self._clock()
        cfg = self.config
        logger.info(
            "rw cleaner started %s",
            kv(
                mode=cfg.mode,
                tier=self._tier_label,
                execute=cfg.execute,
                grace_days=cfg.grace_days,
                expire_tolerance_hours=cfg.expire_tolerance_hours,
                recent_payment_days=cfg.recent_payment_days,
                max_deletions=cfg.max_deletions,
                free_settle_seconds=cfg.free_settle_seconds,
                uncertain_confirm_seconds=cfg.confirm_bound,
                uncertain_quarantine_hours=cfg.uncertain_quarantine_hours,
                state_path=str(self._state.path),
                full_scan=cfg.full_scan,
                allowlist_size=len(cfg.username_allowlist),
            ),
        )
        try:
            if cfg.clear_uncertain:
                self._clear_uncertain()
                return self.summary
            if not await self._recover_disabled():
                return self.summary
            if not self._load_settings():
                return self.summary
            if not self._payment_schema_ready():
                return self.summary
            try:
                planned = await self._scan(started_at)
            except PanelScanError as e:
                logger.error("panel scan failed, nothing deleted: %s", e)
                self.summary.stop_reason = "panel_scan_failed"
                self.summary.exit_code = EXIT_PANEL_SCAN_FAILED
                return self.summary
            if not self._guards_pass():
                self.summary.exit_code = EXIT_AUTO_STOP
                return self.summary
            await self._execute(planned)
            return self.summary
        finally:
            if self._state_locked:
                self._state.release()
                self._state_locked = False
            if (
                not cfg.execute
                and self.summary.uncertain_pending
                and self.summary.exit_code == EXIT_OK
            ):
                # Dry-run ничего не трогает, но человек всё равно нужен.
                self.summary.exit_code = EXIT_UNCERTAIN
            self._finish(started_at)

    def _acquire_state(self) -> bool:
        """flock + чтение файла состояния (только --execute). False — запуск
        прекращается (занят — код 3, не читается — код 6)."""
        try:
            acquired = self._state.acquire()
        except OSError as e:
            acquired = False
            logger.error(
                "disabled-state lock failed %s",
                kv(path=str(self._state.path), error=type(e).__name__),
            )
        if not acquired:
            logger.error(
                "disabled-state file is locked by another rw-cleaner process %s",
                kv(path=str(self._state.path)),
            )
            self.summary.stop_reason = "state_locked"
            self.summary.exit_code = EXIT_LOCKED
            return False
        self._state_locked = True
        try:
            self._state.load()
        except StateError as e:
            logger.critical(
                "disabled-state file unreadable: subscriptions disabled by a previous "
                "run may stay DISABLED and uncertain panel mutations are unknown, "
                "manual review needed; nothing done %s",
                kv(path=str(self._state.path), error=str(e)),
            )
            self.summary.stop_reason = "state_unreadable"
            self.summary.exit_code = EXIT_UNCERTAIN
            return False
        return True

    async def _recover_disabled(self) -> bool:
        """До всего остального: перепроверка записей uncertain (запоздавший
        DISABLE — включить обратно) и обратное включение подписок, которые
        прошлый (упавший) запуск перевёл в DISABLED и не довёл до конца.
        Dry-run ничего не меняет — только ERROR в лог. False — запуск
        прекращается (файл занят/не читается, включить не удалось или
        остались записи uncertain — ничего нового не отключаем и не
        удаляем)."""
        if not self.config.execute:
            self._report_state_dry_run()
            return True
        if not self._acquire_state():
            return False
        uncertain_left = await self._recheck_uncertain()
        pending = self._state.entries
        if pending:
            logger.warning(
                "re-enabling subscriptions left DISABLED by a previous run %s",
                kv(count=len(pending)),
            )
        for entry in pending:
            if await self._reenable(entry, cause="recovery"):
                self.summary.recovered += 1
        self.summary.reenable_failed = len(self._unresolved)
        if uncertain_left:
            logger.critical(
                "STOP: uncertain panel mutations are pending (a DELETE/DISABLE "
                "accepted by the panel may still land); this run re-checked them "
                "and does not disable or delete anything new. Investigate, then "
                "wait for the quarantine or use --clear-uncertain %s",
                kv(
                    count=uncertain_left,
                    panel_uuids=",".join(e.panel_uuid for e in self._state.uncertain),
                    quarantine_hours=self.config.uncertain_quarantine_hours,
                ),
            )
            self.summary.stop_reason = "uncertain_pending"
            self.summary.exit_code = EXIT_UNCERTAIN
            return False
        if self._unresolved:
            logger.critical(
                "AUTO-STOP: subscriptions disabled by a previous run could not be "
                "re-enabled, nothing else done %s",
                kv(count=len(self._unresolved)),
            )
            self.summary.stop_reason = "pending_reenable_failed"
            self.summary.exit_code = EXIT_UNCERTAIN
            return False
        return True

    def _report_state_dry_run(self) -> None:
        try:
            pending = self._state.load()
        except StateError as e:
            logger.error(
                "disabled-state file unreadable (dry-run, nothing changed) %s",
                kv(path=str(self._state.path), error=str(e)),
            )
            return
        if pending:
            logger.error(
                "subscriptions disabled by a previous crashed run are pending "
                "re-enable; dry-run does not touch them, run with --execute %s",
                kv(
                    count=len(pending),
                    usernames=",".join(e.username for e in pending),
                ),
            )
        uncertain = self._state.uncertain
        self.summary.uncertain_pending = len(uncertain)
        for record in uncertain:
            logger.error(
                "uncertain panel mutation pending (dry-run, not re-checked); "
                "--execute runs refuse new disables/deletions until it is "
                "resolved %s",
                self._uncertain_fields(record),
            )

    @staticmethod
    def _uncertain_fields(record: UncertainEntry, **extra) -> str:
        return kv(
            op=record.op,
            state=record.state,
            username=record.username,
            panel_uuid=record.panel_uuid,
            user_id=record.user_id,
            archive_id=record.archive_id,
            started_at=record.started_at,
            last_checked_at=record.last_checked_at,
            last_observed=record.last_observed,
            **extra,
        )

    # --- записи uncertain --------------------------------------------------

    async def _observe(self, username: str, panel_uuid: str) -> str:
        """Что панель показывает по подписке прямо сейчас (строгое
        перечитывание): статус, GONE, UUID_CHANGED или UNREADABLE."""
        try:
            fresh = await self._rwms.get_user_by_username_strict(
                username, timeout=self.config.rwms_timeout_seconds
            )
        except Exception:  # noqa: BLE001 - панель не ответила
            return "UNREADABLE"
        if fresh is None:
            return "GONE"
        if fresh.uuid != panel_uuid:
            return "UUID_CHANGED"
        return _status_name(fresh) or "UNKNOWN"

    async def _recheck_uncertain(self) -> int:
        """Перепроверка всех записей uncertain. Возвращает, сколько осталось
        (> 0 — запуск ничего нового не отключает и не удаляет)."""
        records = self._state.uncertain
        if not records:
            return 0
        logger.critical(
            "uncertain panel mutations found, re-checking; no new disable/delete "
            "in this run while any remains %s",
            kv(count=len(records)),
        )
        for record in records:
            try:
                await self._recheck_one_uncertain(record)
            except Exception as e:  # noqa: BLE001 - запись остаётся
                logger.error(
                    "uncertain re-check failed, record kept %s",
                    self._uncertain_fields(record, error=type(e).__name__),
                )
        left = len(self._state.uncertain)
        self.summary.uncertain_pending = left
        return left

    async def _recheck_one_uncertain(self, record: UncertainEntry) -> None:
        now = self._clock()
        if record.pending:
            # Упреждающая запись пережила запуск: процесс убит/отменён во
            # время запроса (или сразу после) — исход неизвестен, ровно как
            # у uncertain.
            logger.critical(
                "write-ahead record left PENDING by an interrupted run: the "
                "request may have reached the panel, treated as uncertain %s",
                self._uncertain_fields(record),
            )
        observed = await self._observe(record.username, record.panel_uuid)
        if record.op == OP_DISABLE:
            safe, observed = await self._recheck_uncertain_disable(record, observed)
        else:
            safe = self._recheck_uncertain_delete(record, observed, now)
        started = parse_iso_utc(record.started_at)
        quarantine = timedelta(hours=self.config.uncertain_quarantine_hours)
        expired = started is not None and now - started >= quarantine
        if started is None:
            logger.error(
                "uncertain record has no valid started_at, it is never dropped "
                "automatically; use --clear-uncertain after review %s",
                self._uncertain_fields(record),
            )
        if expired and safe:
            try:
                self._state.remove_uncertain(record.panel_uuid)
            except OSError as e:
                logger.error(
                    "uncertain record drop failed, kept %s",
                    self._uncertain_fields(record, error=type(e).__name__),
                )
                return
            self.summary.uncertain_dropped += 1
            logger.info(
                "uncertain record dropped after quarantine %s",
                self._uncertain_fields(record, observed_now=observed),
            )
            return
        updated = dataclasses.replace(
            record,
            last_checked_at=iso_utc(now),
            last_observed=observed,
            state=STATE_UNCERTAIN,
        )
        try:
            self._state.put_uncertain(updated)
        except OSError as e:
            logger.error(
                "uncertain record update failed, old record kept %s",
                self._uncertain_fields(record, error=type(e).__name__),
            )
        logger.warning(
            "uncertain record kept %s",
            self._uncertain_fields(
                updated,
                quarantine_passed=expired,
                safe_to_drop=safe,
            ),
        )

    async def _recheck_uncertain_disable(
        self, record: UncertainEntry, observed: str
    ) -> tuple[bool, str]:
        """Неопределённый DISABLE. Панель показывает DISABLED — запоздавший
        запрос дошёл: включаем обратно под блокировкой users (если нет
        user_blocks — как при любом обратном включении). Новых удалений в
        этом запуске нет (записи uncertain их запрещают), так что подписка
        точно не из тех, что вот-вот удаляются. ACTIVE (и любой другой
        статус, кроме DISABLED) — запись остаётся до конца карантина: ACTIVE
        не доказывает, что старый запрос не дойдёт позже. Возвращает
        (можно ли снять после карантина, наблюдение)."""
        if observed == "UNREADABLE":
            return False, observed
        if observed != PANEL_STATUS_DISABLED:
            logger.warning(
                "uncertain DISABLE: subscription is not DISABLED now, kept until "
                "the quarantine ends %s",
                self._uncertain_fields(record, observed_now=observed),
            )
            return True, observed
        logger.critical(
            "uncertain DISABLE landed late: subscription is DISABLED, re-enabling %s",
            self._uncertain_fields(record, observed_now=observed),
        )
        entry = DisabledEntry(
            panel_uuid=record.panel_uuid,
            username=record.username,
            user_id=record.user_id,
            disabled_at=record.started_at,
        )
        resolved = await self._reenable(entry, cause="uncertain_disable_late")
        result = self._last_reenable_result
        if not resolved:
            return False, observed
        if result == "reenabled":
            self.summary.uncertain_reenabled += 1
            return True, PANEL_STATUS_ACTIVE
        if result == "kept_disabled_blocked":
            # Статусом владеет админская блокировка (user_blocks).
            return True, "DISABLED_BLOCKED"
        return True, observed

    def _recheck_uncertain_delete(
        self, record: UncertainEntry, observed: str, now: datetime
    ) -> bool:
        """Неопределённый DELETE. Подписки нет — запрос дошёл (или её удалил
        кто-то ещё): у активного в БД пользователя с открытым архивом её
        вернёт сверка user-notify (heal), без архива — нужен человек; у
        неактивного удаление и было целью. Подписка на месте — ждём конца
        карантина. Возвращает, можно ли снять запись после карантина."""
        if observed == "UNREADABLE":
            return False
        if observed not in ("GONE", "UUID_CHANGED"):
            logger.warning(
                "uncertain DELETE: subscription is still in the panel, kept until "
                "the quarantine ends %s",
                self._uncertain_fields(record, observed_now=observed),
            )
            return True
        with self._session_factory() as session:
            expire_at = session.execute(
                select(User.expire_at).where(User.id == record.user_id)
            ).scalar()
            # Только чтение (без FOR UPDATE open_archive_query): не мешаем
            # идущему восстановлению.
            open_archive_id = session.execute(
                select(SubscriptionArchive.id)
                .where(SubscriptionArchive.username == record.username)
                .where(SubscriptionArchive.restored_at.is_(None))
            ).scalar()
            session.rollback()
        db_active = expire_at is not None and expire_at > naive_utc(now)
        fields = self._uncertain_fields(
            record,
            observed_now=observed,
            db_active=db_active,
            open_archive_id=open_archive_id,
        )
        if not db_active:
            logger.warning(
                "uncertain DELETE landed: subscription gone, user is not active in "
                "the DB (the deletion was intended) %s",
                fields,
            )
        elif open_archive_id is not None:
            logger.error(
                "uncertain DELETE landed for a DB-active user: user-notify heal "
                "restores it from the open archive (same link/keys) - verify %s",
                fields,
            )
        else:
            logger.critical(
                "uncertain DELETE landed for a DB-active user WITHOUT an open "
                "archive: manual restore needed %s",
                fields,
            )
        return True

    def _write_ahead(self, record: UncertainEntry) -> bool:
        """Упреждающая запись (state=pending, fsync) ДО мутирующего запроса
        к панели. False — записать не удалось: запрос НЕ отправлять."""
        try:
            self._state.put_uncertain(record)
        except Exception as e:  # noqa: BLE001 - без записи запрос не уходит
            logger.error(
                "write-ahead record NOT persisted, request not sent %s",
                self._uncertain_fields(record, error=type(e).__name__),
            )
            return False
        logger.info("write-ahead record persisted %s", self._uncertain_fields(record))
        return True

    def _mark_uncertain(self, record: UncertainEntry) -> None:
        """pending → uncertain, как только исход стал неизвестен (до
        перечитываний). Не записалось — в файле остаётся pending, а его
        следующий запуск разбирает точно так же."""
        try:
            self._state.put_uncertain(
                dataclasses.replace(record, state=STATE_UNCERTAIN)
            )
        except Exception as e:  # noqa: BLE001 - pending равнозначен uncertain
            logger.error(
                "uncertain mark failed, the write-ahead record stays pending "
                "(treated as uncertain) %s",
                self._uncertain_fields(record, error=type(e).__name__),
            )

    def _drop_uncertain(self, panel_uuid: str) -> bool:
        """Исход подтвердился — запись не нужна. False — убрать не удалось:
        запись остаётся (в памяти тоже) — новые изменения панели в этом
        запуске запрещены, следующий запуск её перепроверит; код 6."""
        try:
            self._state.remove_uncertain(panel_uuid)
        except Exception as e:  # noqa: BLE001 - лишняя запись безопасна
            logger.critical(
                "write-ahead record removal failed after a confirmed outcome; "
                "this run stops mutating, the next run re-checks it %s",
                kv(panel_uuid=panel_uuid, error=type(e).__name__),
            )
            self._state_write_stop(EXIT_UNCERTAIN)
            return False
        return True

    def _state_write_stop(self, exit_code: int) -> str:
        """Файл состояния не пишется: дальше в этом запуске ничего не
        отключаем и не удаляем."""
        if not self.summary.stop_reason:
            self.summary.stop_reason = "state_write_failed"
        if self.summary.exit_code in (EXIT_OK, EXIT_FAILURES):
            self.summary.exit_code = exit_code
        return "stop"

    def _interrupted(self, row: ReportRow, record: UncertainEntry, error) -> None:
        """Отмена задачи / KeyboardInterrupt / SystemExit во время
        мутирующего запроса (или ожидания его подтверждения): исход
        неизвестен — запись uncertain (не записалась — остаётся pending),
        счётчики и код 6; вызывающий пробрасывает исключение дальше."""
        self._mark_uncertain(record)
        if record.op == OP_DELETE:
            self.summary.delete_uncertain += 1
        else:
            self.summary.disable_uncertain += 1
        row.action = f"failed:{record.op}_interrupted"
        self.summary.stop_reason = "interrupted"
        self.summary.exit_code = EXIT_UNCERTAIN
        logger.critical(
            "INTERRUPTED during a panel mutation: outcome unknown, kept as an "
            "uncertain record; next --execute runs only re-check it %s",
            self._uncertain_fields(record, error=type(error).__name__),
        )

    def _clear_uncertain(self) -> None:
        """--clear-uncertain: снять записи после ручного разбора и выйти, ничего
        больше не делая (кто и когда — в лог)."""
        if not self.config.execute:  # parse_args не пропустит; страховка
            self.summary.exit_code = EXIT_CONFIG
            self.summary.stop_reason = "clear_uncertain_requires_execute"
            return
        if not self._acquire_state():
            return
        self.summary.stop_reason = "clear_uncertain"
        cleared_by = operator_name()
        cleared_at = iso_utc(self._clock())
        for panel_uuid in self.config.clear_uncertain:
            try:
                removed = self._state.remove_uncertain(panel_uuid)
            except OSError as e:
                logger.critical(
                    "uncertain record clear failed (state write) %s",
                    kv(panel_uuid=panel_uuid, error=type(e).__name__),
                )
                self.summary.exit_code = EXIT_UNCERTAIN
                continue
            if removed is None:
                logger.error(
                    "no uncertain record for this panel uuid, nothing cleared %s",
                    kv(panel_uuid=panel_uuid),
                )
                if self.summary.exit_code == EXIT_OK:
                    self.summary.exit_code = EXIT_CONFIG
                continue
            self.summary.uncertain_cleared += 1
            logger.warning(
                "uncertain record cleared by operator %s",
                self._uncertain_fields(
                    removed, cleared_by=cleared_by, cleared_at=cleared_at
                ),
            )
        self.summary.uncertain_pending = len(self._state.uncertain)

    async def _scan(self, now: datetime) -> list[ReportRow]:
        cfg = self.config
        status = proto.UserStatus.EXPIRED if cfg.mode == MODE_EXPIRED else None
        external_squad_uuid = None
        if cfg.mode == MODE_FREE_INACTIVE:
            if not self._free_squad_uuid:
                # _load_settings уже отказал бы; защита от перебора всей панели.
                raise PanelScanError("free_inactive scan without the FREE external squad")
            external_squad_uuid = self._free_squad_uuid
        planned: list[ReportRow] = []
        async for user in iter_panel_users(
            self._rwms,
            page_size=cfg.page_size,
            timeout=cfg.stream_timeout_seconds,
            status=status,
            external_squad_uuid=external_squad_uuid,
        ):
            panel = snapshot_from_proto(user)
            if cfg.username_allowlist and panel.username not in cfg.username_allowlist:
                self.summary.skip_reasons["not_in_allowlist"] += 1
                continue
            self.summary.scanned += 1
            with self._session_factory() as session:
                facts = load_db_facts(session, panel, now, cfg.recent_payment_days)
            decision = self._decide(panel, facts, now)
            row = ReportRow(panel, facts, decision, cfg.mode, self._tier_label)
            self.rows.append(row)
            if facts.orphan:
                self.summary.orphans += 1
            if not decision.candidate:
                self.summary.skip_reasons[decision.reason] += 1
                logger.info(
                    "skip %s",
                    kv(
                        username=panel.username,
                        panel_uuid=panel.uuid,
                        reason=decision.reason,
                    ),
                )
                continue
            self.summary.candidates += 1
            if len(planned) < cfg.max_deletions:
                row.action = "planned"
                planned.append(row)
                logger.warning("candidate %s", row.log_line())
            else:
                row.action = "over_cap"
                self.summary.over_cap += 1
                logger.info("candidate over cap %s", row.log_line())
            if (
                not cfg.full_scan
                and len(planned) >= cfg.max_deletions
                and self.summary.scanned >= cfg.ratio_min_sample
            ):
                logger.info("max_deletions reached, scan stopped early")
                break
        self.summary.planned = len(planned)
        return planned

    def _guards_pass(self) -> bool:
        cfg = self.config
        scanned = self.summary.scanned
        if scanned == 0:
            return True
        enough = scanned >= cfg.ratio_min_sample
        orphan_ratio = self.summary.orphans / scanned
        if enough and orphan_ratio > cfg.max_orphan_ratio:
            self.summary.stop_reason = "orphan_ratio"
            logger.critical(
                "AUTO-STOP: too many panel users without a users row %s",
                kv(
                    orphans=self.summary.orphans,
                    scanned=scanned,
                    max_orphan_ratio=_ratio(cfg.max_orphan_ratio),
                ),
            )
            return False
        candidate_ratio = self.summary.candidates / scanned
        if cfg.mode == MODE_FREE_INACTIVE:
            ratio_setting, max_ratio = (
                "max_free_candidate_ratio",
                cfg.max_free_candidate_ratio,
            )
        else:
            ratio_setting, max_ratio = "max_candidate_ratio", cfg.max_candidate_ratio
        if enough and candidate_ratio > max_ratio:
            self.summary.stop_reason = "candidate_ratio"
            logger.critical(
                "AUTO-STOP: candidate share is suspiciously high %s",
                kv(
                    mode=cfg.mode,
                    candidates=self.summary.candidates,
                    scanned=scanned,
                    candidate_ratio=_ratio(candidate_ratio),
                    **{ratio_setting: _ratio(max_ratio)},
                ),
            )
            return False
        if not enough:
            logger.info(
                "ratio guards use a small sample %s",
                kv(scanned=scanned, ratio_min_sample=cfg.ratio_min_sample),
            )
        return True

    async def _execute(self, planned: list[ReportRow]) -> None:
        cfg = self.config
        if not cfg.execute:
            for row in planned:
                row.action = "dry_run"
                self.summary.dry_run += 1
                logger.warning("dry-run candidate %s", row.log_line())
            return
        if self._mutations_blocked():
            return
        if cfg.mode == MODE_FREE_INACTIVE:
            await self._execute_two_phase(planned)
        else:
            await self._run_batch(planned, self._process)
        s = self.summary
        s.reenable_failed = len(self._unresolved)
        if s.delete_uncertain or s.disable_uncertain or s.reenable_failed:
            s.exit_code = EXIT_UNCERTAIN
        elif s.exit_code == EXIT_OK and (s.failed or s.archive_mismatch):
            s.exit_code = EXIT_FAILURES

    def _mutations_blocked(self) -> bool:
        """Есть запись uncertain — ни одного нового DISABLE/DELETE (запоздавший
        запрос ещё может дойти до панели). Страховка: _recover_disabled уже
        остановил бы запуск, а неопределённый исход останавливает партию."""
        if not self._state.uncertain:
            return False
        if self.summary.exit_code == EXIT_OK:
            self.summary.exit_code = EXIT_UNCERTAIN
        if not self.summary.stop_reason:
            self.summary.stop_reason = "uncertain_pending"
        logger.critical(
            "new disables/deletions refused: uncertain panel mutations pending %s",
            kv(count=len(self._state.uncertain)),
        )
        return True

    async def _run_batch(self, rows: list[ReportRow], step) -> bool:
        """Кандидаты по одному с паузой DELETE_PAUSE_SECONDS. False — запуск
        остановлен (серия ошибок, неопределённый DELETE/DISABLE)."""
        cfg = self.config
        consecutive_failures = 0
        for index, row in enumerate(rows):
            if self._mutations_blocked():
                return False
            if index and cfg.delete_pause_seconds > 0:
                await self._sleep(cfg.delete_pause_seconds)
            outcome = await step(row)
            if outcome in ("uncertain", "stop"):
                return False
            if outcome in ("deleted", "disabled"):
                consecutive_failures = 0
            elif outcome == "failed":
                consecutive_failures += 1
                if consecutive_failures >= cfg.max_consecutive_failures:
                    self.summary.stop_reason = "consecutive_failures"
                    self.summary.exit_code = EXIT_AUTO_STOP
                    logger.critical(
                        "AUTO-STOP: consecutive failures %s",
                        kv(failures=consecutive_failures),
                    )
                    return False
        return True

    # --- двухфазное удаление (free_inactive) -------------------------------

    async def _execute_two_phase(self, planned: list[ReportRow]) -> None:
        """Фаза 1 — DISABLED всей партии; пауза один раз; фаза 2 — удаление
        или обратное включение. Что бы ни случилось (остановка, исключение,
        отмена), всё отключённое этим запуском и не удалённое включается
        обратно в finally."""
        cfg = self.config
        disabled_rows: list[ReportRow] = []

        async def phase1(row: ReportRow) -> str:
            outcome = await self._disable_free(row)
            if outcome == "disabled":
                disabled_rows.append(row)
            return outcome

        try:
            proceed = await self._run_batch(planned, phase1)
            if proceed and disabled_rows:
                logger.warning(
                    "phase 1 done, waiting for panel stats to settle %s",
                    kv(disabled=len(disabled_rows), settle_seconds=cfg.free_settle_seconds),
                )
                await self._sleep(cfg.free_settle_seconds)
                await self._run_batch(disabled_rows, self._phase2)
        finally:
            await self._reenable_leftovers()

    async def _reenable_leftovers(self) -> None:
        # Здесь только подписки с ПОДТВЕРЖДЁННЫМ DISABLE (запись entries
        # появляется лишь после подтверждения); неопределённые DISABLE ведёт
        # запись uncertain.
        for entry in list(self._state.entries):
            row = self._rows_by_uuid.get(entry.panel_uuid)
            if row is not None and row.action == "disabled":
                row.action = f"not_run:{self.summary.stop_reason or 'stopped'}"
            await self._reenable(entry, cause=row.action if row else "leftover")

    async def _disable_free(self, row: ReportRow) -> str:
        """Фаза 1 для одного кандидата: строгое перечитывание и решение без
        блокировки, затем под блокировкой users — ещё раз и UpdateUser
        (только status=DISABLED)."""
        cfg = self.config
        username = row.panel.username
        try:
            fresh = await self._rwms.get_user_by_username_strict(
                username, timeout=cfg.rwms_timeout_seconds
            )
        except RwmsUnavailableError:
            return self._fail(row, "panel_reread_failed")
        if fresh is None:
            return self._skip(row, "gone_from_panel")
        if fresh.uuid != row.panel.uuid:
            return self._fail(row, "panel_uuid_changed")
        fresh_panel = snapshot_from_proto(fresh)
        now = self._clock()
        with self._session_factory() as session:
            facts = load_db_facts(session, fresh_panel, now, cfg.recent_payment_days)
            session.rollback()
        decision = self._decide(fresh_panel, facts, now)
        if not decision.candidate:
            return self._skip(row, f"no_longer_eligible:{decision.reason}")
        if facts.orphan:
            return self._skip(row, "orphan_never_deleted")
        if facts.user_id != row.facts.user_id:
            return self._fail(row, "user_mapping_changed")
        row.panel, row.facts, row.decision = fresh_panel, facts, decision
        return await self._disable_under_lock(row, facts.user_id)

    async def _disable_under_lock(self, row: ReportRow, user_id: int) -> str:
        cfg = self.config
        username = row.panel.username
        uuid = row.panel.uuid
        with self._session_factory() as session:
            try:
                outcome = await self._recheck_under_lock(session, row, user_id)
            except Exception as e:  # noqa: BLE001 - любая ошибка = не отключать
                session.rollback()
                logger.error(
                    "locked recheck failed %s",
                    kv(username=username, error=type(e).__name__),
                )
                return self._fail(row, "locked_recheck_failed")
            if outcome is not None:
                session.rollback()
                return outcome
            started_at = iso_utc(self._clock())
            entry = DisabledEntry(
                panel_uuid=uuid,
                username=username,
                user_id=user_id,
                disabled_at=started_at,
            )
            record = UncertainEntry(
                op=OP_DISABLE,
                panel_uuid=uuid,
                username=username,
                user_id=user_id,
                started_at=started_at,
                state=STATE_PENDING,
            )
            # Упреждающая запись — в файл ДО UpdateUser: процесс убит или
            # задача отменена во время запроса — следующий запуск увидит
            # pending и разберёт её как uncertain (запоздавший DISABLE —
            # включит обратно). Не записалась — не отключаем.
            if not self._write_ahead(record):
                session.rollback()
                self._fail(row, "state_write_failed")
                return self._state_write_stop(EXIT_FAILURES)
            self._rows_by_uuid[uuid] = row
            confirmed_by = None
            verdict = None
            try:
                try:
                    response = await self._rwms.update_user(
                        proto.UpdateUserRequest(
                            uuid=uuid, status=proto.UserStatus.DISABLED
                        ),
                        timeout=cfg.rwms_timeout_seconds,
                    )
                except Exception as e:  # noqa: BLE001 - исход неизвестен
                    logger.error(
                        "disable rpc raised %s",
                        kv(username=username, error=type(e).__name__),
                    )
                    response = None
                if (
                    response is not None
                    and response.uuid == uuid
                    and _status_name(response) == PANEL_STATUS_DISABLED
                ):
                    confirmed_by = "rpc"
                else:
                    # Исход неизвестен: запоздавший UpdateUser может дойти
                    # позже. Запись → uncertain сразу, потом ждём ответа
                    # панели, не отпуская блокировку users.
                    logger.error(
                        "disable outcome unknown, confirming under the lock %s",
                        kv(
                            username=username,
                            panel_uuid=uuid,
                            response="none" if response is None else "unexpected",
                        ),
                    )
                    self._mark_uncertain(record)
                    verdict = await self._confirm(row, self._disable_verdict)
                    if verdict == "disabled":
                        confirmed_by = "reread"
            except BaseException as e:  # CancelledError, KeyboardInterrupt, ...
                self._interrupted(row, record, e)
                raise
            converted = False
            if confirmed_by is not None:
                # Подтверждён: упреждающая запись → запись об отключении
                # (одной атомарной записью файла), дальше её ведёт фаза 2.
                converted = self._convert_disable(entry)
            try:
                # Транзакция только читала — COMMIT лишь снимает блокировку.
                session.commit()
            except Exception as e:  # noqa: BLE001 - снятие блокировки
                session.rollback()
                logger.error(
                    "lock release commit failed %s",
                    kv(username=username, error=type(e).__name__),
                )
        if confirmed_by is None:
            if verdict == "gone":
                # Подписки с этим uuid больше нет — запоздавшему DISABLE
                # нечего менять.
                self._drop_uncertain(uuid)
                return self._skip(row, "gone_after_disable")
            return self._disable_uncertain(row, entry)
        if not converted:
            # DISABLE подтверждён, но файл не пишется: вести подписку через
            # фазу 2 нельзя — включаем обратно сейчас (блокировка уже снята).
            # Упреждающая запись остаётся (и в памяти — новые изменения в
            # этом запуске запрещены); следующий запуск её перепроверит.
            await self._reenable(entry, cause="state_write_failed")
            self._fail(row, "state_write_failed")
            return self._state_write_stop(EXIT_UNCERTAIN)
        self._phase1[uuid] = Phase1(entry, row.panel.activity_fingerprint())
        row.action = "disabled"
        self.summary.disabled += 1
        logger.warning(
            "disabled (phase 1) %s",
            kv(
                username=username,
                panel_uuid=uuid,
                user_id=user_id,
                confirmed_by=confirmed_by,
            ),
        )
        return "disabled"

    def _convert_disable(self, entry: DisabledEntry) -> bool:
        try:
            self._state.confirm_disable(entry)
        except Exception as e:  # noqa: BLE001 - запись pending остаётся
            logger.critical(
                "DISABLE confirmed but the state file write failed: the "
                "write-ahead record stays, the subscription is re-enabled now %s",
                kv(
                    username=entry.username,
                    panel_uuid=entry.panel_uuid,
                    error=type(e).__name__,
                ),
            )
            return False
        return True

    @staticmethod
    def _disable_verdict(fresh, panel_uuid: str) -> Optional[str]:
        if fresh is None or fresh.uuid != panel_uuid:
            return "gone"
        if _status_name(fresh) == PANEL_STATUS_DISABLED:
            return "disabled"
        return None

    def _disable_uncertain(self, row: ReportRow, entry: DisabledEntry) -> str:
        """UpdateUser(DISABLED) с неизвестным исходом: запись uncertain (или
        pending, если пометить не удалось) уже в файле, записи об отключении
        нет и не было — подписку дальше ведёт запись uncertain; запуск
        останавливается."""
        uuid = entry.panel_uuid
        row.action = "failed:disable_uncertain"
        self.summary.disable_uncertain += 1
        self.summary.stop_reason = "disable_uncertain"
        self.summary.exit_code = EXIT_UNCERTAIN
        logger.critical(
            "DISABLE UNCERTAIN: UpdateUser(DISABLED) outcome unknown and not "
            "confirmed; a late DISABLE may still land. The run is stopped; next "
            "--execute runs only re-check it (re-enable if it lands) until the "
            "quarantine ends %s",
            kv(
                username=entry.username,
                panel_uuid=uuid,
                user_id=entry.user_id,
            ),
        )
        return "uncertain"

    async def _phase2(self, row: ReportRow) -> str:
        uuid = row.panel.uuid
        phase1 = self._phase1[uuid]
        outcome = await self._process(row, phase1)
        if outcome == "deleted":
            self._state_remove(uuid)
            return outcome
        # Не удалено (пропуск, сбой, неопределённый DELETE): подписку (если
        # она ещё на месте) включаем обратно; неопределённый DELETE дальше
        # ведёт его запись uncertain/pending.
        await self._reenable(phase1.entry, cause=row.action)
        return outcome

    def _state_remove(self, uuid: str) -> None:
        try:
            self._state.remove(uuid)
        except Exception as e:  # noqa: BLE001 - запись проверит следующий запуск
            logger.error(
                "disabled-state update failed, the next run re-checks it %s",
                kv(panel_uuid=uuid, error=type(e).__name__),
            )

    async def _reenable(self, entry: DisabledEntry, *, cause: str) -> bool:
        """Включить обратно подписку, которую отключил этот инструмент. Под
        блокировкой users: строгое перечитывание панели; включается, только
        если это та же подписка, она всё ещё DISABLED и у пользователя нет
        админской блокировки (user_blocks). True — вопрос решён (запись
        убирается из файла); False — не удалось (CRITICAL, запись остаётся —
        повтор в конце запуска и при следующем запуске). Запись об отключении
        и служит упреждающей записью для UpdateUser(ACTIVE): она убирается
        только после определённого итога, поэтому прерванное обратное
        включение повторит следующий запуск. Запоздавший ACTIVE ничего не
        отключает и не удаляет (подписка остаётся рабочей), поэтому отдельной
        записи pending у обратного включения нет; остаточный риск — снять
        DISABLED админской блокировки, поставленной между запросом и его
        доставкой, — тот же, что и без падения."""
        result = "error"
        for attempt in range(REENABLE_LOCK_ATTEMPTS):
            retry = False
            with self._session_factory() as session:
                try:
                    locked = session.execute(lock_user_query(entry.user_id)).first()
                except OperationalError as e:
                    session.rollback()
                    if _lock_not_available(e):
                        result = "user_row_locked"
                        retry = attempt + 1 < REENABLE_LOCK_ATTEMPTS
                    else:
                        result = "error"
                    locked = None
                except Exception:  # noqa: BLE001
                    session.rollback()
                    result = "error"
                    locked = None
                else:
                    try:
                        result = await self._reenable_locked(session, entry, locked)
                        session.commit()
                    except Exception as e:  # noqa: BLE001
                        session.rollback()
                        logger.error(
                            "re-enable failed %s",
                            kv(username=entry.username, error=type(e).__name__),
                        )
                        result = "error"
            if not retry:
                break
            await self._sleep(REENABLE_LOCK_RETRY_SECONDS)

        self._last_reenable_result = result
        row = self._rows_by_uuid.get(entry.panel_uuid)
        if row is not None:
            row.reenable = result
        fields = kv(
            username=entry.username,
            panel_uuid=entry.panel_uuid,
            user_id=entry.user_id,
            cause=cause,
            result=result,
        )
        if result not in REENABLE_RESOLVED:
            self._unresolved.add(entry.panel_uuid)
            logger.critical(
                "RE-ENABLE FAILED: subscription disabled by rw-cleaner may stay "
                "DISABLED, retried at the end of the run and by the next run %s",
                fields,
            )
            return False
        self._unresolved.discard(entry.panel_uuid)
        if result == "reenabled":
            self.summary.reenabled += 1
            logger.warning("re-enabled %s", fields)
        elif result == "kept_disabled_blocked":
            self.summary.kept_disabled_blocked += 1
            logger.warning("left DISABLED: admin block (user_blocks) %s", fields)
        else:
            logger.warning("re-enable not needed %s", fields)
        self._state_remove(entry.panel_uuid)
        return True

    async def _reenable_locked(self, session, entry: DisabledEntry, locked) -> str:
        if locked is None or locked.username != entry.username:
            return "user_row_changed"
        try:
            fresh = await self._rwms.get_user_by_username_strict(
                entry.username, timeout=self.config.rwms_timeout_seconds
            )
        except RwmsUnavailableError:
            return "panel_reread_failed"
        if fresh is None:
            return "gone"
        if fresh.uuid != entry.panel_uuid:
            return "panel_uuid_changed"
        if _status_name(fresh) != PANEL_STATUS_DISABLED:
            return "not_disabled"
        blocked = session.execute(
            select(exists().where(UserBlock.user_id == entry.user_id))
        ).scalar()
        if blocked:
            return "kept_disabled_blocked"
        try:
            response = await self._rwms.update_user(
                proto.UpdateUserRequest(
                    uuid=entry.panel_uuid, status=proto.UserStatus.ACTIVE
                ),
                timeout=self.config.rwms_timeout_seconds,
            )
        except Exception:  # noqa: BLE001 - исход неизвестен
            response = None
        if (
            response is None
            or response.uuid != entry.panel_uuid
            or _status_name(response) != PANEL_STATUS_ACTIVE
        ):
            return "reenable_rpc_failed"
        return "reenabled"

    # --- один кандидат -----------------------------------------------------

    def _skip(self, row: ReportRow, reason: str) -> str:
        row.action = f"skipped:{reason}"
        self.summary.skipped_at_execution += 1
        logger.warning(
            "execution skip %s",
            kv(username=row.panel.username, panel_uuid=row.panel.uuid, reason=reason),
        )
        return "skipped"

    def _archive_mismatch(self, row: ReportRow, archive_id) -> str:
        """Открытый архив username не совпадает с подпиской в панели: не
        удаляем и не считаем ошибкой (один застарелый архив не должен
        останавливать каждый запуск через AUTO-STOP) — нужен ручной разбор."""
        row.archive_id = archive_id
        self.summary.archive_mismatch += 1
        logger.critical(
            "open archive does not match the panel subscription, manual review "
            "needed %s",
            kv(
                username=row.panel.username,
                panel_uuid=row.panel.uuid,
                archive_id=archive_id,
            ),
        )
        return self._skip(row, "open_archive_mismatch")

    def _fail(self, row: ReportRow, reason: str) -> str:
        row.action = f"failed:{reason}"
        self.summary.failed += 1
        logger.error(
            "execution failed %s",
            kv(
                username=row.panel.username,
                panel_uuid=row.panel.uuid,
                archive_id=row.archive_id,
                reason=reason,
            ),
        )
        return "failed"

    def _ensure_archive(
        self, session, fresh, user_id: int, now: datetime, panel_status=None
    ):
        """Открытый архив username: существующий (после сбоя прошлого
        запуска — только если совпадает с панелью) или новый с COMMIT.
        panel_status — статус для архива вместо текущего (фаза 2: подписку
        DISABLED сделали мы, до этого она была ACTIVE). Возвращает
        (archive_id, reused)."""
        existing = session.execute(open_archive_query(fresh.username)).scalars().first()
        if existing is not None:
            consistent = existing.user_id == user_id and same_credentials(
                fresh, existing
            )
            archive_id = existing.id
            session.rollback()
            if not consistent:
                raise ArchiveMismatchError(archive_id)
            return archive_id, True
        archive = build_archive(
            fresh, user_id=user_id, reason=self._archive_reason, now=naive_utc(now)
        )
        if panel_status:
            archive.panel_status = panel_status
        session.add(archive)
        session.flush()
        archive_id = archive.id
        session.commit()
        return archive_id, False

    async def _process(self, row: ReportRow, phase1: Optional[Phase1] = None) -> str:
        cfg = self.config
        username = row.panel.username
        try:
            fresh = await self._rwms.get_user_by_username_strict(
                username, timeout=cfg.rwms_timeout_seconds
            )
        except RwmsUnavailableError:
            return self._fail(row, "panel_reread_failed")
        if fresh is None:
            # Подписки уже нет в панели (удалена прошлым запуском после
            # архива или вручную) — делать нечего.
            return self._skip(row, "gone_from_panel")
        if fresh.uuid != row.panel.uuid:
            return self._fail(row, "panel_uuid_changed")

        fresh_panel = snapshot_from_proto(fresh)
        problem, decide_panel = self._phase2_view(fresh_panel, phase1)
        if problem:
            row.panel = fresh_panel
            return self._skip(row, problem)
        now = self._clock()
        archive_id = None
        reused = False
        with self._session_factory() as session:
            facts = load_db_facts(session, decide_panel, now, cfg.recent_payment_days)
            session.rollback()
            decision = self._decide(decide_panel, facts, now)
            if not decision.candidate:
                return self._skip(row, f"no_longer_eligible:{decision.reason}")
            if facts.orphan:
                # Сирот не удаляем никогда (нет users → нет архива и
                # блокировки); правила их и не выбирают — это страховка.
                return self._skip(row, "orphan_never_deleted")
            if facts.user_id != row.facts.user_id:
                return self._fail(row, "user_mapping_changed")
            row.panel, row.facts, row.decision = fresh_panel, facts, decision
            try:
                archive_id, reused = self._ensure_archive(
                    session,
                    fresh,
                    facts.user_id,
                    now,
                    panel_status=PANEL_STATUS_ACTIVE if phase1 else None,
                )
            except ArchiveMismatchError as e:
                return self._archive_mismatch(row, e.args[0])
            except Exception as e:  # noqa: BLE001 - любая ошибка БД = не удалять
                session.rollback()
                logger.error(
                    "archive commit failed %s",
                    kv(username=username, error=type(e).__name__),
                )
                return self._fail(row, "archive_commit_failed")
        row.archive_id = archive_id
        if reused:
            self.summary.archive_reused += 1
        else:
            self.summary.archived += 1
        logger.warning(
            "archived %s",
            kv(
                username=username,
                user_id=facts.user_id,
                archive_id=archive_id,
                archive_reused=reused,
                reason=self._archive_reason,
            ),
        )
        return await self._delete_under_lock(
            row, facts.user_id, archive_id, reused, phase1
        )

    async def _recheck_under_lock(
        self,
        session,
        row: ReportRow,
        user_id: int,
        archive_id: Optional[int] = None,
        phase1: Optional[Phase1] = None,
    ) -> Optional[str]:
        """Повторная проверка внутри транзакции с блокировкой строки users.
        None — всё ещё кандидат (row обновлён свежими данными), иначе —
        итог (skipped/failed), удалять/отключать нельзя. archive_id — архив
        должен совпадать с панелью (перед DeleteUser); phase1 — фаза 2
        двухфазного удаления (подписка обязана остаться DISABLED и без
        активности)."""
        cfg = self.config
        username = row.panel.username
        try:
            locked = session.execute(lock_user_query(user_id)).first()
        except OperationalError as e:
            if _lock_not_available(e):
                return self._skip(row, "user_row_locked")
            raise
        if locked is None or locked.username != username:
            return self._fail(row, "user_row_changed")

        try:
            fresh = await self._rwms.get_user_by_username_strict(
                username, timeout=cfg.rwms_timeout_seconds
            )
        except RwmsUnavailableError:
            return self._fail(row, "panel_reread_failed")
        if fresh is None:
            return self._skip(row, "gone_from_panel")
        if fresh.uuid != row.panel.uuid:
            return self._fail(row, "panel_uuid_changed")

        if archive_id is not None:
            archive = session.get(SubscriptionArchive, archive_id)
            if (
                archive is None
                or archive.restored_at is not None
                or archive.user_id != user_id
                or not same_credentials(fresh, archive)
            ):
                return self._archive_mismatch(row, archive_id)

        fresh_panel = snapshot_from_proto(fresh)
        problem, decide_panel = self._phase2_view(fresh_panel, phase1)
        if problem:
            row.panel = fresh_panel
            return self._skip(row, f"{problem}_locked")
        now = self._clock()
        facts = load_db_facts(session, decide_panel, now, cfg.recent_payment_days)
        decision = self._decide(decide_panel, facts, now)
        if not decision.candidate:
            return self._skip(row, f"no_longer_eligible_locked:{decision.reason}")
        if facts.user_id != user_id:
            return self._fail(row, "user_mapping_changed")
        row.panel, row.facts, row.decision = fresh_panel, facts, decision
        return None

    def _close_aborted_archive(
        self, row: ReportRow, archive_id: int, user_id: int
    ) -> None:
        """Удаление прервано ДО DeleteUser, а архив создан ЭТИМ запуском:
        закрыть его отдельной транзакцией (restored_at=now,
        restored_by="rw-cleaner:aborted"), чтобы рядом с живой подпиской не
        осталось открытого архива (иначе будущий путь «нет в панели —
        создать» мог бы вернуть устаревшие ключи, а следующий запуск упёрся бы
        в open_archive_mismatch). Закрывается только открытый архив этого
        пользователя (FOR UPDATE); уже закрытый/чужой не трогаем. Сбой —
        ERROR в лог, архив остаётся открытым (как до этой правки), итог
        кандидата не меняется."""
        username = row.panel.username
        aborted_by = row.action
        try:
            with self._session_factory() as session:
                archive = session.get(
                    SubscriptionArchive, archive_id, with_for_update=True
                )
                if (
                    archive is None
                    or archive.restored_at is not None
                    or archive.user_id != user_id
                    or archive.username != username
                ):
                    session.rollback()
                    logger.warning(
                        "aborted deletion: archive not closed, it is not open "
                        "anymore or belongs to another user %s",
                        kv(
                            username=username,
                            archive_id=archive_id,
                            aborted_by=aborted_by,
                        ),
                    )
                    return
                # Не common.mark_restored: тот пишет «subscription restored
                # from archive», а подписка не восстанавливалась — её не
                # удаляли. Поля те же (restored_at / restored_by).
                archive.restored_at = naive_utc(self._clock())
                archive.restored_by = ABORTED_ARCHIVE_RESTORED_BY
                session.commit()
        except Exception as e:  # noqa: BLE001 - закрытие необязательно
            self.summary.archive_close_failed += 1
            logger.error(
                "aborted deletion: failed to close the archive created by this "
                "run, it stays open next to the live subscription %s",
                kv(
                    username=username,
                    archive_id=archive_id,
                    aborted_by=aborted_by,
                    error=type(e).__name__,
                ),
            )
            return
        self.summary.archive_closed_aborted += 1
        logger.warning(
            "aborted deletion: archive closed %s",
            kv(
                username=username,
                user_id=user_id,
                archive_id=archive_id,
                aborted_by=aborted_by,
                restored_by=ABORTED_ARCHIVE_RESTORED_BY,
            ),
        )

    async def _delete_under_lock(
        self,
        row: ReportRow,
        user_id: int,
        archive_id: int,
        reused: bool,
        phase1: Optional[Phase1] = None,
    ) -> str:
        """Архив уже закоммичен. Блокируем строку users (FOR UPDATE NOWAIT),
        перечитываем панель и БД, повторяем решение и вызываем DeleteUser,
        не отпуская блокировку. Любая ошибка до DeleteUser — ROLLBACK и без
        удаления.

        Прервано ДО DeleteUser и архив создан этим запуском (не reused) —
        архив закрывается (_close_aborted_archive), кроме случая «подписки уже
        нет в панели» (тогда архив — единственный способ восстановления).
        После вызова DeleteUser архив не закрывается никогда.

        ДО DeleteUser — упреждающая запись (pending, fsync); не записалась —
        DeleteUser не вызывается, запуск останавливается ("stop"). Исход
        подтверждён — запись убирается. Исход DeleteUser неизвестен (None,
        is_deleted=False, исключение) — запись сразу становится uncertain,
        блокировка НЕ снимается, пока строгие перечитывания панели не дадут
        определённый ответ (_confirm, не дольше confirm_bound); не дали —
        "uncertain" (CRITICAL, запись остаётся, запуск останавливается).
        Отмена/KeyboardInterrupt во время запроса или ожидания — запись
        uncertain, исключение пробрасывается."""
        cfg = self.config
        username = row.panel.username
        with self._session_factory() as session:
            try:
                outcome = await self._recheck_under_lock(
                    session, row, user_id, archive_id, phase1
                )
            except Exception as e:  # noqa: BLE001 - любая ошибка = не удалять
                session.rollback()
                logger.error(
                    "locked recheck failed %s",
                    kv(username=username, error=type(e).__name__),
                )
                outcome = self._fail(row, "locked_recheck_failed")
            else:
                if outcome is not None:
                    session.rollback()
            if outcome is not None:
                # Блокировка уже снята (ROLLBACK); DeleteUser не вызывался.
                reason = row.action.split(":", 1)[-1]
                if not reused and reason not in KEEP_ARCHIVE_OPEN_REASONS:
                    self._close_aborted_archive(row, archive_id, user_id)
                return outcome
            started_at = iso_utc(self._clock())
            record = UncertainEntry(
                op=OP_DELETE,
                panel_uuid=row.panel.uuid,
                username=username,
                user_id=user_id,
                started_at=started_at,
                archive_id=archive_id,
                state=STATE_PENDING,
            )
            # Упреждающая запись — в файл ДО DeleteUser: процесс убит или
            # задача отменена во время запроса — следующий запуск увидит
            # pending (= uncertain) и ничего нового не удалит. Не
            # записалась — не удаляем.
            if not self._write_ahead(record):
                session.rollback()
                self._fail(row, "state_write_failed")
                if not reused:
                    self._close_aborted_archive(row, archive_id, user_id)
                return self._state_write_stop(EXIT_FAILURES)
            confirmed_by = None
            try:
                try:
                    response = await self._rwms.delete_user(
                        row.panel.uuid, timeout=cfg.rwms_timeout_seconds
                    )
                except Exception as e:  # noqa: BLE001 - исход неизвестен
                    logger.error(
                        "delete rpc raised %s",
                        kv(username=username, error=type(e).__name__),
                    )
                    response = None
                if response is not None and response.is_deleted:
                    confirmed_by = "rpc"
                else:
                    logger.error(
                        "delete outcome unknown, confirming under the lock %s",
                        kv(
                            username=username,
                            panel_uuid=row.panel.uuid,
                            response=(
                                "none" if response is None else "is_deleted_false"
                            ),
                        ),
                    )
                    # Запись → uncertain сразу (упадём во время ожидания —
                    # она останется), потом ждём ответа панели под
                    # блокировкой.
                    self._mark_uncertain(record)
                    if await self._confirm(row, self._delete_verdict) == "deleted":
                        confirmed_by = "reread"
            except BaseException as e:  # CancelledError, KeyboardInterrupt, ...
                self._interrupted(row, record, e)
                raise
            if confirmed_by is not None:
                # Подписки больше нет — запоздавшему DELETE нечего удалять.
                self._drop_uncertain(row.panel.uuid)
            try:
                # Транзакция только читала — COMMIT лишь снимает блокировку.
                session.commit()
            except Exception as e:  # noqa: BLE001 - снятие блокировки
                session.rollback()
                logger.error(
                    "lock release commit failed %s",
                    kv(username=username, error=type(e).__name__),
                )
        if confirmed_by is None:
            return self._delete_uncertain(row)
        row.action = "deleted"
        self.summary.deleted += 1
        logger.warning(
            "deleted %s",
            kv(archive_reused=reused, confirmed_by=confirmed_by, **row.fields()),
        )
        return "deleted"

    @staticmethod
    def _delete_verdict(fresh, panel_uuid: str) -> Optional[str]:
        if fresh is None or fresh.uuid != panel_uuid:
            return "deleted"
        return None

    async def _confirm(self, row: ReportRow, verdict) -> Optional[str]:
        """Строгие перечитывания панели (блокировка users держится), пока
        verdict(fresh, uuid) не даст определённый ответ; не дольше
        confirm_bound (UNCERTAIN_CONFIRM_SECONDS, но не меньше
        2×RWMS_TIMEOUT_SECONDS). None — так и не выяснилось (панель не
        отвечает или показывает прежнее состояние)."""
        cfg = self.config
        bound = cfg.confirm_bound
        deadline = self._monotonic() + bound
        max_attempts = max(1, math.ceil(bound / DELETE_CONFIRM_POLL_SECONDS))
        username = row.panel.username
        for attempt in range(1, max_attempts + 1):
            remaining = deadline - self._monotonic()
            # Каждый вызов не дольше RWMS_TIMEOUT_SECONDS и не дольше остатка
            # бюджета (но не меньше 1 с, чтобы последний вызов имел смысл).
            timeout = min(cfg.rwms_timeout_seconds, max(remaining, 1.0))
            try:
                fresh = await self._rwms.get_user_by_username_strict(
                    username, timeout=timeout
                )
            except Exception:  # noqa: BLE001 - панель не ответила, ждём дальше
                pass
            else:
                result = verdict(fresh, row.panel.uuid)
                if result is not None:
                    return result
            if attempt >= max_attempts or self._monotonic() >= deadline:
                return None
            await self._sleep(DELETE_CONFIRM_POLL_SECONDS)
        return None

    def _delete_uncertain(self, row: ReportRow) -> str:
        row.action = "failed:delete_uncertain"
        self.summary.delete_uncertain += 1
        self.summary.stop_reason = "delete_uncertain"
        self.summary.exit_code = EXIT_UNCERTAIN
        logger.critical(
            "DELETE UNCERTAIN: DeleteUser outcome unknown and the subscription is "
            "still in the panel; a late DELETE may still land. The archive stays "
            "open, the run is stopped; next --execute runs only re-check it until "
            "the quarantine ends, manual review needed %s",
            kv(
                username=row.panel.username,
                panel_uuid=row.panel.uuid,
                user_id=row.facts.user_id,
                archive_id=row.archive_id,
            ),
        )
        return "uncertain"

    # --- итог --------------------------------------------------------------

    def _finish(self, started_at: datetime) -> None:
        for row in self.rows:
            if row.action in ("planned", "disabled"):
                row.action = f"not_run:{self.summary.stop_reason or 'stopped'}"
        path = self._report_path(started_at)
        try:
            write_report(path, self.rows)
            self.summary.report_path = str(path)
        except OSError as e:
            logger.error("report write failed %s", kv(path=path, error=e))
            if self.summary.exit_code == EXIT_OK:
                self.summary.exit_code = EXIT_FAILURES
        s = self.summary
        logger.info(
            "rw cleaner finished %s",
            kv(
                mode=self.config.mode,
                tier=self._tier_label,
                execute=self.config.execute,
                scanned=s.scanned,
                candidates=s.candidates,
                planned=s.planned,
                over_cap=s.over_cap,
                orphans=s.orphans,
                dry_run=s.dry_run,
                archived=s.archived,
                archive_reused=s.archive_reused,
                deleted=s.deleted,
                skipped_at_execution=s.skipped_at_execution,
                archive_mismatch=s.archive_mismatch,
                archive_closed_aborted=s.archive_closed_aborted,
                archive_close_failed=s.archive_close_failed,
                failed=s.failed,
                delete_uncertain=s.delete_uncertain,
                disable_uncertain=s.disable_uncertain,
                uncertain_pending=s.uncertain_pending,
                uncertain_dropped=s.uncertain_dropped,
                uncertain_reenabled=s.uncertain_reenabled,
                uncertain_cleared=s.uncertain_cleared,
                disabled=s.disabled,
                reenabled=s.reenabled,
                kept_disabled_blocked=s.kept_disabled_blocked,
                reenable_failed=s.reenable_failed,
                recovered=s.recovered,
                stop_reason=s.stop_reason,
                exit_code=s.exit_code,
                report=s.report_path,
            ),
        )
        if s.skip_reasons:
            logger.info("skip reasons %s", kv(**dict(s.skip_reasons.most_common())))


async def run_locked(lock, cleaner: Cleaner) -> int:
    with lock as acquired:
        if not acquired:
            logger.error("another rw-cleaner run holds the lock, exiting")
            return EXIT_LOCKED
        summary = await cleaner.run()
        return summary.exit_code


async def run_from_config(config: CleanerConfig) -> int:
    engine_kwargs = {"pool_pre_ping": True}
    if not config.execute:
        # dry-run: транзакции только на чтение на уровне PostgreSQL.
        engine_kwargs["execution_options"] = {"postgresql_readonly": True}
    engine = create_engine(config.database_url, **engine_kwargs)
    rwms = RwmsClient(
        config.rwms_host, config.rwms_port, timeout=config.rwms_timeout_seconds
    )
    try:
        cleaner = Cleaner(config, session_factory=sessionmaker(bind=engine), rwms=rwms)
        return await run_locked(AdvisoryLock(engine), cleaner)
    finally:
        await rwms.close()
        engine.dispose()


# --- настройки -------------------------------------------------------------


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value else default


INCLUDE_ORPHANS_REMOVED = (
    "--include-orphans was removed: panel users without a users row (orphans) "
    "are never deleted (no users row -> no archive -> mismatch -> skip). They "
    "are always listed in the report (reason local_user_not_found / "
    "orphan_identity_match)."
)


def parse_args(argv: Optional[list[str]] = None) -> CleanerConfig:
    parser = argparse.ArgumentParser(
        description="Archive and delete old expired / inactive free subscriptions "
        "from the panel. Dry-run unless --execute is passed."
    )
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--rwms-host", default=os.getenv("RWMS_HOST"))
    parser.add_argument("--rwms-port", type=int, default=env_int("RWMS_PORT", 0))
    parser.add_argument(
        "--mode", choices=MODES, default=os.getenv("CLEANER_MODE") or MODE_EXPIRED
    )
    parser.add_argument(
        "--tier",
        choices=TIERS,
        default=os.getenv("CLEANER_TIER") or TIER_NEVER_CONNECTED,
    )
    parser.add_argument("--grace-days", type=int, default=env_int("GRACE_DAYS", 90))
    parser.add_argument(
        "--expire-tolerance-hours",
        type=int,
        default=env_int("EXPIRE_TOLERANCE_HOURS", 24),
    )
    parser.add_argument(
        "--recent-payment-days", type=int, default=env_int("RECENT_PAYMENT_DAYS", 14)
    )
    parser.add_argument("--page-size", type=int, default=env_int("PAGE_SIZE", 500))
    parser.add_argument(
        "--max-deletions", type=int, default=env_int("MAX_DELETIONS", 25)
    )
    parser.add_argument(
        "--delete-pause-seconds",
        type=float,
        default=env_float("DELETE_PAUSE_SECONDS", 1.0),
    )
    parser.add_argument(
        "--max-consecutive-failures",
        type=int,
        default=env_int("MAX_CONSECUTIVE_FAILURES", 3),
    )
    parser.add_argument(
        "--max-candidate-ratio",
        type=float,
        default=env_float("MAX_CANDIDATE_RATIO", 0.6),
    )
    parser.add_argument(
        "--max-free-candidate-ratio",
        type=float,
        default=env_float("MAX_FREE_CANDIDATE_RATIO", 0.9),
        help="Candidate-share auto-stop threshold for --mode free_inactive "
        "(most free users may be inactive). --max-candidate-ratio applies to "
        "--mode expired only.",
    )
    parser.add_argument(
        "--max-orphan-ratio", type=float, default=env_float("MAX_ORPHAN_RATIO", 0.05)
    )
    parser.add_argument(
        "--ratio-min-sample", type=int, default=env_int("RATIO_MIN_SAMPLE", 200)
    )
    parser.add_argument(
        "--rwms-timeout-seconds",
        type=float,
        default=env_float("RWMS_TIMEOUT_SECONDS", 30.0),
    )
    parser.add_argument(
        "--stream-timeout-seconds",
        type=float,
        default=env_float("STREAM_TIMEOUT_SECONDS", 60.0),
    )
    parser.add_argument(
        "--free-settle-seconds",
        type=float,
        default=env_float("FREE_SETTLE_SECONDS", DEFAULT_FREE_SETTLE_SECONDS),
        help="free_inactive: pause between disabling the batch (phase 1) and "
        f"deleting it (phase 2); at least {MIN_FREE_SETTLE_SECONDS:.0f} s and "
        "not less than the panel's online/traffic stats interval.",
    )
    parser.add_argument("--report-path", default=os.getenv("REPORT_PATH") or None)
    parser.add_argument(
        "--state-path",
        default=os.getenv("STATE_PATH") or None,
        help="Two-phase deletion state file (subscriptions disabled by this tool "
        "and not finished yet, uncertain DELETE/DISABLE records). Default "
        f"{DEFAULT_STATE_PATH}; must be on a persistent volume.",
    )
    parser.add_argument(
        "--uncertain-confirm-seconds",
        type=float,
        default=env_float(
            "UNCERTAIN_CONFIRM_SECONDS", DEFAULT_UNCERTAIN_CONFIRM_SECONDS
        ),
        help="How long to re-read the panel (holding the users row lock) after a "
        "DELETE/DISABLE with an unknown outcome; never less than "
        f"{DELETE_CONFIRM_FACTOR} x --rwms-timeout-seconds.",
    )
    parser.add_argument(
        "--uncertain-quarantine-hours",
        type=float,
        default=env_float(
            "UNCERTAIN_QUARANTINE_HOURS", DEFAULT_UNCERTAIN_QUARANTINE_HOURS
        ),
        help="An uncertain DELETE/DISABLE record is dropped automatically only "
        "after this many hours since the request (and a fresh safe panel "
        f"observation); at least {MIN_UNCERTAIN_QUARANTINE_HOURS:.0f}.",
    )
    parser.add_argument(
        "--clear-uncertain",
        action="append",
        default=[],
        metavar="PANEL_UUID",
        help="After manual investigation: drop the uncertain record of this panel "
        "uuid (repeatable) and exit without doing anything else. Requires "
        "--execute; logged with the operator name and time.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        default=False,
        help="Actually archive and delete. Default is dry-run.",
    )
    parser.add_argument(
        "--include-orphans",
        action="store_true",
        default=False,
        help="REMOVED: orphans are never deleted; passing it is an error.",
    )
    parser.add_argument(
        "--full-scan",
        action="store_true",
        default=False,
        help="Scan the whole stream even after max-deletions candidates "
        "(for dry-run review; never deletes more than max-deletions).",
    )
    parser.add_argument(
        "--username-allowlist",
        default=os.getenv("USERNAME_ALLOWLIST", ""),
        help="Comma-separated usernames to consider. Empty means all users.",
    )
    args = parser.parse_args(argv)

    if args.include_orphans:
        parser.error(INCLUDE_ORPHANS_REMOVED)
    clear_uncertain = tuple(
        dict.fromkeys(u.strip() for u in args.clear_uncertain if u.strip())
    )
    if args.clear_uncertain and not clear_uncertain:
        parser.error("--clear-uncertain needs a panel uuid")
    if clear_uncertain and not args.execute:
        parser.error("--clear-uncertain requires --execute")
    missing = [
        name
        for name, value in {
            "DATABASE_URL": args.database_url,
            "RWMS_HOST": args.rwms_host,
            "RWMS_PORT": args.rwms_port,
        }.items()
        if not value
    ]
    if missing:
        parser.error(f"Missing required settings: {', '.join(missing)}")
    if args.grace_days < MIN_GRACE_DAYS:
        parser.error(f"--grace-days must be at least {MIN_GRACE_DAYS}")
    if not 1 <= args.max_deletions <= HARD_MAX_DELETIONS:
        parser.error(f"--max-deletions must be within 1..{HARD_MAX_DELETIONS}")
    if not 1 <= args.page_size <= MAX_PAGE_SIZE:
        parser.error(f"--page-size must be within 1..{MAX_PAGE_SIZE}")
    if not 0 <= args.expire_tolerance_hours <= 24 * 7:
        parser.error("--expire-tolerance-hours must be within 0..168")
    if args.recent_payment_days < 1:
        parser.error("--recent-payment-days must be positive")
    if args.delete_pause_seconds < 0:
        parser.error("--delete-pause-seconds must not be negative")
    if args.max_consecutive_failures < 1:
        parser.error("--max-consecutive-failures must be positive")
    if not 0 < args.max_candidate_ratio <= 1:
        parser.error("--max-candidate-ratio must be within (0, 1]")
    if not 0 < args.max_free_candidate_ratio <= 1:
        parser.error("--max-free-candidate-ratio must be within (0, 1]")
    if not 0 <= args.max_orphan_ratio <= 1:
        parser.error("--max-orphan-ratio must be within [0, 1]")
    if args.ratio_min_sample < 1:
        parser.error("--ratio-min-sample must be positive")
    if args.rwms_timeout_seconds <= 0 or args.stream_timeout_seconds <= 0:
        parser.error("timeouts must be positive")
    if not math.isfinite(args.uncertain_confirm_seconds) or (
        args.uncertain_confirm_seconds <= 0
    ):
        parser.error("--uncertain-confirm-seconds must be positive")
    if not math.isfinite(args.uncertain_quarantine_hours) or (
        args.uncertain_quarantine_hours < MIN_UNCERTAIN_QUARANTINE_HOURS
    ):
        parser.error(
            "--uncertain-quarantine-hours must be at least "
            f"{MIN_UNCERTAIN_QUARANTINE_HOURS:.0f}"
        )
    if not math.isfinite(args.free_settle_seconds) or (
        args.free_settle_seconds < MIN_FREE_SETTLE_SECONDS
    ):
        parser.error(
            f"--free-settle-seconds must be at least {MIN_FREE_SETTLE_SECONDS:.0f}"
        )

    return CleanerConfig(
        database_url=args.database_url,
        rwms_host=args.rwms_host,
        rwms_port=args.rwms_port,
        mode=args.mode,
        tier=args.tier,
        grace_days=args.grace_days,
        expire_tolerance_hours=args.expire_tolerance_hours,
        recent_payment_days=args.recent_payment_days,
        page_size=args.page_size,
        max_deletions=args.max_deletions,
        delete_pause_seconds=args.delete_pause_seconds,
        max_consecutive_failures=args.max_consecutive_failures,
        max_candidate_ratio=args.max_candidate_ratio,
        max_free_candidate_ratio=args.max_free_candidate_ratio,
        max_orphan_ratio=args.max_orphan_ratio,
        ratio_min_sample=args.ratio_min_sample,
        rwms_timeout_seconds=args.rwms_timeout_seconds,
        stream_timeout_seconds=args.stream_timeout_seconds,
        free_settle_seconds=args.free_settle_seconds,
        execute=args.execute,
        full_scan=args.full_scan,
        username_allowlist=_csv_set(args.username_allowlist),
        report_path=args.report_path,
        state_path=args.state_path,
        uncertain_confirm_seconds=args.uncertain_confirm_seconds,
        uncertain_quarantine_hours=args.uncertain_quarantine_hours,
        clear_uncertain=clear_uncertain,
    )


def main(argv: Optional[list[str]] = None) -> int:
    config = parse_args(argv)
    setup_logger("rw-cleaner.log")
    return asyncio.run(run_from_config(config))


if __name__ == "__main__":
    raise SystemExit(main())
