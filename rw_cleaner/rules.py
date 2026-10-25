"""Правила отбора кандидатов rw-cleaner.

Факты из БД (DbFacts) собираются только чтением (SQLAlchemy ORM). Решение —
чистая функция от снимка панели, фактов и настроек: кандидат только если
выполнены ВСЕ условия, иначе — пропуск с причиной (reason), которая попадает
в лог и в отчёт.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Optional

from sqlalchemy import exists
from sqlalchemy import func
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from common.account_deletion import REQUEST_STATUS_COMPLETED
from common.account_deletion import REQUEST_STATUS_PENDING
from common.autopay_chains import RECURRENT_CHAIN_MODELS
from common.autopay_chains import autopay_exists_clause
from common.free_tier import active_ban_query
from common.free_tier import active_grace_query
from common.models.db import CP_PAYMENT_STATUS_COMPLETED
from common.models.db import CP_PAYMENT_STATUS_PARTIALLY_REFUNDED
from common.models.db import CP_PAYMENT_STATUS_REFUNDED
from common.models.db import AccountDeletionRequest
from common.models.db import PAW_PAYMENT_STATUS_COMPLETED
from common.models.db import PAW_PAYMENT_STATUS_PARTIALLY_REFUNDED
from common.models.db import PAW_PAYMENT_STATUS_REFUNDED
from common.models.db import CpPayment
from common.models.db import EventLog
from common.models.db import PawPayment
from common.models.db import FreeTierAccess
from common.models.db import User
from common.models.db import UserBlock
from common.models.db import WataInvoice
from common.models.db import WataTransaction
from common.models.db import WebsitePaymentAttempt
from common.models.db import WinbackOffer
from common.models.db import YkPayment
from rw_cleaner.panel import PanelSnapshot

MODE_EXPIRED = "expired"
MODE_FREE_INACTIVE = "free_inactive"
MODES = (MODE_EXPIRED, MODE_FREE_INACTIVE)

# Ступени выкатки (режим expired): каждая следующая шире предыдущей.
TIER_NEVER_CONNECTED = "never_connected"  # ни разу не подключался и не платил
TIER_NEVER_PAID = "never_paid"  # подключался, но ни одного успешного платежа
TIER_ALL = "all"
TIERS = (TIER_NEVER_CONNECTED, TIER_NEVER_PAID, TIER_ALL)

# «Платил» для ступеней: успешный платёж (как PAYS_EXISTS_SQL в
# common/models/segments.py) плюс возвращённый — консервативнее: вернувший
# деньги всё равно когда-то платил и в never_* ступени не попадает.
PAID_YK_STATUSES = ("succeeded", "refunded")
# WATA: payment при полном возврате переводит исходную транзакцию из Paid в
# Refunded (wata_refunds.py), PartiallyRefunded учитывается защитно — тот же
# набор, что CREDITED_WATA_STATUSES в payment. Без возвратных статусов
# полностью вернувший деньги через WATA выглядел бы «никогда не платившим» и
# попадал бы в never_* ступени, в отличие от ЮKassa/CloudPayments (ревью CP,
# круг 7, R1).
PAID_WATA_STATUSES = ("Paid", "PartiallyRefunded", "Refunded")
# CloudPayments: строка cp_payments появляется только на успешную оплату
# (уведомление Pay), возврат меняет её статус. Сегменты считают «платил» только
# Completed, rw-cleaner — консервативнее (как refunded у ЮKassa): любой статус,
# включая полный и частичный возврат.
PAID_CP_STATUSES = (
    CP_PAYMENT_STATUS_COMPLETED,
    CP_PAYMENT_STATUS_REFUNDED,
    CP_PAYMENT_STATUS_PARTIALLY_REFUNDED,
)
# PayAnyWay — как CloudPayments: строка paw_payments появляется только на
# успешную оплату (уведомление Pay URL), возврат меняет её статус; для
# rw-cleaner «платил» — любой статус.
PAID_PAW_STATUSES = (
    PAW_PAYMENT_STATUS_COMPLETED,
    PAW_PAYMENT_STATUS_REFUNDED,
    PAW_PAYMENT_STATUS_PARTIALLY_REFUNDED,
)

# Счёт из бота: при нажатии тарифа бот пишет event_logs.create_invoice_<slug>
# (handlers/tariffs.py). У CloudPayments это ЕДИНСТВЕННЫЙ след неоплаченного
# счёта бота — строка cp_payments появляется только на Pay, попытки сайта
# (website_payment_attempts) бот не пишет. Свежий такой счёт = пользователь,
# возможно, прямо сейчас платит (ревью CP, круг 1). Префикс — с autoescape
# («_» в LIKE — любой символ).
BOT_INVOICE_EVENT_PREFIX = "create_invoice"

# Таблицы, по которым собираются платёжные факты (ступени, недавняя
# активность, автоплатёж; event_logs — счета бота). Если хотя бы одна не читается (миграция ещё не
# применена — например cp_payments / cp_recurrent_payments до e07e3968ff54),
# платёжное состояние НЕОДНОЗНАЧНО: «нет строк» нельзя отличить от «нет
# таблицы», поэтому запуск останавливается до перебора панели
# (payment_schema_problems), а не считает «оплат CP нет». Цепочки автоплатежей
# — из common/autopay_chains.RECURRENT_CHAIN_MODELS: новая цепочка попадает
# сюда сама. Таблицы ОПЛАТ нового шлюза — нет: их статусы «платил» и
# недавняя активность (last_successful_payment, recent_payment_activity)
# пишутся вручную. Гард — tests/test_rules.py::test_every_payment_table_is_a_fact:
# новая платёжная таблица в common без разбора здесь роняет тесты.
PAYMENT_FACT_MODELS = (
    YkPayment,
    WataInvoice,
    WataTransaction,
    WebsitePaymentAttempt,
    CpPayment,
    PawPayment,
    EventLog,
    *RECURRENT_CHAIN_MODELS,
)

# Запросы на удаление аккаунта, при которых состояние считается
# неоднозначным: pending — пользователь ещё может отменить; completed —
# аккаунт обработан админом, подписку не трогаем.
BLOCKING_DELETION_STATUSES = (REQUEST_STATUS_PENDING, REQUEST_STATUS_COMPLETED)

PANEL_STATUS_EXPIRED = "EXPIRED"
# Режим free_inactive — только ACTIVE. LIMITED не трогаем: пользователя
# ограничили нашим же лимитом трафика бесплатного тарифа, подключиться он не
# может, и «нет подключений N дней» вызвано нами, а не его неактивностью.
FREE_PANEL_STATUSES = ("ACTIVE",)
# После сброса трафика панелью (last_traffic_reset_at позже последнего
# online_at) бесплатный пользователь получает минимум столько дней, чтобы
# снова подключиться: он мог быть LIMITED до сброса. Полный
# free_tier_inactive_days здесь не берём — помесячный сброс у всех FREE
# сделал бы режим неработающим.
FREE_RESET_GRACE_DAYS = 7
# Жёсткий нижний предел срока неактивности бесплатного тарифа в
# разрушающем пути: какое бы значение ни стояло в
# system_settings.free_tier_inactive_days (parse_positive_int_setting
# пропускает и 1), подписку, которой пользовались меньше 30 дней назад,
# rw-cleaner не тронет. Совпадает с периодом сброса трафика (month_rolling).
MIN_FREE_INACTIVE_DAYS = 30

REASON_EXPIRED_CANDIDATE = "expired_retention"
REASON_FREE_INACTIVE_CANDIDATE = "free_inactive"
# Тег подписки (поле tag) — ручная пометка владельца («вечная ссылка»,
# «знакомый»): такие подписки rw-cleaner не архивирует и не удаляет НИКОГДА,
# в любом режиме, ни при отборе, ни при перепроверке под блокировкой.
REASON_OWNER_TAGGED = "owner_tagged"


@dataclasses.dataclass(frozen=True)
class RuleConfig:
    grace_days: int = 90
    expire_tolerance_hours: int = 24
    recent_payment_days: int = 14
    tier: str = TIER_NEVER_CONNECTED
    username_allowlist: frozenset[str] = frozenset()


@dataclasses.dataclass(frozen=True)
class DbFacts:
    user_id: Optional[int]
    db_expire_at: Optional[datetime] = None
    blocked: bool = False
    deletion_request: Optional[str] = None
    winback_active: bool = False
    recent_payment: Optional[str] = None
    has_recurrent: bool = False
    on_free: Optional[bool] = None
    free_switched_at: Optional[datetime] = None
    ever_paid: bool = False
    last_payment_at: Optional[datetime] = None
    # Активный временный бан / grace-доступ: отряды подписки сейчас
    # подменены вотчером бота — состояние в движении, не трогаем.
    squad_ban_active: bool = False
    grace_active: bool = False
    # Только для сирот: e-mail / telegram_id из панели совпал с другим users.
    identity_match: bool = False

    @property
    def orphan(self) -> bool:
        return self.user_id is None


@dataclasses.dataclass(frozen=True)
class Decision:
    candidate: bool
    reason: str
    notes: tuple[str, ...] = ()


def as_utc(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def naive_utc(value: datetime) -> datetime:
    return as_utc(value).replace(tzinfo=None)


def _exists(session: Session, *criteria) -> bool:
    return bool(session.execute(select(exists().where(*criteria))).scalar())


def payment_schema_problems(session: Session) -> list[str]:
    """Таблицы PAYMENT_FACT_MODELS, которые не читаются (нет таблицы или
    колонки модели — миграция не применена, либо любая другая ошибка БД).
    Пустой список — платёжные факты собираются полностью. Каждая таблица
    проверяется SELECT всех колонок модели с LIMIT 0 (только чтение); после
    ошибки транзакция откатывается, чтобы проверить остальные."""
    problems = []
    for model in PAYMENT_FACT_MODELS:
        try:
            session.execute(select(*model.__table__.columns).limit(0)).all()
        except SQLAlchemyError:
            session.rollback()
            problems.append(model.__tablename__)
    session.rollback()
    return problems


def recent_payment_activity(
    session: Session, user_id: int, now: datetime, recent_days: int
) -> Optional[str]:
    """Признак недавней платёжной активности (любой статус) — пользователь,
    возможно, прямо сейчас платит. None — активности нет. Кроме платёжных
    таблиц — счёт, выставленный в боте (event_logs create_invoice_*): счёт
    CloudPayments из бота до оплаты не оставляет других следов в БД."""
    cutoff = now - timedelta(days=recent_days)
    if _exists(
        session,
        YkPayment.user_id == user_id,
        YkPayment.created_at >= naive_utc(cutoff),
    ):
        return "recent_yk_payment"
    if _exists(
        session,
        CpPayment.user_id == user_id,
        or_(
            CpPayment.created_at >= naive_utc(cutoff),
            CpPayment.captured_at >= naive_utc(cutoff),
        ),
    ):
        return "recent_cp_payment"
    if _exists(
        session,
        PawPayment.user_id == user_id,
        or_(
            PawPayment.created_at >= naive_utc(cutoff),
            PawPayment.captured_at >= naive_utc(cutoff),
        ),
    ):
        return "recent_paw_payment"
    if _exists(
        session,
        WataInvoice.user_id == user_id,
        or_(
            WataInvoice.creation_time >= cutoff,
            WataInvoice.expiration_datetime >= now,
        ),
    ):
        return "recent_wata_invoice"
    if _exists(
        session,
        WebsitePaymentAttempt.user_id == user_id,
        WebsitePaymentAttempt.created_at >= naive_utc(cutoff),
    ):
        return "recent_website_payment_attempt"
    if _exists(
        session,
        EventLog.user_id == user_id,
        EventLog.event_type.startswith(BOT_INVOICE_EVENT_PREFIX, autoescape=True),
        EventLog.timestamp >= naive_utc(cutoff),
    ):
        return "recent_bot_invoice"
    return None


def last_successful_payment(session: Session, user_id: int) -> Optional[datetime]:
    last_yk = session.execute(
        select(func.max(func.coalesce(YkPayment.captured_at, YkPayment.created_at)))
        .where(YkPayment.user_id == user_id)
        .where(YkPayment.status.in_(PAID_YK_STATUSES))
    ).scalar()
    last_wata = session.execute(
        select(func.max(WataTransaction.payment_time))
        .join(WataInvoice, WataInvoice.order_id == WataTransaction.order_id)
        .where(WataInvoice.user_id == user_id)
        .where(WataTransaction.transaction_status.in_(PAID_WATA_STATUSES))
    ).scalar()
    last_cp = session.execute(
        select(func.max(func.coalesce(CpPayment.captured_at, CpPayment.created_at)))
        .where(CpPayment.user_id == user_id)
        .where(CpPayment.status.in_(PAID_CP_STATUSES))
    ).scalar()
    last_paw = session.execute(
        select(func.max(func.coalesce(PawPayment.captured_at, PawPayment.created_at)))
        .where(PawPayment.user_id == user_id)
        .where(PawPayment.status.in_(PAID_PAW_STATUSES))
    ).scalar()
    dates = [as_utc(v) for v in (last_yk, last_wata, last_cp, last_paw) if v is not None]
    return max(dates) if dates else None


def load_db_facts(
    session: Session, panel: PanelSnapshot, now: datetime, recent_days: int
) -> DbFacts:
    """Факты о пользователе из БД (только чтение)."""
    row = session.execute(
        select(User.id, User.expire_at).where(User.username == panel.username)
    ).first()
    if row is None:
        conditions = []
        if panel.telegram_id:
            conditions.append(User.telegram_id == panel.telegram_id)
        if panel.email:
            conditions.append(User.email == panel.email)
        identity_match = bool(conditions) and _exists(session, or_(*conditions))
        return DbFacts(user_id=None, identity_match=identity_match)

    user_id = row.id
    free = session.execute(
        select(FreeTierAccess.on_free, FreeTierAccess.switched_at).where(
            FreeTierAccess.user_id == user_id
        )
    ).first()
    last_payment_at = last_successful_payment(session, user_id)
    return DbFacts(
        user_id=user_id,
        db_expire_at=as_utc(row.expire_at),
        blocked=_exists(session, UserBlock.user_id == user_id),
        deletion_request=session.execute(
            select(AccountDeletionRequest.status)
            .where(AccountDeletionRequest.user_id == user_id)
            .where(AccountDeletionRequest.status.in_(BLOCKING_DELETION_STATUSES))
            .limit(1)
        ).scalar(),
        winback_active=_exists(
            session,
            WinbackOffer.user_id == user_id,
            WinbackOffer.valid_until > naive_utc(now),
        ),
        recent_payment=recent_payment_activity(session, user_id, now, recent_days),
        # Цепочка автоплатежа в ЛЮБОЙ таблице (ЮKassa, CloudPayments, PayAnyWay):
        # autopay_exists_clause выполняется в этом запросе без деградации —
        # нет таблицы цепочек = ошибка БД (удаления не будет), а не «цепочек
        # нет», как у common.autopay_chains.user_has_autopay.
        has_recurrent=bool(
            session.execute(select(autopay_exists_clause(user_id))).scalar()
        ),
        on_free=None if free is None else bool(free.on_free),
        free_switched_at=None if free is None else as_utc(free.switched_at),
        ever_paid=last_payment_at is not None,
        last_payment_at=last_payment_at,
        squad_ban_active=bool(
            session.execute(select(active_ban_query(user_id).exists())).scalar()
        ),
        grace_active=bool(
            session.execute(select(active_grace_query(user_id).exists())).scalar()
        ),
    )


def panel_notes(
    panel: PanelSnapshot, default_squads: frozenset[str]
) -> tuple[str, ...]:
    """Нестандартные параметры подписки: на решение не влияют (архив их
    хранит), но пишутся в лог и отчёт для ревью."""
    notes = []
    if panel.traffic_limit_bytes:
        notes.append(f"traffic_limit_bytes={panel.traffic_limit_bytes}")
    if panel.hwid_device_limit:
        notes.append(f"hwid_device_limit={panel.hwid_device_limit}")
    if default_squads and set(panel.internal_squads) != set(default_squads):
        notes.append("internal_squads_non_default")
    if panel.external_squad_uuid:
        notes.append("external_squad_set")
    if panel.tag:
        notes.append(f"tag={panel.tag}")
    return tuple(notes)


def _common_db_blockers(facts: DbFacts) -> Optional[str]:
    if facts.blocked:
        return "user_blocked"
    if facts.deletion_request:
        return f"account_deletion_{facts.deletion_request}"
    if facts.winback_active:
        return "winback_offer_active"
    if facts.recent_payment:
        return facts.recent_payment
    if facts.has_recurrent:
        return "recurrent_payment_saved"
    if facts.squad_ban_active:
        return "temporary_squad_ban_active"
    if facts.grace_active:
        return "grace_access_active"
    return None


def _identity_skip(panel: PanelSnapshot, config: RuleConfig) -> Optional[str]:
    if config.username_allowlist and panel.username not in config.username_allowlist:
        return "not_in_allowlist"
    if not panel.uuid or not panel.username:
        return "missing_rwms_identity"
    if panel.tag:
        # Любой непустой тег (даже из пробелов) — пометка владельца.
        return REASON_OWNER_TAGGED
    return None


def panel_on_free_squad(panel: PanelSnapshot, free_squad_uuid: Optional[str]) -> bool:
    """Подписка на бесплатном тарифе: её внешний отряд — отряд FREE из
    настроек (как common.free_tier.panel_on_free_plan, без учёта регистра).
    Отряд FREE не настроен — бесплатного тарифа нет ни у кого. Тег подписки
    признаком НЕ является: он принадлежит владельцу."""
    free = str(free_squad_uuid or "").strip().lower()
    if not free:
        return False
    return str(panel.external_squad_uuid or "").strip().lower() == free


def decide_expired(
    panel: PanelSnapshot,
    facts: DbFacts,
    config: RuleConfig,
    now: datetime,
    default_squads: frozenset[str] = frozenset(),
    free_squad_uuid: Optional[str] = None,
) -> Decision:
    """Режим expired: истёкшая сверх срока хранения подписка. Подписки на
    внешнем отряде FREE (free_squad_uuid) и с тегом владельца пропускаются."""
    skip = _identity_skip(panel, config)
    if skip:
        return Decision(False, skip)
    if panel.status != PANEL_STATUS_EXPIRED:
        # DISABLED — блокировка, LIMITED — лимит трафика, ACTIVE — действует.
        return Decision(False, f"panel_status_{panel.status.lower() or 'unknown'}")
    if panel_on_free_squad(panel, free_squad_uuid):
        return Decision(False, "panel_free_squad")
    cutoff = now - timedelta(days=config.grace_days)
    if panel.expire_at is None:
        return Decision(False, "panel_expire_at_missing")
    if panel.expire_at >= cutoff:
        return Decision(False, "panel_not_old_enough")

    notes = panel_notes(panel, default_squads)

    if facts.orphan:
        # Сирота (нет строки users) не удаляется НИКОГДА: без user_id её
        # нельзя заархивировать, а панель и БД расходятся — по правилам
        # RW Cleaner это пропуск. В отчёт попадает с причиной.
        if facts.identity_match:
            return Decision(False, "orphan_identity_match", notes)
        return Decision(False, "local_user_not_found", notes)

    if facts.db_expire_at is None:
        return Decision(False, "db_expire_at_missing", notes)
    if facts.db_expire_at >= now:
        return Decision(False, "db_user_active", notes)
    if facts.db_expire_at >= cutoff:
        return Decision(False, "db_not_old_enough", notes)
    tolerance = timedelta(hours=config.expire_tolerance_hours)
    if abs(panel.expire_at - facts.db_expire_at) > tolerance:
        return Decision(False, "expire_mismatch", notes)
    blocker = _common_db_blockers(facts)
    if blocker:
        return Decision(False, blocker, notes)
    if facts.on_free:
        return Decision(False, "free_tier_on_free", notes)
    if config.tier in (TIER_NEVER_CONNECTED, TIER_NEVER_PAID) and facts.ever_paid:
        return Decision(False, "tier_paid", notes)
    if config.tier == TIER_NEVER_CONNECTED and not panel.never_connected:
        return Decision(False, "tier_connected", notes)
    if not panel.restorable:
        return Decision(False, "not_restorable", notes)
    return Decision(True, REASON_EXPIRED_CANDIDATE, notes)


def last_free_activity(panel: PanelSnapshot, facts: DbFacts) -> Optional[datetime]:
    """Последняя активность на бесплатном тарифе: самое позднее из
    online_at, first_connected и момента перевода на тариф (switched_at) —
    только что переведённый пользователь всегда получает полный срок."""
    dates = [
        d
        for d in (panel.online_at, panel.first_connected_at, facts.free_switched_at)
        if d is not None
    ]
    return max(dates) if dates else None


def effective_free_inactive_days(inactive_days) -> int:
    """Срок неактивности с жёстким нижним пределом MIN_FREE_INACTIVE_DAYS
    (None/мусор — тоже предел)."""
    try:
        value = int(inactive_days)
    except (TypeError, ValueError):
        value = 0
    return max(value, MIN_FREE_INACTIVE_DAYS)


def decide_free_inactive(
    panel: PanelSnapshot,
    facts: DbFacts,
    config: RuleConfig,
    now: datetime,
    inactive_days: int,
    default_squads: frozenset[str] = frozenset(),
    free_squad_uuid: Optional[str] = None,
) -> Decision:
    """Режим free_inactive: бесплатный тариф (внешний отряд FREE =
    free_squad_uuid) без подключений N дней. Отряд не задан — кандидатов нет;
    тег владельца — пропуск. Срок неактивности не меньше
    MIN_FREE_INACTIVE_DAYS, что бы ни передал вызывающий."""
    inactive_days = effective_free_inactive_days(inactive_days)
    skip = _identity_skip(panel, config)
    if skip:
        return Decision(False, skip)
    if not panel_on_free_squad(panel, free_squad_uuid):
        return Decision(False, "panel_not_free")
    if panel.status not in FREE_PANEL_STATUSES:
        return Decision(False, f"panel_status_{panel.status.lower() or 'unknown'}")
    notes = panel_notes(panel, default_squads)
    if facts.orphan:
        return Decision(False, "local_user_not_found", notes)
    if facts.on_free is None:
        return Decision(False, "free_tier_access_missing", notes)
    if not facts.on_free:
        return Decision(False, "free_tier_not_on_free", notes)
    if facts.db_expire_at is not None and facts.db_expire_at >= now:
        return Decision(False, "db_paid_time_active", notes)
    blocker = _common_db_blockers(facts)
    if blocker:
        return Decision(False, blocker, notes)
    last_activity = last_free_activity(panel, facts)
    if last_activity is None:
        return Decision(False, "free_activity_unknown", notes)
    if last_activity >= now - timedelta(days=inactive_days):
        return Decision(False, "free_recently_active", notes)
    reset_at = panel.last_traffic_reset_at
    if (
        reset_at is not None
        and reset_at > last_activity
        and reset_at >= now - timedelta(days=FREE_RESET_GRACE_DAYS)
    ):
        return Decision(False, "free_traffic_reset_recently", notes)
    if not panel.restorable:
        return Decision(False, "not_restorable", notes)
    return Decision(True, REASON_FREE_INACTIVE_CANDIDATE, notes)
