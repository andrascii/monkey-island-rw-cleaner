"""Правила отбора (rw_cleaner/rules.py): каждое условие режима expired и
free_inactive, ступени выкатки и сбор фактов из БД."""

import dataclasses
from datetime import timedelta

import pytest

import proto.rwmanager_pb2 as proto
from common.models.db import AccountDeletionRequest
from common.models.db import FreeTierAccess
from common.models.db import GraceAccessGrant
from common.models.db import TemporarySquadBan
from common.models.db import UserBlock
from common.models.db import WataInvoice
from common.models.db import WataTransaction
from common.models.db import WebsitePaymentAttempt
from common.models.db import WinbackOffer
from common.models.db import YkPayment
from common.models.db import YkRecurrentPayment
from rw_cleaner.panel import snapshot_from_proto
from rw_cleaner.rules import MIN_FREE_INACTIVE_DAYS
from rw_cleaner.rules import DbFacts
from rw_cleaner.rules import RuleConfig
from rw_cleaner.rules import TIER_ALL
from rw_cleaner.rules import TIER_NEVER_CONNECTED
from rw_cleaner.rules import TIER_NEVER_PAID
from rw_cleaner.rules import decide_expired
from rw_cleaner.rules import decide_free_inactive
from rw_cleaner.rules import effective_free_inactive_days
from rw_cleaner.rules import load_db_facts
from tests.helpers import NOW
from tests.helpers import add_user
from tests.helpers import days_ago
from tests.helpers import naive
from tests.helpers import panel_user


FREE_SQUAD = "ext-free"


def snap(username="u1", **kw):
    return snapshot_from_proto(panel_user(username, **kw))


def facts(**kw):
    values = {"user_id": 1, "db_expire_at": days_ago(120)}
    values.update(kw)
    return DbFacts(**values)


def expired(panel=None, db=None, free_squad=FREE_SQUAD, **config):
    return decide_expired(
        panel or snap(),
        db or facts(),
        RuleConfig(**config),
        NOW,
        frozenset({"squad-default"}),
        free_squad,
    )


# --- режим expired -----------------------------------------------------------


def test_baseline_candidate_never_connected():
    decision = expired()
    assert decision.candidate
    assert decision.reason == "expired_retention"
    assert decision.notes == ()


@pytest.mark.parametrize(
    "panel_kwargs, db_kwargs, config, reason",
    [
        (
            {"username": "other"},
            {},
            {"username_allowlist": frozenset({"u1"})},
            "not_in_allowlist",
        ),
        ({"status": proto.UserStatus.ACTIVE}, {}, {}, "panel_status_active"),
        ({"status": proto.UserStatus.DISABLED}, {}, {}, "panel_status_disabled"),
        ({"status": proto.UserStatus.LIMITED}, {}, {}, "panel_status_limited"),
        # Тег — ручная пометка владельца: никогда не архивируем/не удаляем.
        ({"tag": "FREE"}, {}, {}, "owner_tagged"),
        ({"tag": "VIP"}, {}, {}, "owner_tagged"),
        ({"tag": "вечная ссылка"}, {}, {}, "owner_tagged"),
        ({"tag": " "}, {}, {}, "owner_tagged"),
        # Бесплатный тариф — внешний отряд FREE (без учёта регистра).
        ({"external_squad_uuid": FREE_SQUAD}, {}, {}, "panel_free_squad"),
        ({"external_squad_uuid": "EXT-FREE"}, {}, {}, "panel_free_squad"),
        (
            {"expire_at": days_ago(80)},
            {"db_expire_at": days_ago(80)},
            {},
            "panel_not_old_enough",
        ),
        ({}, {"db_expire_at": None}, {}, "db_expire_at_missing"),
        ({}, {"db_expire_at": NOW + timedelta(days=3)}, {}, "db_user_active"),
        ({}, {"db_expire_at": days_ago(60)}, {}, "db_not_old_enough"),
        ({}, {"db_expire_at": days_ago(118)}, {}, "expire_mismatch"),
        ({}, {"blocked": True}, {}, "user_blocked"),
        ({}, {"deletion_request": "pending"}, {}, "account_deletion_pending"),
        ({}, {"deletion_request": "completed"}, {}, "account_deletion_completed"),
        ({}, {"winback_active": True}, {}, "winback_offer_active"),
        ({}, {"recent_payment": "recent_wata_invoice"}, {}, "recent_wata_invoice"),
        ({}, {"has_recurrent": True}, {}, "recurrent_payment_saved"),
        ({}, {"squad_ban_active": True}, {}, "temporary_squad_ban_active"),
        ({}, {"grace_active": True}, {}, "grace_access_active"),
        ({}, {"on_free": True}, {}, "free_tier_on_free"),
        ({}, {"ever_paid": True}, {}, "tier_paid"),
        ({}, {"ever_paid": True}, {"tier": TIER_NEVER_PAID}, "tier_paid"),
        ({"lifetime_traffic": 1024.0}, {}, {}, "tier_connected"),
        ({"first_connected": days_ago(200)}, {}, {}, "tier_connected"),
        ({"online_at": days_ago(150)}, {}, {}, "tier_connected"),
        ({"short_uuid": "bad uuid!"}, {}, {}, "not_restorable"),
        ({"trojan_password": "short"}, {}, {}, "not_restorable"),
    ],
)
def test_expired_skip_rules(panel_kwargs, db_kwargs, config, reason):
    decision = expired(snap(**panel_kwargs), facts(**db_kwargs), **config)
    assert not decision.candidate
    assert decision.reason == reason


def test_missing_panel_identity_is_skipped():
    assert (
        expired(dataclasses.replace(snap(), uuid="")).reason == "missing_rwms_identity"
    )


def test_missing_panel_expire_is_skipped():
    panel = dataclasses.replace(snap(), expire_at=None)
    assert expired(panel).reason == "panel_expire_at_missing"


def test_expire_tolerance_boundary():
    panel = snap(expire_at=days_ago(120))
    within = facts(db_expire_at=days_ago(120) + timedelta(hours=23))
    assert expired(panel, within).candidate
    beyond = facts(db_expire_at=days_ago(120) + timedelta(hours=25))
    assert expired(panel, beyond).reason == "expire_mismatch"
    assert expired(panel, beyond, expire_tolerance_hours=48).candidate


def test_grace_days_is_respected_on_both_sides():
    panel = snap(expire_at=days_ago(100))
    db = facts(db_expire_at=days_ago(100))
    assert expired(panel, db, grace_days=90).candidate
    assert expired(panel, db, grace_days=120).reason == "panel_not_old_enough"


def test_tiers_are_nested():
    connected = snap(
        first_connected=days_ago(300), online_at=days_ago(130), lifetime_traffic=5e9
    )
    assert expired(connected, tier=TIER_NEVER_CONNECTED).reason == "tier_connected"
    assert expired(connected, tier=TIER_NEVER_PAID).candidate
    paid = facts(ever_paid=True, last_payment_at=days_ago(200))
    assert expired(connected, paid, tier=TIER_NEVER_PAID).reason == "tier_paid"
    assert expired(connected, paid, tier=TIER_ALL).candidate


def test_tier_all_still_applies_every_safety_rule():
    for db in (
        facts(blocked=True),
        facts(has_recurrent=True),
        facts(winback_active=True),
        facts(recent_payment="recent_yk_payment"),
    ):
        assert not expired(db=db, tier=TIER_ALL).candidate


def test_orphans_are_skipped_by_default():
    assert expired(db=DbFacts(user_id=None)).reason == "local_user_not_found"


def test_orphan_with_matching_identity_is_never_deleted():
    decision = expired(db=DbFacts(user_id=None, identity_match=True), tier=TIER_ALL)
    assert decision.reason == "orphan_identity_match"


@pytest.mark.parametrize("tier", [TIER_NEVER_CONNECTED, TIER_NEVER_PAID, TIER_ALL])
def test_orphans_are_never_candidates(tier):
    # R4: сирота (нет строки users) не удаляется никогда — нечего
    # архивировать, панель и БД расходятся. Даже самая широкая ступень и
    # никогда не подключавшийся пользователь.
    for panel in (snap(), snap(lifetime_traffic=10.0)):
        decision = expired(panel, DbFacts(user_id=None), tier=tier)
        assert not decision.candidate
        assert decision.reason == "local_user_not_found"


def test_rule_config_has_no_orphan_switch():
    assert "include_orphans" not in {f.name for f in dataclasses.fields(RuleConfig)}


def test_non_default_parameters_are_noted_but_still_archivable():
    panel = snap(
        traffic_limit_bytes=5 * 1024**3,
        hwid_device_limit=2,
        squads=("squad-other",),
        external_squad_uuid="ext-1",
    )
    decision = expired(panel)
    assert decision.candidate
    assert "traffic_limit_bytes=5368709120" in decision.notes
    assert "hwid_device_limit=2" in decision.notes
    assert "internal_squads_non_default" in decision.notes
    assert "external_squad_set" in decision.notes


# --- режим free_inactive -----------------------------------------------------


def free_snap(**kw):
    values = {
        "status": proto.UserStatus.ACTIVE,
        "external_squad_uuid": FREE_SQUAD,
        "expire_at": NOW + timedelta(days=20000),
        "online_at": days_ago(45),
        "first_connected": days_ago(100),
        "lifetime_traffic": 1e9,
    }
    values.update(kw)
    return snap(**values)


def free_facts(**kw):
    values = {
        "user_id": 1,
        "db_expire_at": days_ago(40),
        "on_free": True,
        "free_switched_at": days_ago(40),
    }
    values.update(kw)
    return DbFacts(**values)


def free(panel=None, db=None, inactive_days=30, free_squad=FREE_SQUAD, **config):
    return decide_free_inactive(
        panel or free_snap(),
        db or free_facts(),
        RuleConfig(**config),
        NOW,
        inactive_days,
        free_squad_uuid=free_squad,
    )


def test_free_inactive_candidate():
    decision = free()
    assert decision.candidate
    assert decision.reason == "free_inactive"


def test_free_limited_status_is_skipped():
    # LIMITED — упёрся в наш лимит трафика бесплатного тарифа и подключиться
    # не может: «нет подключений» вызвано нами, не удаляем.
    decision = free(free_snap(status=proto.UserStatus.LIMITED))
    assert not decision.candidate
    assert decision.reason == "panel_status_limited"


def test_free_recent_traffic_reset_gives_grace_to_reconnect():
    # Сброс трафика позже последнего подключения и недавно — пользователь
    # мог быть LIMITED до сброса: даём FREE_RESET_GRACE_DAYS на подключение.
    panel = dataclasses.replace(free_snap(), last_traffic_reset_at=days_ago(3))
    assert free(panel).reason == "free_traffic_reset_recently"
    old_reset = dataclasses.replace(free_snap(), last_traffic_reset_at=days_ago(8))
    assert free(old_reset).candidate
    # Сброс раньше последней активности — на решение не влияет.
    before = dataclasses.replace(
        free_snap(online_at=days_ago(45)), last_traffic_reset_at=days_ago(46)
    )
    assert free(before).candidate


def test_snapshot_reads_last_traffic_reset_at():
    user = panel_user("u1")
    user.last_traffic_reset_at.FromDatetime(naive(days_ago(2)))
    assert snapshot_from_proto(user).last_traffic_reset_at == days_ago(2)
    assert snapshot_from_proto(panel_user("u1")).last_traffic_reset_at is None


@pytest.mark.parametrize(
    "panel_kwargs, db_kwargs, reason",
    [
        ({"external_squad_uuid": "ext-paid"}, {}, "panel_not_free"),
        ({"external_squad_uuid": ""}, {}, "panel_not_free"),
        ({"tag": "FREE"}, {}, "owner_tagged"),
        ({"tag": "PAID"}, {}, "owner_tagged"),
        ({"tag": "знакомый"}, {}, "owner_tagged"),
        ({"status": proto.UserStatus.DISABLED}, {}, "panel_status_disabled"),
        ({}, {"on_free": None, "free_switched_at": None}, "free_tier_access_missing"),
        ({}, {"on_free": False}, "free_tier_not_on_free"),
        ({}, {"db_expire_at": NOW + timedelta(days=1)}, "db_paid_time_active"),
        ({}, {"blocked": True}, "user_blocked"),
        ({}, {"deletion_request": "pending"}, "account_deletion_pending"),
        ({}, {"recent_payment": "recent_yk_payment"}, "recent_yk_payment"),
        ({}, {"squad_ban_active": True}, "temporary_squad_ban_active"),
        ({}, {"grace_active": True}, "grace_access_active"),
        ({"online_at": days_ago(5)}, {}, "free_recently_active"),
        ({"short_uuid": "x"}, {}, "not_restorable"),
    ],
)
def test_free_skip_rules(panel_kwargs, db_kwargs, reason):
    decision = free(free_snap(**panel_kwargs), free_facts(**db_kwargs))
    assert not decision.candidate
    assert decision.reason == reason


def test_free_squad_not_configured_means_no_free_candidates():
    for squad in (None, "", "  "):
        assert free(free_squad=squad).reason == "panel_not_free"
    assert free(free_snap(external_squad_uuid="EXT-FREE")).candidate


def test_owner_tag_beats_every_other_rule_in_both_modes():
    # Даже самый «удаляемый» кандидат (tier all, сироты разрешены) с тегом
    # владельца не трогается.
    assert expired(snap(tag="VIP"), tier=TIER_ALL).reason == "owner_tagged"
    assert (
        expired(snap(tag="VIP"), DbFacts(user_id=None), tier=TIER_ALL).reason
        == "owner_tagged"
    )
    assert free(free_snap(tag="вечная ссылка")).reason == "owner_tagged"
    # Пустой тег — не пометка.
    assert expired(snap(tag="")).candidate
    assert free(free_snap(tag="")).candidate


def test_owner_tagged_free_squad_subscription_is_not_a_candidate():
    assert free(free_snap(tag="FREE")).reason == "owner_tagged"


def test_free_orphan_is_never_a_candidate():
    assert free(db=DbFacts(user_id=None)).reason == "local_user_not_found"


def test_free_recent_switch_gives_full_inactivity_period():
    # Подключался давно (на платном), на бесплатный перевели только что.
    db = free_facts(free_switched_at=days_ago(3))
    assert free(free_snap(online_at=days_ago(200)), db).reason == "free_recently_active"


def test_free_never_connected_uses_switched_at():
    panel = free_snap(online_at=None, first_connected=None, lifetime_traffic=0.0)
    panel = dataclasses.replace(panel, online_at=None, first_connected_at=None)
    assert free(panel, free_facts(free_switched_at=days_ago(31))).candidate
    assert (
        free(panel, free_facts(free_switched_at=days_ago(29))).reason
        == "free_recently_active"
    )


def test_free_inactive_days_setting():
    assert free(inactive_days=60).reason == "free_recently_active"


@pytest.mark.parametrize("configured", [None, 0, -5, 1, 2, 7, 29, "garbage"])
def test_free_inactive_days_has_a_hard_floor(configured):
    # R3: free_tier_inactive_days=1 из system_settings не должен удалить
    # подписку, которой пользовались 2 дня (и 29 дней) назад.
    for days in (2, 29):
        decision = free(free_snap(online_at=days_ago(days)), inactive_days=configured)
        assert decision.reason == "free_recently_active", (configured, days)
    assert free(free_snap(online_at=days_ago(31)), inactive_days=configured).candidate


def test_effective_free_inactive_days():
    assert MIN_FREE_INACTIVE_DAYS == 30
    assert effective_free_inactive_days(1) == 30
    assert effective_free_inactive_days(None) == 30
    assert effective_free_inactive_days(30) == 30
    assert effective_free_inactive_days(45) == 45


# --- факты из БД -------------------------------------------------------------


def test_load_db_facts_clean_user(session_factory):
    with session_factory() as s:
        user_id = add_user(s, "clean", expire_at=days_ago(120)).id
        s.commit()
        result = load_db_facts(s, snap("clean"), NOW, 14)
    assert result.user_id == user_id
    assert result.db_expire_at == days_ago(120)
    assert not result.blocked
    assert result.deletion_request is None
    assert not result.winback_active
    assert result.recent_payment is None
    assert not result.has_recurrent
    assert result.on_free is None
    assert not result.ever_paid
    assert result.last_payment_at is None


def test_load_db_facts_orphan_and_identity_match(session_factory):
    with session_factory() as s:
        add_user(s, "someone", telegram_id=777, email="a@example.com")
        s.commit()
        plain = load_db_facts(s, snap("ghost"), NOW, 14)
        by_tg = load_db_facts(s, snap("ghost", telegram_id=777), NOW, 14)
        by_email = load_db_facts(s, snap("ghost", email="a@example.com"), NOW, 14)
    assert plain.orphan and not plain.identity_match
    assert by_tg.orphan and by_tg.identity_match
    assert by_email.orphan and by_email.identity_match


def _yk(s, user_id, status, created, pid):
    s.add(
        YkPayment(
            id=pid,
            user_id=user_id,
            amount=100,
            currency="RUB",
            status=status,
            created_at=naive(created),
            captured_at=naive(created) if status == "succeeded" else None,
            payment_id=f"p{pid}",
            subscription_period="month",
        )
    )


def test_load_db_facts_blocks_deletions_winback_recurrent_free(session_factory):
    with session_factory() as s:
        u = add_user(s, "busy", expire_at=days_ago(120))
        s.add(UserBlock(user_id=u.id))
        s.add(
            AccountDeletionRequest(id=1, user_id=u.id, status="cancelled", source="app")
        )
        s.add(
            AccountDeletionRequest(id=2, user_id=u.id, status="pending", source="app")
        )
        s.add(
            WinbackOffer(
                id=1,
                user_id=u.id,
                offer_scope="all",
                valid_until=naive(NOW + timedelta(hours=5)),
            )
        )
        s.add(
            YkRecurrentPayment(
                id=1,
                user_id=u.id,
                currency="RUB",
                amount=1,
                recurrent_payment_id="r1",
                subscription_period="month",
                captured_at=naive(days_ago(300)),
            )
        )
        s.add(
            FreeTierAccess(
                id=1, user_id=u.id, on_free=True, switched_at=naive(days_ago(10))
            )
        )
        s.commit()
        result = load_db_facts(s, snap("busy"), NOW, 14)
    assert result.blocked
    assert result.deletion_request == "pending"
    assert result.winback_active
    assert result.has_recurrent
    assert result.on_free is True
    assert result.free_switched_at == days_ago(10)


def test_cancelled_deletion_and_used_winback_do_not_block(session_factory):
    with session_factory() as s:
        u = add_user(s, "calm", expire_at=days_ago(120))
        s.add(
            AccountDeletionRequest(
                id=3, user_id=u.id, status="cancelled", source="site"
            )
        )
        s.add(
            WinbackOffer(
                id=2, user_id=u.id, offer_scope="month", valid_until=naive(days_ago(80))
            )
        )
        s.commit()
        result = load_db_facts(s, snap("calm"), NOW, 14)
    assert result.deletion_request is None
    assert not result.winback_active


def test_payment_history_and_recent_activity(session_factory):
    with session_factory() as s:
        u = add_user(s, "payer", expire_at=days_ago(120))
        _yk(s, u.id, "succeeded", days_ago(200), 1)
        _yk(s, u.id, "canceled", days_ago(150), 2)
        s.commit()
        old = load_db_facts(s, snap("payer"), NOW, 14)
        _yk(s, u.id, "canceled", days_ago(3), 3)
        s.commit()
        recent = load_db_facts(s, snap("payer"), NOW, 14)
    assert old.ever_paid
    assert old.last_payment_at == days_ago(200)
    assert old.recent_payment is None
    assert recent.recent_payment == "recent_yk_payment"


def test_refunded_payment_counts_as_paid(session_factory):
    with session_factory() as s:
        u = add_user(s, "refund", expire_at=days_ago(120))
        _yk(s, u.id, "refunded", days_ago(200), 10)
        s.commit()
        assert load_db_facts(s, snap("refund"), NOW, 14).ever_paid


def _wata_invoice(s, iid, user_id, created, expires, order):
    s.add(
        WataInvoice(
            id=iid,
            user_id=user_id,
            invoice_id=f"inv{iid}",
            amount=100,
            currency="RUB",
            status="Opened",
            terminal_name="t",
            terminal_public_id="tp",
            creation_time=created,
            order_id=order,
            expiration_datetime=expires,
        )
    )


def test_wata_paid_transaction_and_invoices(session_factory):
    with session_factory() as s:
        u = add_user(s, "wata", expire_at=days_ago(120))
        _wata_invoice(s, 1, u.id, days_ago(250), days_ago(249), "o1")
        s.add(
            WataTransaction(
                id=1,
                transaction_id="t1",
                transaction_type="CardCrypto",
                terminal_public_id="tp",
                transaction_status="Paid",
                terminal_name="t",
                amount=100,
                currency="RUB",
                order_id="o1",
                order_description="d",
                commission=0,
                payment_time=days_ago(250),
            )
        )
        s.commit()
        paid = load_db_facts(s, snap("wata"), NOW, 14)
        _wata_invoice(s, 2, u.id, days_ago(5), days_ago(4), "o2")
        s.commit()
        recent = load_db_facts(s, snap("wata"), NOW, 14)
    assert paid.ever_paid
    assert paid.last_payment_at == days_ago(250)
    assert paid.recent_payment is None
    assert recent.recent_payment == "recent_wata_invoice"


def test_old_but_unexpired_wata_invoice_blocks(session_factory):
    with session_factory() as s:
        u = add_user(s, "wata2", expire_at=days_ago(120))
        _wata_invoice(s, 3, u.id, days_ago(60), NOW + timedelta(days=1), "o3")
        s.commit()
        assert (
            load_db_facts(s, snap("wata2"), NOW, 14).recent_payment
            == "recent_wata_invoice"
        )


def test_recent_website_payment_attempt(session_factory):
    with session_factory() as s:
        u = add_user(s, "site", expire_at=days_ago(120))
        s.add(
            WebsitePaymentAttempt(
                id="a1",
                user_id=u.id,
                fingerprint="f",
                gateway="wata",
                tariff_id="month",
                state="created",
                request_payload={},
                status_token_hash="h1",
                status_url="https://x",
                created_at=naive(days_ago(2)),
            )
        )
        s.commit()
        result = load_db_facts(s, snap("site"), NOW, 14)
        assert result.recent_payment == "recent_website_payment_attempt"
        assert (
            load_db_facts(s, snap("site"), NOW + timedelta(days=30), 14).recent_payment
            is None
        )


def test_load_db_facts_active_ban_and_grace(session_factory):
    with session_factory() as s:
        banned = add_user(s, "banned", expire_at=days_ago(120)).id
        graced = add_user(s, "graced", expire_at=days_ago(120)).id
        s.add(TemporarySquadBan(id=1, user_id=banned, unban_at=naive(NOW)))
        s.add(
            GraceAccessGrant(
                id=1, user_id=graced, squad_uuid="grace", expires_at=naive(NOW)
            )
        )
        s.commit()
        ban_facts = load_db_facts(s, snap("banned"), NOW, 14)
        grace_facts = load_db_facts(s, snap("graced"), NOW, 14)
    assert ban_facts.squad_ban_active and not ban_facts.grace_active
    assert grace_facts.grace_active and not grace_facts.squad_ban_active
    assert expired(snap("banned"), ban_facts).reason == "temporary_squad_ban_active"
    assert expired(snap("graced"), grace_facts).reason == "grace_access_active"


def test_restored_ban_and_grace_do_not_block(session_factory):
    with session_factory() as s:
        uid = add_user(s, "past", expire_at=days_ago(120)).id
        s.add(
            TemporarySquadBan(
                id=1,
                user_id=uid,
                unban_at=naive(days_ago(200)),
                restored_at=naive(days_ago(199)),
            )
        )
        s.add(
            GraceAccessGrant(
                id=1,
                user_id=uid,
                squad_uuid="grace",
                expires_at=naive(days_ago(119)),
                restored_at=naive(days_ago(119)),
            )
        )
        s.commit()
        result = load_db_facts(s, snap("past"), NOW, 14)
    assert not result.squad_ban_active and not result.grace_active
    assert expired(snap("past"), result).candidate
