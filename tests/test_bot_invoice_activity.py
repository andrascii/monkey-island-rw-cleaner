"""Счёт, выставленный в боте (event_logs create_invoice_*), — недавняя
платёжная активность (ревью CloudPayments, круг 1).

Счёт CloudPayments из бота до оплаты не оставляет следов в платёжных таблицах:
строка cp_payments появляется только на Pay, website_payment_attempts пишет
только сайт. Единственный след — событие create_invoice_<slug>, которое бот
пишет при нажатии тарифа. Без него rw-cleaner удалил бы подписку человека,
который прямо сейчас оплачивает счёт CP из бота. Таблица event_logs входит в
проверку платёжных таблиц: не читается — остановка до перебора панели."""

import asyncio
from datetime import timedelta

import pytest

from common.models.db import EventLog
from rw_cleaner.cleanup import EXIT_AUTO_STOP
from rw_cleaner.cleanup import EXIT_OK
from rw_cleaner.cleanup import Cleaner
from rw_cleaner.rules import BOT_INVOICE_EVENT_PREFIX
from rw_cleaner.rules import PAYMENT_FACT_MODELS
from rw_cleaner.rules import TIER_ALL
from rw_cleaner.rules import load_db_facts
from rw_cleaner.rules import payment_schema_problems
from tests.helpers import NOW
from tests.helpers import FakeRwms
from tests.helpers import add_user
from tests.helpers import days_ago
from tests.helpers import naive
from tests.test_cleaner_run import Sleeps
from tests.test_cleaner_run import archives
from tests.test_cleaner_run import enable_free_tier
from tests.test_cleaner_run import make_config
from tests.test_cleaner_run import read_report
from tests.test_cleaner_run import run
from tests.test_cleaner_run import seed
from tests.test_cleaner_run import seed_free
from tests.test_rules import expired
from tests.test_rules import snap

# Слаги, которые реально пишет бот (handlers/tariffs.py: встроенные тарифы и
# create_invoice_<slug> тарифов реестра).
BOT_INVOICE_EVENTS = (
    "create_invoice_one_day",
    "create_invoice_one_month",
    "create_invoice_one_year",
    "create_invoice_intro99",
)


def add_event(s, user_id, event_type, *, eid, at):
    s.add(
        EventLog(
            id=eid,
            user_id=user_id,
            event_type=event_type,
            event_payload={"event_type": event_type},
            timestamp=naive(at),
        )
    )


def test_event_logs_is_a_payment_fact_table():
    assert EventLog in PAYMENT_FACT_MODELS
    assert BOT_INVOICE_EVENT_PREFIX == "create_invoice"


@pytest.mark.parametrize("event_type", BOT_INVOICE_EVENTS)
def test_recent_bot_invoice_blocks_every_tier(session_factory, event_type):
    with session_factory() as s:
        u = add_user(s, "payer", expire_at=days_ago(120))
        add_event(s, u.id, event_type, eid=1, at=days_ago(2))
        s.commit()
        facts = load_db_facts(s, snap("payer"), NOW, 14)
    assert facts.recent_payment == "recent_bot_invoice"
    decision = expired(db=facts, tier=TIER_ALL)
    assert not decision.candidate
    assert decision.reason == "recent_bot_invoice"


def test_old_bot_invoice_and_other_events_do_not_block(session_factory):
    with session_factory() as s:
        u = add_user(s, "quiet", expire_at=days_ago(120))
        other = add_user(s, "other", expire_at=days_ago(120))
        add_event(s, u.id, "create_invoice_one_month", eid=1, at=days_ago(15))
        add_event(s, u.id, "show_tariffs_clicked", eid=2, at=days_ago(1))
        add_event(s, u.id, "subscription_expired", eid=3, at=days_ago(1))
        # «_» в префиксе — не подстановочный символ LIKE.
        add_event(s, u.id, "createXinvoice_month", eid=4, at=days_ago(1))
        # Счёт другого пользователя не протекает.
        add_event(s, other.id, "create_invoice_one_month", eid=5, at=days_ago(1))
        s.commit()
        facts = load_db_facts(s, snap("quiet"), NOW, 14)
    assert facts.recent_payment is None
    assert expired(db=facts, tier=TIER_ALL).candidate


def test_bot_invoice_window_follows_recent_payment_days(session_factory):
    with session_factory() as s:
        u = add_user(s, "w", expire_at=days_ago(120))
        add_event(s, u.id, "create_invoice_one_month", eid=1, at=days_ago(20))
        s.commit()
        assert load_db_facts(s, snap("w"), NOW, 14).recent_payment is None
        assert load_db_facts(s, snap("w"), NOW, 30).recent_payment == "recent_bot_invoice"


def test_bot_invoice_user_is_never_deleted(tmp_path, session_factory):
    rwms = FakeRwms()
    uid = seed(session_factory, rwms, "payer", user_id=601)
    seed(session_factory, rwms, "plain", user_id=602)
    with session_factory() as s:
        add_event(s, uid, "create_invoice_one_month", eid=1, at=NOW - timedelta(hours=1))
        s.commit()

    _, summary = run(make_config(tmp_path, execute=True, tier=TIER_ALL), session_factory, rwms)

    assert summary.exit_code == EXIT_OK
    assert rwms.deleted == ["plain"]
    assert "payer" in rwms.users
    report = {row["username"]: row for row in read_report(tmp_path / "report.csv")}
    assert report["payer"]["reason"] == "recent_bot_invoice"
    assert [a.username for a in archives(session_factory)] == ["plain"]


def test_bot_invoice_blocks_free_inactive_too(tmp_path, session_factory):
    enable_free_tier(session_factory, free_tier_inactive_days="30")
    rwms = FakeRwms()
    uid = seed_free(
        session_factory, rwms, "idle", online_at=days_ago(45), switched_at=days_ago(50), fid=1
    )
    with session_factory() as s:
        add_event(s, uid, "create_invoice_one_month", eid=1, at=days_ago(1))
        s.commit()

    _, summary = run(
        make_config(tmp_path, mode="free_inactive", execute=True), session_factory, rwms
    )

    assert summary.exit_code == EXIT_OK
    assert rwms.update_calls == [] and rwms.delete_calls == []
    assert read_report(tmp_path / "report.csv")[0]["reason"] == "recent_bot_invoice"


def test_bot_invoice_committed_before_the_lock_prevents_deletion(
    tmp_path, session_factory, monkeypatch
):
    # Счёт выставлен в боте уже после COMMIT архива, но до блокировки users —
    # под блокировкой факты перечитываются, DeleteUser не уходит.
    rwms = FakeRwms()
    uid = seed(session_factory, rwms, "u1")
    cleaner = Cleaner(
        make_config(tmp_path, execute=True),
        session_factory=session_factory,
        rwms=rwms,
        sleep=Sleeps(),
        clock=lambda: NOW,
    )
    original = cleaner._delete_under_lock

    async def invoice_then_lock(row, user_id, archive_id, reused, *rest):
        with session_factory() as s:
            add_event(s, uid, "create_invoice_one_month", eid=9, at=NOW - timedelta(minutes=1))
            s.commit()
        return await original(row, user_id, archive_id, reused, *rest)

    monkeypatch.setattr(cleaner, "_delete_under_lock", invoice_then_lock)
    summary = asyncio.run(cleaner.run())

    assert rwms.delete_calls == []
    assert "u1" in rwms.users
    assert summary.deleted == 0
    assert read_report(tmp_path / "report.csv")[0]["action"] == (
        "skipped:no_longer_eligible_locked:recent_bot_invoice"
    )


@pytest.mark.parametrize("execute", [False, True], ids=["dry_run", "execute"])
def test_unreadable_event_logs_stop_the_run_before_scan(
    tmp_path, engine, session_factory, caplog, execute
):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    EventLog.__table__.drop(engine)
    with session_factory() as s:
        assert payment_schema_problems(s) == ["event_logs"]

    _, summary = run(make_config(tmp_path, execute=execute), session_factory, rwms)

    assert summary.exit_code == EXIT_AUTO_STOP
    assert summary.stop_reason == "payment_schema_unreadable"
    assert rwms.stream_calls == []
    assert rwms.delete_calls == [] and rwms.update_calls == []
    assert "u1" in rwms.users
    assert "event_logs" in caplog.text
