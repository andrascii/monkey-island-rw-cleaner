"""CloudPayments (cp_payments / cp_recurrent_payments) в правилах rw-cleaner.

Пользователь с цепочкой автоплатежа CP или недавней оплатой CP защищён ровно
как с ЮKassa; оплата CP в любом статусе (включая возвраты) — «платил» для
ступеней. Нет таблиц CP (миграция не применена) — состояние неоднозначно:
запуск останавливается до перебора панели, а не считает «оплат CP нет»."""

import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import text

from common.autopay_chains import RECURRENT_CHAIN_MODELS
from common.models.db import CP_PAYMENT_STATUS_COMPLETED
from common.models.db import CP_PAYMENT_STATUS_PARTIALLY_REFUNDED
from common.models.db import CP_PAYMENT_STATUS_REFUNDED
from common.models.db import CpPayment
from common.models.db import CpRecurrentPayment
from rw_cleaner.cleanup import EXIT_AUTO_STOP
from rw_cleaner.cleanup import EXIT_OK
from rw_cleaner.cleanup import Cleaner
from rw_cleaner.rules import PAID_CP_STATUSES
from rw_cleaner.rules import PAYMENT_FACT_MODELS
from rw_cleaner.rules import TIER_ALL
from rw_cleaner.rules import TIER_NEVER_CONNECTED
from rw_cleaner.rules import TIER_NEVER_PAID
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
from tests.test_rules import free
from tests.test_rules import free_facts
from tests.test_rules import snap


def add_cp_payment(s, user_id, *, pid, status=CP_PAYMENT_STATUS_COMPLETED, at, captured=None):
    s.add(
        CpPayment(
            id=pid,
            user_id=user_id,
            transaction_id=900000 + pid,
            invoice_id=f"inv-{pid}",
            public_id="pk_terminal",
            amount=249,
            currency="RUB",
            status=status,
            payment_method="Card",
            subscription_period="month",
            created_at=naive(at),
            captured_at=naive(captured) if captured else naive(at),
        )
    )


def add_cp_chain(s, user_id, *, cid=1):
    s.add(
        CpRecurrentPayment(
            id=cid,
            user_id=user_id,
            token=f"tk_secret_{cid}",
            public_id="pk_terminal",
            account_id=str(user_id),
            payment_method="Card",
            amount=249,
            currency="RUB",
            subscription_period="month",
            captured_at=naive(days_ago(300)),
        )
    )


def drop_cp_tables(engine):
    CpRecurrentPayment.__table__.drop(engine)
    CpPayment.__table__.drop(engine)


# --- правила -----------------------------------------------------------------


def test_payment_fact_models_cover_every_autopay_chain_table():
    # Новый шлюз цепочек (common/autopay_chains) проверяется автоматически.
    for model in RECURRENT_CHAIN_MODELS:
        assert model in PAYMENT_FACT_MODELS
    assert CpPayment in PAYMENT_FACT_MODELS
    assert set(PAID_CP_STATUSES) == {
        CP_PAYMENT_STATUS_COMPLETED,
        CP_PAYMENT_STATUS_REFUNDED,
        CP_PAYMENT_STATUS_PARTIALLY_REFUNDED,
    }


def test_cp_chain_is_recurrent_and_blocks_every_tier(session_factory):
    with session_factory() as s:
        u = add_user(s, "cpchain", expire_at=days_ago(120))
        add_cp_chain(s, u.id)
        s.commit()
        facts = load_db_facts(s, snap("cpchain"), NOW, 14)
    assert facts.has_recurrent
    assert not facts.ever_paid
    for tier in (TIER_NEVER_CONNECTED, TIER_NEVER_PAID, TIER_ALL):
        decision = expired(db=facts, tier=tier)
        assert not decision.candidate
        assert decision.reason == "recurrent_payment_saved"


def test_cp_chain_blocks_free_inactive_too():
    assert free().candidate
    decision = free(db=free_facts(has_recurrent=True))
    assert not decision.candidate
    assert decision.reason == "recurrent_payment_saved"


@pytest.mark.parametrize("status", PAID_CP_STATUSES)
def test_cp_payment_in_any_status_counts_as_paid(session_factory, status):
    with session_factory() as s:
        u = add_user(s, f"cp-{status}", expire_at=days_ago(120))
        add_cp_payment(s, u.id, pid=1, status=status, at=days_ago(200))
        s.commit()
        facts = load_db_facts(s, snap(f"cp-{status}"), NOW, 14)
    assert facts.ever_paid
    assert facts.last_payment_at == days_ago(200)
    assert facts.recent_payment is None
    assert not facts.has_recurrent
    assert expired(db=facts, tier=TIER_NEVER_PAID).reason == "tier_paid"
    assert expired(db=facts, tier=TIER_ALL).candidate


def test_cp_last_payment_prefers_captured_at_and_takes_the_latest(session_factory):
    with session_factory() as s:
        u = add_user(s, "cplast", expire_at=days_ago(120))
        add_cp_payment(s, u.id, pid=1, at=days_ago(300), captured=days_ago(301))
        add_cp_payment(s, u.id, pid=2, at=days_ago(100), captured=days_ago(101))
        s.commit()
        facts = load_db_facts(s, snap("cplast"), NOW, 14)
    assert facts.last_payment_at == days_ago(101)


@pytest.mark.parametrize(
    "created, captured",
    [(3, 3), (3, 40), (40, 3)],
    ids=["both_recent", "created_recent", "captured_recent"],
)
def test_recent_cp_payment_blocks(session_factory, created, captured):
    with session_factory() as s:
        u = add_user(s, "cprecent", expire_at=days_ago(120))
        add_cp_payment(
            s,
            u.id,
            pid=1,
            status=CP_PAYMENT_STATUS_REFUNDED,
            at=days_ago(created),
            captured=days_ago(captured),
        )
        s.commit()
        facts = load_db_facts(s, snap("cprecent"), NOW, 14)
    assert facts.recent_payment == "recent_cp_payment"
    assert expired(db=facts, tier=TIER_ALL).reason == "recent_cp_payment"


def test_old_cp_payment_is_not_recent(session_factory):
    with session_factory() as s:
        u = add_user(s, "cpold", expire_at=days_ago(120))
        add_cp_payment(s, u.id, pid=1, at=days_ago(30))
        s.commit()
        assert load_db_facts(s, snap("cpold"), NOW, 14).recent_payment is None


def test_cp_rows_of_another_user_do_not_leak(session_factory):
    with session_factory() as s:
        other = add_user(s, "other", expire_at=days_ago(120))
        add_user(s, "clean", expire_at=days_ago(120))
        add_cp_chain(s, other.id)
        add_cp_payment(s, other.id, pid=1, at=days_ago(2))
        s.commit()
        facts = load_db_facts(s, snap("clean"), NOW, 14)
    assert not facts.has_recurrent
    assert not facts.ever_paid
    assert facts.recent_payment is None


# --- нет таблиц CP: неоднозначно, fail closed ----------------------------------


def test_payment_schema_problems_clean_schema(session_factory):
    with session_factory() as s:
        assert payment_schema_problems(s) == []


def test_payment_schema_problems_lists_missing_cp_tables(engine, session_factory):
    drop_cp_tables(engine)
    with session_factory() as s:
        assert payment_schema_problems(s) == ["cp_payments", "cp_recurrent_payments"]


def test_payment_schema_problems_detects_a_missing_column(engine, session_factory):
    # Таблица есть, но колонки модели нет (миграция применена не та) —
    # тоже неоднозначно.
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE cp_payments DROP COLUMN refunded_amount"))
    with session_factory() as s:
        assert payment_schema_problems(s) == ["cp_payments"]


def test_missing_cp_chain_table_is_an_error_not_no_chain(engine, session_factory):
    # В отличие от common.autopay_chains.user_has_autopay («нет таблицы» =
    # «цепочек нет») rw-cleaner на отсутствующей таблице падает: удаления нет.
    with session_factory() as s:
        add_user(s, "u1", expire_at=days_ago(120))
        s.commit()
    CpRecurrentPayment.__table__.drop(engine)
    with session_factory() as s:
        with pytest.raises(Exception, match="cp_recurrent_payments"):
            load_db_facts(s, snap("u1"), NOW, 14)


@pytest.mark.parametrize("execute", [False, True], ids=["dry_run", "execute"])
def test_missing_cp_tables_stop_expired_run_before_scan(
    tmp_path, engine, session_factory, caplog, execute
):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    drop_cp_tables(engine)

    _, summary = run(make_config(tmp_path, execute=execute), session_factory, rwms)

    assert summary.exit_code == EXIT_AUTO_STOP
    assert summary.stop_reason == "payment_schema_unreadable"
    assert summary.scanned == 0 and summary.candidates == 0
    assert rwms.stream_calls == []
    assert rwms.delete_calls == [] and rwms.update_calls == []
    assert "u1" in rwms.users
    assert archives(session_factory) == []
    assert "payment state is ambiguous" in caplog.text
    assert "cp_payments,cp_recurrent_payments" in caplog.text


def test_missing_cp_tables_stop_free_inactive_run(tmp_path, engine, session_factory):
    enable_free_tier(session_factory, free_tier_inactive_days="30")
    rwms = FakeRwms()
    seed_free(
        session_factory, rwms, "idle", online_at=days_ago(45), switched_at=days_ago(50), fid=1
    )
    drop_cp_tables(engine)

    _, summary = run(
        make_config(tmp_path, mode="free_inactive", execute=True), session_factory, rwms
    )

    assert summary.exit_code == EXIT_AUTO_STOP
    assert summary.stop_reason == "payment_schema_unreadable"
    assert rwms.stream_calls == []
    assert rwms.update_calls == [] and rwms.delete_calls == []
    assert "idle" in rwms.users


# --- запуск: цепочка / оплата CP защищают подписку ----------------------------


def test_cp_chain_user_is_never_deleted(tmp_path, session_factory):
    rwms = FakeRwms()
    uid = seed(session_factory, rwms, "chain")
    seed(session_factory, rwms, "plain")
    with session_factory() as s:
        add_cp_chain(s, uid)
        s.commit()

    _, summary = run(
        make_config(tmp_path, execute=True, tier=TIER_ALL), session_factory, rwms
    )

    assert summary.exit_code == EXIT_OK
    assert rwms.deleted == ["plain"]
    report = {row["username"]: row for row in read_report(tmp_path / "report.csv")}
    assert report["chain"]["reason"] == "recurrent_payment_saved"
    assert [a.username for a in archives(session_factory)] == ["plain"]


def test_recent_cp_payment_user_is_never_deleted(tmp_path, session_factory):
    rwms = FakeRwms()
    uid = seed(session_factory, rwms, "payer")
    with session_factory() as s:
        add_cp_payment(s, uid, pid=1, status=CP_PAYMENT_STATUS_REFUNDED, at=days_ago(1))
        s.commit()

    _, summary = run(
        make_config(tmp_path, execute=True, tier=TIER_ALL), session_factory, rwms
    )

    assert rwms.delete_calls == []
    assert summary.deleted == 0
    assert read_report(tmp_path / "report.csv")[0]["reason"] == "recent_cp_payment"


def test_cp_chain_saved_between_scan_and_delete_is_rechecked(tmp_path, session_factory):
    rwms = FakeRwms()
    uid = seed(session_factory, rwms, "u1")

    def chain_saved(_username):
        with session_factory() as s:
            if s.get(CpRecurrentPayment, 7) is None:
                add_cp_chain(s, uid, cid=7)
                s.commit()

    rwms.on_strict = chain_saved
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert summary.deleted == 0
    assert rwms.delete_calls == []
    assert archives(session_factory) == []
    assert read_report(tmp_path / "report.csv")[0]["action"] == (
        "skipped:no_longer_eligible:recurrent_payment_saved"
    )


def test_cp_payment_committed_before_the_lock_prevents_deletion(
    tmp_path, session_factory, monkeypatch
):
    # Оплата CP пришла уже после COMMIT архива, но до блокировки users —
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

    async def payment_then_lock(row, user_id, archive_id, reused, *rest):
        with session_factory() as s:
            add_cp_payment(s, uid, pid=5, at=NOW - timedelta(minutes=1))
            s.commit()
        return await original(row, user_id, archive_id, reused, *rest)

    monkeypatch.setattr(cleaner, "_delete_under_lock", payment_then_lock)
    summary = asyncio.run(cleaner.run())

    assert rwms.delete_calls == []
    assert "u1" in rwms.users
    assert summary.deleted == 0
    assert read_report(tmp_path / "report.csv")[0]["action"] == (
        "skipped:no_longer_eligible_locked:recent_cp_payment"
    )


def test_cp_chain_blocks_free_inactive_deletion(tmp_path, session_factory):
    enable_free_tier(session_factory, free_tier_inactive_days="30")
    rwms = FakeRwms()
    uid = seed_free(
        session_factory, rwms, "idle", online_at=days_ago(45), switched_at=days_ago(50), fid=1
    )
    with session_factory() as s:
        add_cp_chain(s, uid)
        s.commit()

    _, summary = run(
        make_config(tmp_path, mode="free_inactive", execute=True), session_factory, rwms
    )

    assert summary.exit_code == EXIT_OK
    assert rwms.update_calls == [] and rwms.delete_calls == []
    assert read_report(tmp_path / "report.csv")[0]["reason"] == "recurrent_payment_saved"


def test_cp_token_never_reaches_logs_or_report(tmp_path, session_factory, caplog):
    rwms = FakeRwms()
    uid = seed(session_factory, rwms, "chain")
    with session_factory() as s:
        add_cp_chain(s, uid, cid=3)
        s.commit()
    run(make_config(tmp_path, execute=True), session_factory, rwms)
    report = (tmp_path / "report.csv").read_text(encoding="utf-8")
    assert "tk_secret_3" not in caplog.text
    assert "tk_secret_3" not in report
