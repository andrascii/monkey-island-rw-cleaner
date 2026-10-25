"""Платёжные факты rw-cleaner на настоящем PostgreSQL (CloudPayments).

SQLite не повторяет поведение PostgreSQL в главном для безопасности месте:
ошибка запроса (нет таблицы) обрывает всю транзакцию, а EXISTS по нескольким
таблицам цепочек и coalesce/max по TIMESTAMP — синтаксис PostgreSQL. Здесь те
же правила выполняются на PG: цепочка и оплаты CloudPayments, проверка
платёжных таблиц до перебора панели и отказ (fail closed) без таблиц CP.

Нужна переменная RW_CLEANER_TEST_PG_URL — только одноразовая тестовая БД
(например контейнер `docker run --rm -p 127.0.0.1:55439:5432 ...` и
postgresql+psycopg2://user:pass@127.0.0.1:55439/ykrtest), НИКОГДА не
локальная дев-БД 127.0.0.1:5432 (эталон alembic autogenerate). Модуль создаёт схему rwc_<случайно> со всеми таблицами моделей
common (search_path соединений) и удаляет её после себя. Без переменной
модуль пропускается."""

import asyncio
import os
import uuid
from datetime import timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from common.models.db import CP_PAYMENT_STATUS_PARTIALLY_REFUNDED
from common.models.db import Base
from common.models.db import CpPayment
from common.models.db import CpRecurrentPayment
from common.models.db import WataInvoice
from common.models.db import WataTransaction
from common.models.db import YkPayment
from common.models.db import YkRecurrentPayment
from rw_cleaner.cleanup import EXIT_AUTO_STOP
from rw_cleaner.cleanup import EXIT_OK
from rw_cleaner.cleanup import Cleaner
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
from tests.test_cleaner_run import make_config
from tests.test_cleaner_run import read_report
from tests.test_cleaner_run import seed
from tests.test_cloudpayments import add_cp_chain
from tests.test_cloudpayments import add_cp_payment
from tests.test_rules import snap

PG_URL = os.environ.get("RW_CLEANER_TEST_PG_URL")

pytestmark = pytest.mark.skipif(
    not PG_URL, reason="RW_CLEANER_TEST_PG_URL is not set (disposable PostgreSQL)"
)


@pytest.fixture
def pg_engine():
    schema = f"rwc_{uuid.uuid4().hex[:12]}"
    admin = create_engine(PG_URL)
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(
        PG_URL, connect_args={"options": f"-csearch_path={schema}"}
    )
    try:
        Base.metadata.create_all(engine)
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


@pytest.fixture
def pg_sessions(pg_engine):
    return sessionmaker(bind=pg_engine)


def drop_cp_tables(engine):
    CpRecurrentPayment.__table__.drop(engine)
    CpPayment.__table__.drop(engine)


def run_cleaner(config, session_factory, rwms):
    cleaner = Cleaner(
        config, session_factory=session_factory, rwms=rwms, sleep=Sleeps(), clock=lambda: NOW
    )
    return asyncio.run(cleaner.run())


def test_pg_cp_facts(pg_sessions):
    with pg_sessions() as s:
        chain = add_user(s, "chain", expire_at=days_ago(120))
        payer = add_user(s, "payer", expire_at=days_ago(120))
        both = add_user(s, "both", expire_at=days_ago(120))
        add_user(s, "clean", expire_at=days_ago(120))
        add_cp_chain(s, chain.id, cid=1)
        add_cp_payment(
            s,
            payer.id,
            pid=1,
            status=CP_PAYMENT_STATUS_PARTIALLY_REFUNDED,
            at=days_ago(200),
            captured=days_ago(201),
        )
        add_cp_payment(s, payer.id, pid=2, at=days_ago(3))
        # Цепочки в обеих таблицах — EXISTS не дублирует и не путает.
        add_cp_chain(s, both.id, cid=2)
        s.add(
            YkRecurrentPayment(
                id=1,
                user_id=both.id,
                currency="RUB",
                amount=1,
                recurrent_payment_id="r1",
                subscription_period="month",
                captured_at=naive(days_ago(300)),
                shop_id="shop-1",
            )
        )
        s.add(
            YkPayment(
                id=1,
                user_id=both.id,
                amount=100,
                currency="RUB",
                status="succeeded",
                created_at=naive(days_ago(150)),
                captured_at=naive(days_ago(150)),
                payment_id="p1",
                subscription_period="month",
                shop_id="shop-1",
            )
        )
        s.commit()

        chain_facts = load_db_facts(s, snap("chain"), NOW, 14)
        payer_facts = load_db_facts(s, snap("payer"), NOW, 14)
        both_facts = load_db_facts(s, snap("both"), NOW, 14)
        clean_facts = load_db_facts(s, snap("clean"), NOW, 14)
        assert payment_schema_problems(s) == []

    assert chain_facts.has_recurrent and not chain_facts.ever_paid
    assert payer_facts.ever_paid and not payer_facts.has_recurrent
    assert payer_facts.last_payment_at == days_ago(3)
    assert payer_facts.recent_payment == "recent_cp_payment"
    assert both_facts.has_recurrent
    assert both_facts.last_payment_at == days_ago(150)
    assert not clean_facts.has_recurrent and not clean_facts.ever_paid
    assert clean_facts.recent_payment is None


def test_pg_paw_facts(pg_sessions):
    """PayAnyWay на настоящем PostgreSQL: FK mnt_id на paw_accounts, цепочки
    в трёх таблицах — EXISTS без дублей, оплата в любом статусе — «платил»."""
    from tests.test_payanyway import add_paw_chain
    from tests.test_payanyway import add_paw_payment

    with pg_sessions() as s:
        chain = add_user(s, "pawchain", expire_at=days_ago(120))
        payer = add_user(s, "pawpayer", expire_at=days_ago(120))
        add_user(s, "pawclean", expire_at=days_ago(120))
        add_paw_chain(s, chain.id, cid=1)
        add_cp_chain(s, chain.id, cid=1)
        add_paw_payment(s, payer.id, pid=1, status="Refunded", at=days_ago(200))
        add_paw_payment(s, payer.id, pid=2, at=days_ago(2))
        s.commit()
        chain_facts = load_db_facts(s, snap("pawchain"), NOW, 14)
        payer_facts = load_db_facts(s, snap("pawpayer"), NOW, 14)
        clean_facts = load_db_facts(s, snap("pawclean"), NOW, 14)
        assert payment_schema_problems(s) == []
    assert chain_facts.has_recurrent and not chain_facts.ever_paid
    assert payer_facts.ever_paid and payer_facts.last_payment_at == days_ago(2)
    assert payer_facts.recent_payment == "recent_paw_payment"
    assert not clean_facts.has_recurrent and not clean_facts.ever_paid


def test_pg_missing_cp_tables_are_ambiguous(pg_engine, pg_sessions):
    with pg_sessions() as s:
        add_user(s, "u1", expire_at=days_ago(120))
        s.commit()
    drop_cp_tables(pg_engine)
    with pg_sessions() as s:
        # Проверка откатывает оборванную транзакцию и проверяет остальные.
        assert payment_schema_problems(s) == ["cp_payments", "cp_recurrent_payments"]
        with pytest.raises(Exception, match="cp_"):
            load_db_facts(s, snap("u1"), NOW, 14)


@pytest.mark.parametrize("execute", [False, True], ids=["dry_run", "execute"])
def test_pg_run_stops_without_cp_tables(tmp_path, pg_engine, pg_sessions, execute):
    rwms = FakeRwms()
    seed(pg_sessions, rwms, "u1")
    drop_cp_tables(pg_engine)

    summary = run_cleaner(make_config(tmp_path, execute=execute), pg_sessions, rwms)

    assert summary.exit_code == EXIT_AUTO_STOP
    assert summary.stop_reason == "payment_schema_unreadable"
    assert rwms.stream_calls == [] and rwms.delete_calls == []
    assert "u1" in rwms.users


def test_pg_run_keeps_cp_chain_and_deletes_plain(tmp_path, pg_sessions):
    rwms = FakeRwms()
    uid = seed(pg_sessions, rwms, "chain", user_id=501)
    seed(pg_sessions, rwms, "plain", user_id=502)
    with pg_sessions() as s:
        add_cp_chain(s, uid, cid=9)
        s.commit()

    summary = run_cleaner(make_config(tmp_path, execute=True, tier=TIER_ALL), pg_sessions, rwms)

    assert summary.exit_code == EXIT_OK
    assert rwms.deleted == ["plain"]
    assert "chain" in rwms.users
    report = {row["username"]: row for row in read_report(tmp_path / "report.csv")}
    assert report["chain"]["reason"] == "recurrent_payment_saved"
    assert [a.username for a in archives(pg_sessions)] == ["plain"]


def test_pg_recent_bot_invoice_blocks(pg_sessions):
    # Счёт CloudPayments из бота до оплаты оставляет только event_logs
    # create_invoice_<slug> (ревью CP, круг 1). Бот пишет строку без
    # timestamp — его ставит server_default now(); проверяем и такую строку
    # (при реальном «сейчас»), и явную метку времени.
    from datetime import datetime
    from datetime import timezone

    from common.models.db import EventLog

    real_now = datetime.now(timezone.utc)
    with pg_sessions() as s:
        fresh = add_user(s, "fresh", expire_at=days_ago(120))
        old = add_user(s, "old", expire_at=days_ago(120))
        other = add_user(s, "other", expire_at=days_ago(120))
        s.add(
            EventLog(
                user_id=fresh.id,
                event_type="create_invoice_one_month",
                event_payload={"event_type": "create_invoice_one_month"},
            )
        )
        s.add(
            EventLog(
                user_id=old.id,
                event_type="create_invoice_one_month",
                event_payload={},
                timestamp=naive(real_now - timedelta(days=20)),
            )
        )
        s.add(
            EventLog(
                user_id=other.id,
                event_type="createXinvoice_one_month",
                event_payload={},
                timestamp=naive(real_now),
            )
        )
        s.commit()
        assert payment_schema_problems(s) == []
        fresh_facts = load_db_facts(s, snap("fresh"), real_now, 14)
        old_facts = load_db_facts(s, snap("old"), real_now, 14)
        other_facts = load_db_facts(s, snap("other"), real_now, 14)
    assert fresh_facts.recent_payment == "recent_bot_invoice"
    assert old_facts.recent_payment is None
    assert other_facts.recent_payment is None


def test_pg_run_keeps_bot_invoice_user(tmp_path, pg_sessions):
    from common.models.db import EventLog

    rwms = FakeRwms()
    uid = seed(pg_sessions, rwms, "payer", user_id=701)
    seed(pg_sessions, rwms, "plain", user_id=702)
    with pg_sessions() as s:
        s.add(
            EventLog(
                user_id=uid,
                event_type="create_invoice_one_month",
                event_payload={},
                timestamp=naive(NOW - timedelta(hours=2)),
            )
        )
        s.commit()

    summary = run_cleaner(make_config(tmp_path, execute=True, tier=TIER_ALL), pg_sessions, rwms)

    assert summary.exit_code == EXIT_OK
    assert rwms.deleted == ["plain"]
    assert "payer" in rwms.users
    report = {row["username"]: row for row in read_report(tmp_path / "report.csv")}
    assert report["payer"]["reason"] == "recent_bot_invoice"


def _seed_refunded_payers(pg_sessions, rwms, connected):
    """Три пользователя, чья единственная оплата полностью возвращена (по
    шлюзу на каждого), и один никогда не плативший — все истекли >90 дней."""
    panel = (
        dict(first_connected=days_ago(160), online_at=days_ago(140), lifetime_traffic=1e9)
        if connected
        else {}
    )
    paid_at = days_ago(150)
    wata = seed(pg_sessions, rwms, "wata_refunded", user_id=801, **panel)
    yk = seed(pg_sessions, rwms, "yk_refunded", user_id=802, **panel)
    cp = seed(pg_sessions, rwms, "cp_refunded", user_id=803, **panel)
    seed(pg_sessions, rwms, "never_paid", user_id=804, **panel)
    with pg_sessions() as s:
        s.add(
            WataInvoice(
                user_id=wata, invoice_id="wi-1", amount=249, currency="RUB",
                status="Paid", terminal_name="t", terminal_public_id="tp",
                creation_time=paid_at, order_id="order-1",
                expiration_datetime=paid_at + timedelta(days=1),
            )
        )
        # payment/wata_refunds.py при полном возврате: Paid -> Refunded.
        s.add(
            WataTransaction(
                transaction_id="wt-1", transaction_type="CardCrypto",
                terminal_public_id="tp", transaction_status="Refunded",
                terminal_name="t", amount=249, currency="RUB", order_id="order-1",
                order_description="month", commission=0, payment_time=paid_at,
            )
        )
        s.add(
            YkPayment(
                user_id=yk, amount=249, currency="RUB", status="refunded",
                created_at=naive(paid_at), captured_at=naive(paid_at),
                payment_id="yk-1", subscription_period="month", shop_id="shop-1",
            )
        )
        s.commit()
        add_cp_payment(s, cp, pid=1, status="Refunded", at=paid_at)
        s.commit()


@pytest.mark.parametrize(
    "tier,connected",
    [(TIER_NEVER_CONNECTED, False), (TIER_NEVER_PAID, True)],
    ids=["never_connected", "never_paid"],
)
def test_pg_fully_refunded_payer_is_paid_for_narrow_tiers(
    tmp_path, pg_sessions, tier, connected
):
    # Ревью CP, круг 7, R1: полностью вернувший деньги через WATA (строка
    # wata_transactions в Refunded) — «платил», как вернувшие через ЮKassa и
    # CloudPayments; в узких ступенях не удаляется. Удаляется только тот, кто
    # не платил никогда.
    rwms = FakeRwms()
    _seed_refunded_payers(pg_sessions, rwms, connected)

    summary = run_cleaner(make_config(tmp_path, execute=True, tier=tier), pg_sessions, rwms)

    assert summary.exit_code == EXIT_OK
    assert rwms.deleted == ["never_paid"]
    for name in ("wata_refunded", "yk_refunded", "cp_refunded"):
        assert name in rwms.users
    report = {row["username"]: row for row in read_report(tmp_path / "report.csv")}
    for name in ("wata_refunded", "yk_refunded", "cp_refunded"):
        assert report[name]["reason"] == "tier_paid"
        assert report[name]["ever_paid"] == "yes"
    assert [a.username for a in archives(pg_sessions)] == ["never_paid"]
