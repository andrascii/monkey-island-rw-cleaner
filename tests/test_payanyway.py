"""PayAnyWay (paw_payments / paw_recurrent_payments) в правилах rw-cleaner —
ровно как CloudPayments (tests/test_cloudpayments.py): цепочка или недавняя
оплата защищают подписку, оплата в любом статусе — «платил» для ступеней,
нет таблиц — остановка до перебора панели."""


import pytest

from common.models.db import PAW_PAYMENT_STATUS_COMPLETED
from common.models.db import PAW_PAYMENT_STATUS_PARTIALLY_REFUNDED
from common.models.db import PAW_PAYMENT_STATUS_REFUNDED
from common.models.db import PawAccount
from common.models.db import PawPayment
from common.models.db import PawRecurrentPayment
from rw_cleaner.cleanup import EXIT_AUTO_STOP
from rw_cleaner.cleanup import EXIT_OK
from rw_cleaner.rules import PAID_PAW_STATUSES
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
from tests.test_cleaner_run import archives
from tests.test_cleaner_run import make_config
from tests.test_cleaner_run import read_report
from tests.test_cleaner_run import run
from tests.test_cleaner_run import seed
from tests.test_rules import expired
from tests.test_rules import snap

MNT_ID = "86633580"


def _account(s):
    if s.get(PawAccount, MNT_ID) is None:
        s.add(PawAccount(mnt_id=MNT_ID, integrity_code="code-12345"))
        s.flush()


def add_paw_payment(s, user_id, *, pid, status=PAW_PAYMENT_STATUS_COMPLETED, at, captured=None):
    _account(s)
    s.add(
        PawPayment(
            id=pid,
            user_id=user_id,
            operation_id=700000 + pid,
            transaction_id=f"mi-t-{pid}",
            mnt_id=MNT_ID,
            amount=249,
            currency="RUB",
            status=status,
            payment_method="sbp",
            subscription_period="month",
            created_at=naive(at),
            captured_at=naive(captured) if captured else naive(at),
        )
    )


def add_paw_chain(s, user_id, *, cid=1):
    _account(s)
    s.add(
        PawRecurrentPayment(
            id=cid,
            user_id=user_id,
            token=f"pt_secret_{cid}",
            token_kind="sbp",
            mnt_id=MNT_ID,
            payment_method="sbp",
            amount=249,
            currency="RUB",
            subscription_period="month",
            captured_at=naive(days_ago(300)),
        )
    )


def drop_paw_tables(engine):
    PawRecurrentPayment.__table__.drop(engine)
    PawPayment.__table__.drop(engine)


def test_paw_tables_are_payment_facts():
    assert PawPayment in PAYMENT_FACT_MODELS
    assert PawRecurrentPayment in PAYMENT_FACT_MODELS
    assert set(PAID_PAW_STATUSES) == {
        PAW_PAYMENT_STATUS_COMPLETED,
        PAW_PAYMENT_STATUS_REFUNDED,
        PAW_PAYMENT_STATUS_PARTIALLY_REFUNDED,
    }


def test_paw_chain_is_recurrent_and_blocks_every_tier(session_factory):
    with session_factory() as s:
        u = add_user(s, "pawchain", expire_at=days_ago(120))
        add_paw_chain(s, u.id)
        s.commit()
        facts = load_db_facts(s, snap("pawchain"), NOW, 14)
    assert facts.has_recurrent
    for tier in (TIER_NEVER_CONNECTED, TIER_NEVER_PAID, TIER_ALL):
        assert expired(db=facts, tier=tier).reason == "recurrent_payment_saved"


@pytest.mark.parametrize("status", PAID_PAW_STATUSES)
def test_paw_payment_in_any_status_counts_as_paid(session_factory, status):
    with session_factory() as s:
        u = add_user(s, f"paw-{status}", expire_at=days_ago(120))
        add_paw_payment(s, u.id, pid=1, status=status, at=days_ago(200))
        s.commit()
        facts = load_db_facts(s, snap(f"paw-{status}"), NOW, 14)
    assert facts.ever_paid
    assert facts.last_payment_at == days_ago(200)
    assert facts.recent_payment is None
    assert expired(db=facts, tier=TIER_NEVER_PAID).reason == "tier_paid"
    assert expired(db=facts, tier=TIER_ALL).candidate


@pytest.mark.parametrize(
    "created, captured",
    [(3, 3), (3, 40), (40, 3)],
    ids=["both_recent", "created_recent", "captured_recent"],
)
def test_recent_paw_payment_blocks(session_factory, created, captured):
    with session_factory() as s:
        u = add_user(s, "pawrecent", expire_at=days_ago(120))
        add_paw_payment(
            s, u.id, pid=1, status=PAW_PAYMENT_STATUS_REFUNDED,
            at=days_ago(created), captured=days_ago(captured),
        )
        s.commit()
        facts = load_db_facts(s, snap("pawrecent"), NOW, 14)
    assert facts.recent_payment == "recent_paw_payment"
    assert expired(db=facts, tier=TIER_ALL).reason == "recent_paw_payment"


def test_paw_rows_of_another_user_do_not_leak(session_factory):
    with session_factory() as s:
        other = add_user(s, "other", expire_at=days_ago(120))
        add_user(s, "clean", expire_at=days_ago(120))
        add_paw_chain(s, other.id)
        add_paw_payment(s, other.id, pid=1, at=days_ago(2))
        s.commit()
        facts = load_db_facts(s, snap("clean"), NOW, 14)
    assert not facts.has_recurrent
    assert not facts.ever_paid
    assert facts.recent_payment is None


def test_payment_schema_problems_lists_missing_paw_tables(engine, session_factory):
    drop_paw_tables(engine)
    with session_factory() as s:
        assert payment_schema_problems(s) == ["paw_payments", "paw_recurrent_payments"]


@pytest.mark.parametrize("execute", [False, True], ids=["dry_run", "execute"])
def test_missing_paw_tables_stop_run_before_scan(tmp_path, engine, session_factory, execute):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    drop_paw_tables(engine)

    _, summary = run(make_config(tmp_path, execute=execute), session_factory, rwms)

    assert summary.exit_code == EXIT_AUTO_STOP
    assert summary.stop_reason == "payment_schema_unreadable"
    assert rwms.stream_calls == []
    assert rwms.delete_calls == [] and rwms.update_calls == []
    assert archives(session_factory) == []


def test_paw_chain_and_recent_payment_users_are_never_deleted(tmp_path, session_factory):
    rwms = FakeRwms()
    chain_uid = seed(session_factory, rwms, "chain")
    payer_uid = seed(session_factory, rwms, "payer")
    seed(session_factory, rwms, "plain")
    with session_factory() as s:
        add_paw_chain(s, chain_uid)
        add_paw_payment(s, payer_uid, pid=1, at=days_ago(1))
        s.commit()

    _, summary = run(make_config(tmp_path, execute=True, tier=TIER_ALL), session_factory, rwms)

    assert summary.exit_code == EXIT_OK
    assert rwms.deleted == ["plain"]
    report = {row["username"]: row for row in read_report(tmp_path / "report.csv")}
    assert report["chain"]["reason"] == "recurrent_payment_saved"
    assert report["payer"]["reason"] == "recent_paw_payment"


def test_old_paw_payer_is_kept_in_never_paid_tier(tmp_path, session_factory):
    rwms = FakeRwms()
    uid = seed(session_factory, rwms, "oldpayer")
    with session_factory() as s:
        add_paw_payment(s, uid, pid=1, at=days_ago(200))
        s.commit()

    _, summary = run(
        make_config(tmp_path, execute=True, tier=TIER_NEVER_PAID), session_factory, rwms
    )

    assert rwms.delete_calls == []
    assert read_report(tmp_path / "report.csv")[0]["reason"] == "tier_paid"


def _add_paw_attempt(s, user_id, ref, *, marked, at):
    from common.models.db import WebsitePaymentAttempt

    payload = {"transaction_id": ref}
    if marked:
        payload["money_moved_at"] = 1_700_000_000
    s.add(WebsitePaymentAttempt(
        id=f"a-{ref}", user_id=user_id, fingerprint="f", gateway="paw", tariff_id="month",
        state="ready", request_payload=payload, status_token_hash=f"h-{ref}",
        status_url="https://x", provider_reference=ref, created_at=naive(at), attempts=0,
    ))


def test_money_seen_at_payanyway_without_a_payment_blocks_deletion_forever(session_factory):
    # Деньги видели у МОНЕТЫ (отметка сайта), Pay потерян: давно, а подписку
    # всё равно не трогаем.
    with session_factory() as s:
        u = add_user(s, "pawlost", expire_at=days_ago(400))
        _add_paw_attempt(s, u.id, "mi-paw-site-lost", marked=True, at=days_ago(300))
        v = add_user(s, "pawplain", expire_at=days_ago(400))
        _add_paw_attempt(s, v.id, "mi-paw-site-plain", marked=False, at=days_ago(300))
        w = add_user(s, "pawdone", expire_at=days_ago(400))
        _add_paw_attempt(s, w.id, "mi-paw-site-done", marked=True, at=days_ago(300))
        _account(s)
        s.flush()
        s.add(PawPayment(
            id=77, user_id=w.id, operation_id=77, transaction_id="mi-paw-site-done", mnt_id="86633580",
            amount=249, status=PAW_PAYMENT_STATUS_COMPLETED, subscription_period="month",
            created_at=naive(days_ago(300)), captured_at=naive(days_ago(300)),
        ))
        s.commit()
        assert load_db_facts(s, snap("pawlost"), NOW, 14).recent_payment == "paw_money_moved_unrecorded"
        assert load_db_facts(s, snap("pawplain"), NOW, 14).recent_payment is None
        assert load_db_facts(s, snap("pawdone"), NOW, 14).recent_payment is None
