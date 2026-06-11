from datetime import datetime
from datetime import timedelta
from datetime import timezone
from types import SimpleNamespace
from unittest import mock

from common.models.db import User
from rw_cleaner.cleanup import CleanerConfig
from rw_cleaner.cleanup import CleanupDecision
from rw_cleaner.cleanup import RwmsUserSnapshot
from rw_cleaner.cleanup import decide_cleanup
from rw_cleaner.cleanup import parse_args
from rw_cleaner.cleanup import verify_still_eligible


def make_config(**overrides):
    values = {
        "database_url": "postgresql+psycopg2://example/db",
        "rwms_host": "127.0.0.1",
        "rwms_port": 50052,
    }
    values.update(overrides)
    return CleanerConfig(**values)


def make_rwms_user(**overrides):
    values = {
        "uuid": "rwms-uuid",
        "username": "user-1",
        "status": "EXPIRED",
        "expire_at": datetime.now(timezone.utc) - timedelta(days=45),
        "email": "user@example.com",
        "telegram_id": None,
    }
    values.update(overrides)
    return RwmsUserSnapshot(**values)


def make_expired_local_user(days_ago: int = 45):
    return SimpleNamespace(
        id=42,
        expire_at=datetime.now(timezone.utc).replace(tzinfo=None)
        - timedelta(days=days_ago),
    )


class FakeQuery:
    def __init__(self, result):
        self.result = result

    def filter(self, *args):
        return self

    def first(self):
        return self.result

    def scalar(self):
        return self.result


class FakeSession:
    """Dispatches by queried type: User queries return local_user, all others return False."""

    def __init__(self, local_user=None):
        self.local_user = local_user

    def query(self, *args):
        if args and args[0] is User:
            return FakeQuery(self.local_user)
        return FakeQuery(False)


def test_skip_when_rwms_expire_at_is_missing():
    decision = decide_cleanup(
        FakeSession(), make_rwms_user(expire_at=None), make_config()
    )

    assert not decision.should_delete
    assert decision.reason == "rwms_expire_at_missing"


def test_skip_when_rwms_subscription_is_not_old_enough():
    decision = decide_cleanup(
        FakeSession(),
        make_rwms_user(expire_at=datetime.now(timezone.utc) - timedelta(days=5)),
        make_config(),
    )

    assert not decision.should_delete
    assert decision.reason == "rwms_not_old_enough"


def test_skip_when_missing_rwms_identity():
    decision = decide_cleanup(
        FakeSession(), make_rwms_user(uuid="", username=""), make_config()
    )

    assert not decision.should_delete
    assert decision.reason == "missing_rwms_identity"


def test_skip_when_rwms_status_is_active_despite_old_expire_at():
    decision = decide_cleanup(
        FakeSession(),
        make_rwms_user(status="ACTIVE"),
        make_config(),
    )

    assert not decision.should_delete
    assert decision.reason == "rwms_status_active_mismatch"


def test_skip_orphan_by_default():
    decision = decide_cleanup(
        FakeSession(local_user=None), make_rwms_user(), make_config()
    )

    assert not decision.should_delete
    assert decision.reason == "local_user_not_found"


def test_allow_orphan_when_explicitly_enabled():
    decision = decide_cleanup(
        FakeSession(local_user=None),
        make_rwms_user(),
        make_config(include_orphans=True),
    )

    assert decision.should_delete
    assert decision.reason == "orphan_rwms_user"


def test_skip_when_local_expire_at_is_missing():
    local_user = SimpleNamespace(id=42, expire_at=None)
    decision = decide_cleanup(
        FakeSession(local_user=local_user), make_rwms_user(), make_config()
    )

    assert not decision.should_delete
    assert decision.reason == "local_expire_at_missing"


def test_skip_when_local_user_is_active():
    local_user = SimpleNamespace(
        id=42,
        expire_at=datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(days=2),
    )
    decision = decide_cleanup(
        FakeSession(local_user=local_user), make_rwms_user(), make_config()
    )

    assert not decision.should_delete
    assert decision.reason == "local_user_active"


def test_skip_when_local_not_old_enough():
    local_user = SimpleNamespace(
        id=42,
        expire_at=datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=5),
    )
    decision = decide_cleanup(
        FakeSession(local_user=local_user), make_rwms_user(), make_config()
    )

    assert not decision.should_delete
    assert decision.reason == "local_not_old_enough"


def test_skip_when_pending_yk_payment():
    with mock.patch("rw_cleaner.cleanup.has_pending_yk_payment", return_value=True):
        decision = decide_cleanup(
            FakeSession(local_user=make_expired_local_user()),
            make_rwms_user(),
            make_config(),
        )

    assert not decision.should_delete
    assert decision.reason == "pending_yk_payment"


def test_skip_when_pending_wata_invoice():
    with mock.patch("rw_cleaner.cleanup.has_pending_yk_payment", return_value=False):
        with mock.patch(
            "rw_cleaner.cleanup.has_pending_wata_invoice", return_value=True
        ):
            decision = decide_cleanup(
                FakeSession(local_user=make_expired_local_user()),
                make_rwms_user(),
                make_config(),
            )

    assert not decision.should_delete
    assert decision.reason == "pending_wata_invoice"


def test_skip_when_scheduled_recurrent_payment():
    with mock.patch("rw_cleaner.cleanup.has_pending_yk_payment", return_value=False):
        with mock.patch(
            "rw_cleaner.cleanup.has_pending_wata_invoice", return_value=False
        ):
            with mock.patch(
                "rw_cleaner.cleanup.has_scheduled_recurrent_payment", return_value=True
            ):
                decision = decide_cleanup(
                    FakeSession(local_user=make_expired_local_user()),
                    make_rwms_user(),
                    make_config(),
                )

    assert not decision.should_delete
    assert decision.reason == "scheduled_recurrent_payment"


def test_delete_when_rwms_and_local_user_are_expired_past_grace_period():
    with mock.patch("rw_cleaner.cleanup.has_pending_yk_payment", return_value=False):
        with mock.patch(
            "rw_cleaner.cleanup.has_pending_wata_invoice", return_value=False
        ):
            with mock.patch(
                "rw_cleaner.cleanup.has_scheduled_recurrent_payment", return_value=False
            ):
                decision = decide_cleanup(
                    FakeSession(local_user=make_expired_local_user()),
                    make_rwms_user(),
                    make_config(),
                )

    assert decision.should_delete
    assert decision.reason == "expired_in_rwms_and_local_db"
    assert decision.local_user_id == 42


def test_skip_when_username_is_not_in_allowlist():
    decision = decide_cleanup(
        FakeSession(),
        make_rwms_user(username="not-allowed"),
        make_config(username_allowlist=frozenset({"allowed"})),
    )

    assert not decision.should_delete
    assert decision.reason == "not_in_allowlist"


def test_parse_args_requires_explicit_execute_flag_even_when_env_is_true(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg2://example/db")
    monkeypatch.setenv("RWMS_HOST", "127.0.0.1")
    monkeypatch.setenv("RWMS_PORT", "50052")
    monkeypatch.setenv("EXECUTE", "true")
    monkeypatch.setenv("INCLUDE_ORPHANS", "true")

    config = parse_args([])

    assert config.execute is False
    assert config.include_orphans is False


def test_parse_args_accepts_explicit_destructive_flags(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg2://example/db")
    monkeypatch.setenv("RWMS_HOST", "127.0.0.1")
    monkeypatch.setenv("RWMS_PORT", "50052")

    config = parse_args(["--execute", "--include-orphans"])

    assert config.execute is True
    assert config.include_orphans is True


class FakeRwmsClient:
    def __init__(self, user):
        self.user = user

    def get_user_by_uuid(self, uuid):
        return self.user


def test_verify_still_eligible_reruns_full_decision_before_delete():
    rwms_user = make_rwms_user()
    decision = CleanupDecision(
        rwms_user=rwms_user,
        should_delete=True,
        reason="expired_in_rwms_and_local_db",
        local_user_id=42,
        local_expire_at=rwms_user.expire_at,
    )
    proto_user = SimpleNamespace(
        uuid=rwms_user.uuid,
        username=rwms_user.username,
        status=3,
        expire_at=SimpleNamespace(
            seconds=int(rwms_user.expire_at.timestamp()),
            nanos=0,
            ToDatetime=lambda: rwms_user.expire_at.replace(tzinfo=None),
        ),
        email=rwms_user.email,
        telegram_id=0,
        HasField=lambda field: field in {"status", "expire_at", "email"},
    )

    with mock.patch("rw_cleaner.cleanup.has_pending_yk_payment", return_value=True):
        assert (
            verify_still_eligible(
                FakeRwmsClient(proto_user),
                FakeSession(local_user=make_expired_local_user()),
                decision,
                make_config(),
            )
            is False
        )
