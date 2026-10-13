"""Настройки (parse_args), блокировка одного запуска, снимок панели."""

import asyncio
from unittest import mock

import pytest

import proto.rwmanager_pb2 as proto
from rw_cleaner.cleanup import ADVISORY_LOCK_KEY
from rw_cleaner.cleanup import EXIT_LOCKED
from rw_cleaner.cleanup import EXIT_OK
from rw_cleaner.cleanup import AdvisoryLock
from rw_cleaner.cleanup import parse_args
from rw_cleaner.cleanup import run_locked
from rw_cleaner.panel import PanelSnapshot
from rw_cleaner.panel import snapshot_from_proto
from tests.helpers import days_ago
from tests.helpers import panel_user


@pytest.fixture
def base_env(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg2://example/db")
    monkeypatch.setenv("RWMS_HOST", "127.0.0.1")
    monkeypatch.setenv("RWMS_PORT", "50052")
    for name in (
        "GRACE_DAYS",
        "MAX_DELETIONS",
        "CLEANER_MODE",
        "CLEANER_TIER",
        "PAGE_SIZE",
        "MAX_CANDIDATE_RATIO",
        "MAX_FREE_CANDIDATE_RATIO",
        "FREE_SETTLE_SECONDS",
        "STATE_PATH",
    ):
        monkeypatch.delenv(name, raising=False)


# --- parse_args --------------------------------------------------------------


def test_safe_defaults(base_env):
    config = parse_args([])
    assert config.execute is False
    assert not hasattr(config, "include_orphans")
    assert config.free_settle_seconds == 180.0
    assert config.state_path is None
    assert config.mode == "expired"
    assert config.tier == "never_connected"
    assert config.grace_days == 90
    assert config.expire_tolerance_hours == 24
    assert config.recent_payment_days == 14
    assert config.max_deletions == 25
    assert config.max_consecutive_failures == 3
    assert config.max_candidate_ratio == 0.6
    assert config.max_free_candidate_ratio == 0.9
    assert config.rwms_timeout_seconds > 0 and config.stream_timeout_seconds > 0


def test_env_cannot_enable_destructive_flags(base_env, monkeypatch):
    monkeypatch.setenv("EXECUTE", "true")
    monkeypatch.setenv("INCLUDE_ORPHANS", "true")
    config = parse_args([])
    assert config.execute is False


def test_explicit_destructive_flags(base_env):
    config = parse_args(["--execute", "--full-scan"])
    assert config.execute and config.full_scan


def test_include_orphans_was_removed_with_a_clear_error(base_env, capsys):
    # R4: сироты не удаляются никогда; флаг — ошибка с объяснением, а не
    # молчаливое игнорирование.
    with pytest.raises(SystemExit) as exc:
        parse_args(["--execute", "--include-orphans"])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "--include-orphans was removed" in err
    assert "never deleted" in err


def test_free_settle_and_state_path_from_env_and_flag(base_env, monkeypatch):
    monkeypatch.setenv("FREE_SETTLE_SECONDS", "300")
    monkeypatch.setenv("STATE_PATH", "/data/state.json")
    config = parse_args([])
    assert config.free_settle_seconds == 300.0
    assert config.state_path == "/data/state.json"
    config = parse_args(["--free-settle-seconds", "120", "--state-path", "/x.json"])
    assert config.free_settle_seconds == 120.0
    assert config.state_path == "/x.json"


def test_mode_tier_and_allowlist_from_env(base_env, monkeypatch):
    monkeypatch.setenv("CLEANER_MODE", "free_inactive")
    monkeypatch.setenv("CLEANER_TIER", "all")
    monkeypatch.setenv("USERNAME_ALLOWLIST", "a, b,,c")
    monkeypatch.setenv("REPORT_PATH", "/tmp/x.csv")
    config = parse_args([])
    assert config.mode == "free_inactive"
    assert config.tier == "all"
    assert config.username_allowlist == frozenset({"a", "b", "c"})
    assert config.report_path == "/tmp/x.csv"


@pytest.mark.parametrize(
    "argv",
    [
        ["--grace-days", "44"],
        ["--grace-days", "30"],
        ["--max-deletions", "0"],
        ["--max-deletions", "1001"],
        ["--page-size", "1001"],
        ["--page-size", "0"],
        ["--expire-tolerance-hours", "-1"],
        ["--recent-payment-days", "0"],
        ["--delete-pause-seconds", "-1"],
        ["--max-consecutive-failures", "0"],
        ["--max-candidate-ratio", "0"],
        ["--max-candidate-ratio", "1.5"],
        ["--max-free-candidate-ratio", "0"],
        ["--max-free-candidate-ratio", "1.01"],
        ["--max-orphan-ratio", "2"],
        ["--ratio-min-sample", "0"],
        ["--rwms-timeout-seconds", "0"],
        ["--free-settle-seconds", "119"],
        ["--free-settle-seconds", "0"],
        ["--free-settle-seconds", "nan"],
        ["--mode", "everything"],
        ["--tier", "paid"],
    ],
)
def test_invalid_settings_are_rejected(base_env, argv):
    with pytest.raises(SystemExit) as exc:
        parse_args(argv)
    assert exc.value.code == 2


def test_free_candidate_ratio_from_env_and_flag(base_env, monkeypatch):
    monkeypatch.setenv("MAX_FREE_CANDIDATE_RATIO", "0.95")
    assert parse_args([]).max_free_candidate_ratio == 0.95
    config = parse_args(["--max-free-candidate-ratio", "1"])
    assert config.max_free_candidate_ratio == 1.0
    # Порог режима expired не меняется.
    assert config.max_candidate_ratio == 0.6


def test_minimum_grace_is_past_winback(base_env):
    assert parse_args(["--grace-days", "45"]).grace_days == 45


def test_missing_required_settings(monkeypatch):
    for name in ("DATABASE_URL", "RWMS_HOST", "RWMS_PORT"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(SystemExit):
        parse_args([])


# --- блокировка --------------------------------------------------------------


class FakeLock:
    def __init__(self, acquired):
        self.acquired = acquired
        self.exited = False

    def __enter__(self):
        return self.acquired

    def __exit__(self, *exc):
        self.exited = True


class FakeCleaner:
    def __init__(self):
        self.ran = False

    async def run(self):
        self.ran = True
        return mock.Mock(exit_code=EXIT_OK)


def test_run_locked_exits_when_lock_is_taken():
    cleaner = FakeCleaner()
    lock = FakeLock(False)
    assert asyncio.run(run_locked(lock, cleaner)) == EXIT_LOCKED
    assert not cleaner.ran
    assert lock.exited


def test_run_locked_runs_under_the_lock():
    cleaner = FakeCleaner()
    assert asyncio.run(run_locked(FakeLock(True), cleaner)) == EXIT_OK
    assert cleaner.ran


def _pg_engine(try_lock_result):
    conn = mock.MagicMock()
    conn.execute.return_value.scalar.return_value = try_lock_result
    engine = mock.MagicMock()
    engine.dialect.name = "postgresql"
    engine.connect.return_value = conn
    return engine, conn


def _sql(call):
    return str(call.args[0].compile(compile_kwargs={"literal_binds": True}))


def test_advisory_lock_acquires_and_releases():
    engine, conn = _pg_engine(True)
    with AdvisoryLock(engine) as acquired:
        assert acquired is True
        assert "pg_try_advisory_lock" in _sql(conn.execute.call_args_list[0])
        assert str(ADVISORY_LOCK_KEY) in _sql(conn.execute.call_args_list[0])
    assert "pg_advisory_unlock" in _sql(conn.execute.call_args_list[-1])
    conn.close.assert_called_once()


def test_advisory_lock_busy_does_not_unlock():
    engine, conn = _pg_engine(False)
    with AdvisoryLock(engine) as acquired:
        assert acquired is False
    assert all("pg_advisory_unlock" not in _sql(c) for c in conn.execute.call_args_list)
    conn.close.assert_called_once()


def test_advisory_lock_requires_postgres(engine):
    with pytest.raises(RuntimeError):
        with AdvisoryLock(engine):
            pass


# --- снимок панели -----------------------------------------------------------


def test_snapshot_fields_and_never_connected():
    snap = snapshot_from_proto(panel_user("u1"))
    assert snap.status == "EXPIRED"
    assert snap.never_connected
    assert snap.restorable
    assert snap.internal_squads == ("squad-default",)
    used = snapshot_from_proto(panel_user("u2", online_at=days_ago(100)))
    assert not used.never_connected
    assert used.online_at == days_ago(100)


def test_snapshot_has_no_secret_fields():
    names = {f.name for f in PanelSnapshot.__dataclass_fields__.values()}
    for secret in (
        "short_uuid",
        "vless_uuid",
        "trojan_password",
        "ss_password",
        "subscription_url",
    ):
        assert secret not in names


def test_snapshot_without_status_or_expire():
    user = proto.UserResponse(uuid="x", username="u")
    snap = snapshot_from_proto(user)
    assert snap.status == ""
    assert snap.expire_at is None
    assert not snap.restorable


def test_snapshot_activity_fingerprint():
    # Двухфазное удаление: любое из этих полей изменилось между фазами —
    # пользователь активен.
    user = panel_user("u1", online_at=days_ago(40), lifetime_traffic=100.0)
    base = snapshot_from_proto(user)
    assert base.used_traffic_bytes == 0.0 and base.sub_last_opened_at is None
    for mutate in (
        lambda u: u.online_at.FromDatetime(days_ago(0).replace(tzinfo=None)),
        lambda u: u.first_connected.FromDatetime(days_ago(0).replace(tzinfo=None)),
        lambda u: u.sub_last_opened_at.FromDatetime(days_ago(0).replace(tzinfo=None)),
        lambda u: u.last_traffic_reset_at.FromDatetime(days_ago(0).replace(tzinfo=None)),
        lambda u: setattr(u, "used_traffic_bytes", 1.0),
        lambda u: setattr(u, "lifetime_used_traffic_bytes", 101.0),
    ):
        changed = proto.UserResponse()
        changed.CopyFrom(user)
        mutate(changed)
        assert (
            snapshot_from_proto(changed).activity_fingerprint()
            != base.activity_fingerprint()
        )
    # Статус в отпечаток не входит (DISABLED ставим мы сами).
    same = proto.UserResponse()
    same.CopyFrom(user)
    same.status = proto.UserStatus.DISABLED
    assert snapshot_from_proto(same).activity_fingerprint() == base.activity_fingerprint()
