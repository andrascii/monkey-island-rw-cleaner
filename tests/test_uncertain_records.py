"""Записи uncertain (DELETE/DISABLE с неизвестным исходом): запись сразу,
остановка запуска, перепроверка следующими запусками (запоздавший DISABLE —
обратное включение), карантин, запрет новых изменений панели, пока запись
есть, и ручное снятие --clear-uncertain."""

import asyncio
import json
import logging
from datetime import timedelta

import pytest

import proto.rwmanager_pb2 as proto
from common.models.db import User
from common.models.db import UserBlock
from common.subscription_archive import build_archive
from rw_cleaner.cleanup import EXIT_CONFIG
from rw_cleaner.cleanup import EXIT_FAILURES
from rw_cleaner.cleanup import EXIT_OK
from rw_cleaner.cleanup import EXIT_UNCERTAIN
from rw_cleaner.cleanup import Cleaner
from rw_cleaner.cleanup import parse_args
from rw_cleaner.state import DisabledEntry
from rw_cleaner.state import DisabledState
from rw_cleaner.state import StateError
from rw_cleaner.state import UncertainEntry
from tests.helpers import NOW
from tests.helpers import FakeRwms
from tests.helpers import naive
from tests.test_cleaner_run import archives
from tests.test_cleaner_run import make_config
from tests.test_cleaner_run import read_report
from tests.test_cleaner_run import run
from tests.test_cleaner_run import seed
from tests.test_two_phase_and_uncertain import HookSleeps
from tests.test_two_phase_and_uncertain import free_config
from tests.test_two_phase_and_uncertain import setup_idle
from tests.test_two_phase_and_uncertain import state_entries
from tests.test_two_phase_and_uncertain import state_file
from tests.test_two_phase_and_uncertain import status_of
from tests.test_two_phase_and_uncertain import uncertain_entries

DISABLED = proto.UserStatus.DISABLED
STARTED = "2026-10-11T12:00:00Z"  # = NOW


def put_record(tmp_path, op, username, user_id, *, archive_id=None, started=STARTED):
    state = DisabledState(state_file(tmp_path))
    state.load()
    state.put_uncertain(
        UncertainEntry(
            op=op,
            panel_uuid=f"uuid-{username}",
            username=username,
            user_id=user_id,
            started_at=started,
            archive_id=archive_id,
        )
    )


def later(hours):
    return NOW + timedelta(hours=hours)


# --- запись сразу и остановка -----------------------------------------------


class Killed(BaseException):
    """Процесс убит во время ожидания подтверждения (не Exception — его не
    перехватывает ни один обработчик очистителя)."""


def test_uncertain_delete_is_persisted_before_confirmation_polls(
    tmp_path, session_factory
):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    rwms.delete_results["u1"] = None
    seen = []

    def on_strict(name):
        if rwms.delete_calls:
            # Первое перечитывание после DELETE: запись уже в файле.
            seen.append([e["op"] for e in uncertain_entries(tmp_path)])
            raise Killed()

    rwms.on_strict = on_strict
    cleaner = Cleaner(
        make_config(tmp_path, execute=True),
        session_factory=session_factory,
        rwms=rwms,
        sleep=HookSleeps(),
        clock=lambda: NOW,
    )
    with pytest.raises(Killed):
        asyncio.run(cleaner.run())

    assert seen == [["delete"]]
    (record,) = uncertain_entries(tmp_path)
    assert record["username"] == "u1" and record["started_at"] == STARTED
    (archive,) = archives(session_factory, "u1")
    assert record["archive_id"] == archive.id

    # Следующий запуск (процесс перезапущен): подписка на месте —
    # перепроверка, запись остаётся, ничего не удаляется.
    rwms.on_strict = None
    rwms.delete_results.clear()
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)
    assert summary.exit_code == EXIT_UNCERTAIN
    assert len(rwms.delete_calls) == 1
    assert uncertain_entries(tmp_path)[0]["last_observed"] == "EXPIRED"


def test_uncertain_disable_is_persisted_before_confirmation_polls(
    tmp_path, session_factory
):
    rwms = FakeRwms()
    setup_idle(session_factory, rwms)
    rwms.update_results["idle"] = None
    seen = []

    def on_strict(name):
        if rwms.update_calls:
            seen.append(
                ([e["op"] for e in uncertain_entries(tmp_path)], len(state_entries(tmp_path)))
            )
            raise Killed()

    rwms.on_strict = on_strict
    cleaner = Cleaner(
        free_config(tmp_path),
        session_factory=session_factory,
        rwms=rwms,
        sleep=HookSleeps(),
        clock=lambda: NOW,
    )
    with pytest.raises(Killed):
        asyncio.run(cleaner.run())
    # Только запись uncertain: записи об отключении до подтверждения DISABLE
    # нет (её не снимет обычное восстановление при виде ACTIVE).
    assert seen[0] == (["disable"], 0)
    (record,) = uncertain_entries(tmp_path)
    assert record["state"] == "uncertain"
    assert cleaner.summary.stop_reason == "interrupted"
    assert cleaner.summary.disable_uncertain == 1

    rwms.on_strict = None
    rwms.update_results.clear()
    _, summary = run(free_config(tmp_path), session_factory, rwms, HookSleeps())
    assert summary.exit_code == EXIT_UNCERTAIN
    assert state_entries(tmp_path) == []
    assert [e["op"] for e in uncertain_entries(tmp_path)] == ["disable"]
    assert len(rwms.update_calls) == 1  # ничего нового


def test_write_ahead_failure_sends_no_delete(tmp_path, session_factory, monkeypatch):
    # Упреждающая запись не записалась — DeleteUser не вызывается, архив
    # этого запуска закрывается, запуск останавливается (дальше файл
    # состояния тоже не пишется).
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    seed(session_factory, rwms, "u2")

    def broken(self, entry, **kw):
        raise OSError("disk full")

    monkeypatch.setattr(DisabledState, "put_uncertain", broken)
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert rwms.delete_calls == []
    assert summary.exit_code == EXIT_FAILURES
    assert summary.stop_reason == "state_write_failed"
    assert summary.delete_uncertain == 0 and summary.failed == 1
    (archive,) = archives(session_factory)
    assert archive.restored_by == "rw-cleaner:aborted"
    actions = {r["username"]: r["action"] for r in read_report(tmp_path / "report.csv")}
    assert actions == {
        "u1": "failed:state_write_failed",
        "u2": "not_run:state_write_failed",
    }


# --- R2: запоздавший DISABLE у оплатившего пользователя ---------------------


def test_late_disable_after_payment_is_reenabled_by_next_run(
    tmp_path, session_factory, caplog
):
    rwms = FakeRwms()
    ids = setup_idle(session_factory, rwms)
    rwms.update_results["idle"] = None  # DISABLE застрял

    _, first = run(free_config(tmp_path), session_factory, rwms, HookSleeps())
    assert first.disable_uncertain == 1
    assert status_of(rwms, "idle") == "ACTIVE"

    # Пользователь оплатил, подписка всё ещё ACTIVE.
    with session_factory() as s:
        s.get(User, ids["idle"]).expire_at = naive(NOW + timedelta(days=30))
        s.commit()
    rwms.update_results.clear()
    _, second = run(free_config(tmp_path), session_factory, rwms, HookSleeps(), now=later(1))
    # ACTIVE не снимает запись (раньше recovery её убирала — R2).
    assert second.exit_code == EXIT_UNCERTAIN
    assert [e["last_observed"] for e in uncertain_entries(tmp_path)] == ["ACTIVE"]

    # Запоздавший DISABLE дошёл до панели.
    rwms.users["idle"].status = DISABLED
    with caplog.at_level(logging.WARNING):
        _, third = run(
            free_config(tmp_path), session_factory, rwms, HookSleeps(), now=later(2)
        )

    assert "uncertain DISABLE landed late" in caplog.text
    assert status_of(rwms, "idle") == "ACTIVE"
    assert third.uncertain_reenabled == 1
    last = rwms.update_calls[-1]
    assert last["status"] == "ACTIVE" and last["fields"] == ["uuid", "status"]
    # Карантин не прошёл — запись остаётся, нового ничего.
    assert third.exit_code == EXIT_UNCERTAIN
    (record,) = uncertain_entries(tmp_path)
    assert record["last_observed"] == "ACTIVE"
    assert len(rwms.stream_calls) == 1 and rwms.delete_calls == []

    # После карантина — снята.
    _, fourth = run(free_config(tmp_path), session_factory, rwms, HookSleeps(), now=later(25))
    assert fourth.uncertain_dropped == 1
    assert uncertain_entries(tmp_path) == []
    assert status_of(rwms, "idle") == "ACTIVE"


def test_late_disable_with_admin_block_stays_disabled(tmp_path, session_factory):
    rwms = FakeRwms()
    ids = setup_idle(session_factory, rwms)
    rwms.users["idle"].status = DISABLED
    with session_factory() as s:
        s.add(UserBlock(user_id=ids["idle"], reason="abuse"))
        s.commit()
    put_record(tmp_path, "disable", "idle", ids["idle"])

    _, summary = run(free_config(tmp_path), session_factory, rwms, HookSleeps(), now=later(1))
    assert rwms.update_calls == []
    assert status_of(rwms, "idle") == "DISABLED"
    assert summary.exit_code == EXIT_UNCERTAIN
    assert uncertain_entries(tmp_path)[0]["last_observed"] == "DISABLED_BLOCKED"

    _, after = run(free_config(tmp_path), session_factory, rwms, HookSleeps(), now=later(24))
    assert after.uncertain_dropped == 1
    assert status_of(rwms, "idle") == "DISABLED"


def test_late_disable_reenable_failure_keeps_record_past_quarantine(
    tmp_path, session_factory
):
    rwms = FakeRwms()
    ids = setup_idle(session_factory, rwms)
    rwms.users["idle"].status = DISABLED
    rwms.update_results["idle"] = None  # включить не удаётся
    put_record(tmp_path, "disable", "idle", ids["idle"])

    _, summary = run(free_config(tmp_path), session_factory, rwms, HookSleeps(), now=later(48))

    assert summary.exit_code == EXIT_UNCERTAIN
    assert summary.uncertain_dropped == 0 and summary.reenable_failed == 1
    assert [e["op"] for e in uncertain_entries(tmp_path)] == ["disable"]


@pytest.mark.parametrize("op", ["disable", "delete"])
def test_unreadable_panel_keeps_record_past_quarantine(tmp_path, session_factory, op):
    rwms = FakeRwms()
    ids = setup_idle(session_factory, rwms)
    rwms.strict_errors.add("idle")
    put_record(tmp_path, op, "idle", ids["idle"])

    _, summary = run(free_config(tmp_path), session_factory, rwms, HookSleeps(), now=later(48))

    assert summary.exit_code == EXIT_UNCERTAIN
    assert summary.uncertain_dropped == 0
    (record,) = uncertain_entries(tmp_path)
    assert record["last_observed"] == "UNREADABLE"
    assert rwms.update_calls == [] and rwms.delete_calls == []


def test_record_without_valid_started_at_is_never_dropped(tmp_path, session_factory):
    rwms = FakeRwms()
    ids = setup_idle(session_factory, rwms)
    put_record(tmp_path, "disable", "idle", ids["idle"], started="garbage")
    _, summary = run(free_config(tmp_path), session_factory, rwms, HookSleeps(), now=later(1000))
    assert summary.exit_code == EXIT_UNCERTAIN
    assert len(uncertain_entries(tmp_path)) == 1


# --- неопределённый DELETE: перепроверка -------------------------------------


def test_uncertain_delete_landed_for_inactive_user_is_dropped_after_quarantine(
    tmp_path, session_factory, caplog
):
    rwms = FakeRwms()
    uid = seed(session_factory, rwms, "u1")
    rwms.users.pop("u1")  # DELETE дошёл
    put_record(tmp_path, "delete", "u1", uid)

    with caplog.at_level(logging.WARNING):
        _, first = run(make_config(tmp_path, execute=True), session_factory, rwms, now=later(1))
    assert "user is not active in the DB" in caplog.text
    assert first.exit_code == EXIT_UNCERTAIN
    assert uncertain_entries(tmp_path)[0]["last_observed"] == "GONE"

    _, second = run(make_config(tmp_path, execute=True), session_factory, rwms, now=later(24))
    assert second.uncertain_dropped == 1 and second.exit_code == EXIT_OK
    assert uncertain_entries(tmp_path) == []


def test_uncertain_delete_landed_for_paid_user_with_archive_points_to_heal(
    tmp_path, session_factory, caplog
):
    rwms = FakeRwms()
    uid = seed(session_factory, rwms, "u1")
    with session_factory() as s:
        archive = build_archive(rwms.users["u1"], user_id=uid, reason="expired_retention")
        s.add(archive)
        s.get(User, uid).expire_at = naive(NOW + timedelta(days=30))  # оплатил
        s.commit()
    rwms.users.pop("u1")
    put_record(tmp_path, "delete", "u1", uid)

    with caplog.at_level(logging.ERROR):
        _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms, now=later(1))

    assert "user-notify heal restores it from the open archive" in caplog.text
    assert summary.exit_code == EXIT_UNCERTAIN


def test_uncertain_delete_landed_for_paid_user_without_archive_is_critical(
    tmp_path, session_factory, caplog
):
    rwms = FakeRwms()
    uid = seed(session_factory, rwms, "u1", expire_at=NOW + timedelta(days=30))
    rwms.users.pop("u1")
    put_record(tmp_path, "delete", "u1", uid)

    with caplog.at_level(logging.CRITICAL):
        run(make_config(tmp_path, execute=True), session_factory, rwms, now=later(1))

    assert "WITHOUT an open archive" in caplog.text


# --- запрет новых изменений, пока есть запись --------------------------------


@pytest.mark.parametrize("mode", ["expired", "free_inactive"])
def test_pending_record_refuses_new_mutations(tmp_path, session_factory, mode):
    rwms = FakeRwms()
    ids = setup_idle(session_factory, rwms)
    seed(session_factory, rwms, "old1")
    other = seed(session_factory, rwms, "other", expire_at=NOW + timedelta(days=30))
    put_record(tmp_path, "delete", "other", other)

    config = free_config(tmp_path) if mode == "free_inactive" else make_config(
        tmp_path, execute=True
    )
    _, summary = run(config, session_factory, rwms, HookSleeps())

    assert summary.exit_code == EXIT_UNCERTAIN
    assert summary.stop_reason == "uncertain_pending"
    assert summary.uncertain_pending == 1
    assert rwms.stream_calls == []
    assert rwms.update_calls == [] and rwms.delete_calls == []
    assert archives(session_factory) == []
    assert status_of(rwms, "idle") == "ACTIVE" and ids


def test_guard_refuses_mutations_even_if_a_record_appears_mid_run(
    tmp_path, session_factory
):
    # Страховка внутри партии: запись появилась после старта (не через
    # перепроверку) — следующий кандидат уже не трогается.
    rwms = FakeRwms()
    uid1 = seed(session_factory, rwms, "u1")
    seed(session_factory, rwms, "u2")
    cleaner = Cleaner(
        make_config(tmp_path, execute=True),
        session_factory=session_factory,
        rwms=rwms,
        sleep=HookSleeps(),
        clock=lambda: NOW,
    )
    # Файл перечитывается только на старте — подсовываем запись в память.
    original = cleaner._state.put_uncertain

    def on_delete_mem(name):
        if name == "u1":
            original(UncertainEntry("delete", "uuid-x", "x", uid1, STARTED))

    rwms.on_delete = on_delete_mem
    summary = asyncio.run(cleaner.run())

    assert [c["username"] for c in rwms.delete_calls] == ["u1"]
    assert summary.exit_code == EXIT_UNCERTAIN
    assert summary.stop_reason == "uncertain_pending"
    actions = {r["username"]: r["action"] for r in read_report(tmp_path / "report.csv")}
    assert actions["u2"] == "not_run:uncertain_pending"


def test_dry_run_reports_pending_records_without_touching(
    tmp_path, session_factory, caplog
):
    rwms = FakeRwms()
    uid = seed(session_factory, rwms, "u1")
    rwms.users["u1"].status = DISABLED
    put_record(tmp_path, "disable", "u1", uid)

    with caplog.at_level(logging.ERROR):
        _, summary = run(make_config(tmp_path), session_factory, rwms)

    assert "uncertain panel mutation pending (dry-run" in caplog.text
    assert summary.exit_code == EXIT_UNCERTAIN
    assert summary.uncertain_pending == 1
    assert rwms.update_calls == [] and rwms.delete_calls == []
    assert rwms.strict_calls == []
    # Запись не тронута (last_checked_at пуст).
    assert uncertain_entries(tmp_path)[0]["last_checked_at"] == ""


# --- --clear-uncertain ----------------------------------------------------------


def test_clear_uncertain_drops_record_and_does_nothing_else(
    tmp_path, session_factory, caplog, monkeypatch
):
    rwms = FakeRwms()
    uid = seed(session_factory, rwms, "u1")
    put_record(tmp_path, "delete", "u1", uid)
    put_record(tmp_path, "disable", "u2", 77)
    monkeypatch.setenv("SUDO_USER", "alice")

    with caplog.at_level(logging.WARNING):
        _, summary = run(
            make_config(tmp_path, execute=True, clear_uncertain=("uuid-u1",)),
            session_factory,
            rwms,
        )

    assert summary.exit_code == EXIT_OK
    assert summary.uncertain_cleared == 1 and summary.uncertain_pending == 1
    assert "uncertain record cleared by operator" in caplog.text
    assert "sudo:alice" in caplog.text and "cleared_at=" in caplog.text
    assert [e["username"] for e in uncertain_entries(tmp_path)] == ["u2"]
    assert rwms.stream_calls == [] and rwms.strict_calls == []
    assert rwms.delete_calls == [] and rwms.update_calls == []


def test_clear_uncertain_unknown_uuid_is_an_error(tmp_path, session_factory):
    rwms = FakeRwms()
    put_record(tmp_path, "delete", "u1", 1)
    _, summary = run(
        make_config(tmp_path, execute=True, clear_uncertain=("uuid-nope",)),
        session_factory,
        rwms,
    )
    assert summary.exit_code == EXIT_CONFIG
    assert len(uncertain_entries(tmp_path)) == 1


def test_clear_uncertain_then_normal_run_proceeds(tmp_path, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    rwms.delete_results["u1"] = None
    run(make_config(tmp_path, execute=True), session_factory, rwms)
    assert len(uncertain_entries(tmp_path)) == 1

    run(
        make_config(tmp_path, execute=True, clear_uncertain=("uuid-u1",)),
        session_factory,
        rwms,
    )
    rwms.delete_results.clear()
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)
    assert summary.exit_code == EXIT_OK and summary.deleted == 1


# --- настройки ------------------------------------------------------------------


@pytest.fixture
def cli_env(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg2://example/db")
    monkeypatch.setenv("RWMS_HOST", "127.0.0.1")
    monkeypatch.setenv("RWMS_PORT", "50052")
    for name in ("UNCERTAIN_CONFIRM_SECONDS", "UNCERTAIN_QUARANTINE_HOURS"):
        monkeypatch.delenv(name, raising=False)


def test_uncertain_settings_defaults_and_bounds(cli_env, monkeypatch):
    config = parse_args([])
    assert config.uncertain_confirm_seconds == 180.0
    assert config.uncertain_quarantine_hours == 24.0
    assert config.clear_uncertain == ()
    assert config.confirm_bound == 180.0
    # Никогда не меньше 2 × RWMS_TIMEOUT_SECONDS.
    config = parse_args(["--uncertain-confirm-seconds", "10", "--rwms-timeout-seconds", "30"])
    assert config.confirm_bound == 60.0
    monkeypatch.setenv("UNCERTAIN_QUARANTINE_HOURS", "48")
    assert parse_args([]).uncertain_quarantine_hours == 48.0


@pytest.mark.parametrize(
    "argv",
    [
        ["--uncertain-confirm-seconds", "0"],
        ["--uncertain-confirm-seconds", "nan"],
        ["--uncertain-quarantine-hours", "0.5"],
        ["--uncertain-quarantine-hours", "inf"],
        ["--clear-uncertain", "uuid-1"],  # без --execute
        ["--clear-uncertain", " ", "--execute"],
    ],
)
def test_uncertain_settings_rejected(cli_env, argv):
    with pytest.raises(SystemExit) as exc:
        parse_args(argv)
    assert exc.value.code == 2


def test_clear_uncertain_flag(cli_env):
    config = parse_args(
        ["--execute", "--clear-uncertain", "a", "--clear-uncertain", "b", "--clear-uncertain", "a"]
    )
    assert config.clear_uncertain == ("a", "b")


# --- файл состояния ---------------------------------------------------------------


def test_state_v1_file_without_uncertain_still_loads(tmp_path):
    path = tmp_path / "state.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "entries": [
                    {"panel_uuid": "u", "username": "n", "user_id": 1, "disabled_at": "t"}
                ],
            }
        )
    )
    state = DisabledState(path)
    assert state.load() == [DisabledEntry("u", "n", 1, "t")]
    assert state.uncertain == []


def test_state_uncertain_roundtrip(tmp_path):
    path = tmp_path / "state.json"
    state = DisabledState(path)
    state.load()
    state.add(DisabledEntry("uuid-a", "a", 1, "t"))
    record = UncertainEntry("disable", "uuid-a", "a", 1, STARTED)
    state.put_uncertain(record, drop_disabled=True)
    fresh = DisabledState(path)
    assert fresh.load() == []
    assert fresh.uncertain == [record]
    assert json.loads(path.read_text())["version"] == 3
    assert fresh.remove_uncertain("uuid-a") == record
    assert fresh.remove_uncertain("uuid-a") is None
    reloaded = DisabledState(path)
    reloaded.load()
    assert reloaded.uncertain == []


@pytest.mark.parametrize(
    "uncertain",
    [
        "x",
        [{"op": "drop", "panel_uuid": "u", "username": "n", "user_id": 1}],
        [{"op": "delete", "panel_uuid": "", "username": "n", "user_id": 1}],
        [{"op": "delete", "panel_uuid": "u", "username": "n", "user_id": "x"}],
        [{"op": "delete", "username": "n", "user_id": 1}],
    ],
)
def test_bad_uncertain_records_raise(tmp_path, uncertain):
    path = tmp_path / "state.json"
    path.write_text(json.dumps({"entries": [], "uncertain": uncertain}))
    with pytest.raises(StateError):
        DisabledState(path).load()


def test_unreadable_state_with_uncertain_stops_execute(tmp_path, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    state_file(tmp_path).parent.mkdir(parents=True)
    state_file(tmp_path).write_text('{"entries": [], "uncertain": [{"op": "?"}]}')
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)
    assert summary.exit_code == EXIT_UNCERTAIN
    assert summary.stop_reason == "state_unreadable"
    assert rwms.stream_calls == [] and rwms.delete_calls == []
