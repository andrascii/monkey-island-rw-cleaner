"""Журнал упреждающей записи (write-ahead): запись pending ДО DeleteUser /
UpdateUser(DISABLED), переходы после подтверждённого исхода, отмена задачи
(CancelledError), KeyboardInterrupt и жёсткое падение процесса во время
запроса — следующий запуск разбирает такую запись как uncertain."""

import asyncio
import json
import logging
from datetime import timedelta

import pytest

import proto.rwmanager_pb2 as proto
from common.models.db import User
from rw_cleaner.cleanup import EXIT_OK
from rw_cleaner.cleanup import EXIT_UNCERTAIN
from rw_cleaner.cleanup import Cleaner
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


class HardKill(BaseException):
    """Процесс убит (SIGKILL/OOM): ни один обработчик не должен успеть
    ничего записать — см. hard_kill_during."""


def later(hours):
    return NOW + timedelta(hours=hours)


def new_cleaner(config, session_factory, rwms, sleep=None):
    return Cleaner(
        config,
        session_factory=session_factory,
        rwms=rwms,
        sleep=sleep or HookSleeps(),
        clock=lambda: NOW,
    )


def records(tmp_path):
    return [(r["op"], r["username"], r["state"]) for r in uncertain_entries(tmp_path)]


def restore(tmp_path, snapshot):
    state_file(tmp_path).write_text(snapshot["text"])


# --- DELETE --------------------------------------------------------------------


def test_delete_write_ahead_is_on_disk_before_the_rpc(tmp_path, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    seen = []
    rwms.on_delete = lambda name: seen.append(records(tmp_path))

    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert seen == [[("delete", "u1", "pending")]]
    (archive,) = archives(session_factory, "u1")
    # Подтверждённый DELETE — запись убрана.
    assert summary.exit_code == EXIT_OK and summary.deleted == 1
    assert uncertain_entries(tmp_path) == []
    assert archive.restored_at is None


def test_delete_confirmed_by_reread_removes_the_record(tmp_path, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    rwms.delete_results["u1"] = "lost"  # удалено, ответ потерян
    seen = []
    rwms.on_strict = lambda name: rwms.delete_calls and seen.append(records(tmp_path))

    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    # Во время перечитываний — уже uncertain, после подтверждения — пусто.
    assert seen and seen[0] == [("delete", "u1", "uncertain")]
    assert summary.exit_code == EXIT_OK and summary.deleted == 1
    assert uncertain_entries(tmp_path) == []


@pytest.mark.parametrize("landed", [False, True])
def test_cancelled_delete_is_uncertain_and_blocks_the_next_run(
    tmp_path, session_factory, landed
):
    # Отмена задачи (CancelledError — BaseException) во время DeleteUser.
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    seed(session_factory, rwms, "u2")

    def on_delete(name):
        if landed:
            rwms.users.pop(name)  # панель успела удалить
        raise asyncio.CancelledError()

    rwms.on_delete = on_delete
    cleaner = new_cleaner(make_config(tmp_path, execute=True), session_factory, rwms)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(cleaner.run())

    assert cleaner.summary.exit_code == EXIT_UNCERTAIN
    assert cleaner.summary.stop_reason == "interrupted"
    assert cleaner.summary.delete_uncertain == 1
    assert records(tmp_path) == [("delete", "u1", "uncertain")]
    (archive,) = archives(session_factory, "u1")
    assert archive.restored_at is None  # после DeleteUser архив не закрывается
    assert uncertain_entries(tmp_path)[0]["archive_id"] == archive.id
    actions = {r["username"]: r["action"] for r in read_report(tmp_path / "report.csv")}
    assert actions == {"u1": "failed:delete_interrupted", "u2": "not_run:interrupted"}

    # Следующий запуск: ничего нового, код 6, запись остаётся (карантин).
    rwms.on_delete = None
    _, second = run(
        make_config(tmp_path, execute=True), session_factory, rwms, now=later(1)
    )
    assert second.exit_code == EXIT_UNCERTAIN
    assert second.stop_reason == "uncertain_pending"
    assert len(rwms.delete_calls) == 1
    assert "u2" in rwms.users
    (record,) = uncertain_entries(tmp_path)
    assert record["last_observed"] == ("GONE" if landed else "EXPIRED")


def test_hard_crash_during_delete_leaves_pending_treated_as_uncertain(
    tmp_path, session_factory, caplog
):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    seed(session_factory, rwms, "u2")
    cleaner = new_cleaner(make_config(tmp_path, execute=True), session_factory, rwms)
    snapshot = {}

    def on_delete(name):
        snapshot["text"] = state_file(tmp_path).read_text()
        rwms.users.pop(name)  # запрос дошёл до панели
        raise HardKill()

    rwms.on_delete = on_delete
    with pytest.raises(HardKill):
        asyncio.run(cleaner.run())
    restore(tmp_path, snapshot)  # процесс убит: после запроса ничего не записано
    assert records(tmp_path) == [("delete", "u1", "pending")]

    rwms.on_delete = None
    with caplog.at_level(logging.WARNING):
        _, second = run(
            make_config(tmp_path, execute=True), session_factory, rwms, now=later(1)
        )
    assert "left PENDING by an interrupted run" in caplog.text
    assert second.exit_code == EXIT_UNCERTAIN
    assert second.stop_reason == "uncertain_pending"
    assert len(rwms.delete_calls) == 1 and "u2" in rwms.users
    assert rwms.stream_calls and len(rwms.stream_calls) == 1  # только первый запуск
    (record,) = uncertain_entries(tmp_path)
    assert record["state"] == "uncertain" and record["last_observed"] == "GONE"

    # Карантин прошёл — снята, дальше обычная обработка.
    _, third = run(
        make_config(tmp_path, execute=True), session_factory, rwms, now=later(25)
    )
    assert third.uncertain_dropped == 1
    assert uncertain_entries(tmp_path) == []
    assert "u2" not in rwms.users


def test_record_removal_failure_after_confirmed_delete_stops(
    tmp_path, session_factory, monkeypatch
):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    seed(session_factory, rwms, "u2")

    def broken_remove(self, panel_uuid):
        raise OSError("disk full")

    monkeypatch.setattr(DisabledState, "remove_uncertain", broken_remove)
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert [c["username"] for c in rwms.delete_calls] == ["u1"]
    assert summary.deleted == 1
    assert summary.exit_code == EXIT_UNCERTAIN
    assert summary.stop_reason == "state_write_failed"
    # Запись осталась pending — следующий запуск её перепроверит.
    assert records(tmp_path) == [("delete", "u1", "pending")]


# --- DISABLE -------------------------------------------------------------------


def test_disable_write_ahead_then_conversion_to_disabled_entry(
    tmp_path, session_factory
):
    rwms = FakeRwms()
    setup_idle(session_factory, rwms)
    during = []
    at_settle = []

    def on_update(name, request):
        if request.status == DISABLED:
            during.append((records(tmp_path), state_entries(tmp_path)))

    rwms.on_update = on_update
    sleeps = HookSleeps(
        on_settle=lambda: at_settle.append(
            (records(tmp_path), [e["username"] for e in state_entries(tmp_path)])
        )
    )
    _, summary = run(free_config(tmp_path), session_factory, rwms, sleeps)

    # Во время UpdateUser: только pending, записи об отключении ещё нет.
    assert during == [([("disable", "idle", "pending")], [])]
    # Подтверждён: pending → запись об отключении.
    assert at_settle == [([], ["idle"])]
    assert summary.exit_code == EXIT_OK and summary.deleted == 1
    assert state_entries(tmp_path) == [] and uncertain_entries(tmp_path) == []


def test_disable_confirmed_by_reread_converts_the_record(tmp_path, session_factory):
    rwms = FakeRwms()
    setup_idle(session_factory, rwms)
    rwms.update_results["idle"] = "lost"
    at_settle = []
    sleeps = HookSleeps(
        on_settle=lambda: at_settle.append((records(tmp_path), len(state_entries(tmp_path))))
    )
    _, summary = run(free_config(tmp_path), session_factory, rwms, sleeps)
    assert at_settle == [([], 1)]
    assert summary.exit_code == EXIT_OK and summary.deleted == 1


@pytest.mark.parametrize("error", [asyncio.CancelledError, KeyboardInterrupt])
def test_interrupted_disable_then_payment_then_late_disable_is_reenabled(
    tmp_path, session_factory, error
):
    # Сценарий ревьюера: DISABLE прерван, восстановление видит ACTIVE,
    # пользователь платит, запоздавший DISABLE доходит — следующий запуск
    # обязан включить подписку обратно.
    rwms = FakeRwms()
    ids = setup_idle(session_factory, rwms, ("idle1", "idle2"))

    def on_update(name, request):
        if name == "idle2" and request.status == DISABLED:
            raise error()

    rwms.on_update = on_update
    cleaner = new_cleaner(free_config(tmp_path), session_factory, rwms)
    with pytest.raises(error):
        asyncio.run(cleaner.run())

    # idle1 (DISABLE подтверждён) включена обратно в finally; idle2 —
    # запись uncertain, записи об отключении нет.
    assert status_of(rwms, "idle1") == "ACTIVE"
    assert status_of(rwms, "idle2") == "ACTIVE"
    assert state_entries(tmp_path) == []
    assert records(tmp_path) == [("disable", "idle2", "uncertain")]
    assert cleaner.summary.disable_uncertain == 1
    assert cleaner.summary.exit_code == EXIT_UNCERTAIN
    assert rwms.delete_calls == []

    rwms.on_update = None
    with session_factory() as s:
        s.get(User, ids["idle2"]).expire_at = naive(NOW + timedelta(days=30))
        s.commit()
    _, second = run(free_config(tmp_path), session_factory, rwms, now=later(1))
    assert second.exit_code == EXIT_UNCERTAIN
    assert [e["last_observed"] for e in uncertain_entries(tmp_path)] == ["ACTIVE"]

    rwms.users["idle2"].status = DISABLED  # запоздавший DISABLE дошёл
    _, third = run(free_config(tmp_path), session_factory, rwms, now=later(2))
    assert status_of(rwms, "idle2") == "ACTIVE"
    assert third.uncertain_reenabled == 1
    assert third.exit_code == EXIT_UNCERTAIN
    assert rwms.delete_calls == []


@pytest.mark.parametrize("landed", [False, True])
def test_hard_crash_during_disable_pending_is_rechecked(
    tmp_path, session_factory, landed
):
    rwms = FakeRwms()
    ids = setup_idle(session_factory, rwms)
    cleaner = new_cleaner(free_config(tmp_path), session_factory, rwms)
    snapshot = {}

    def on_update(name, request):
        snapshot["text"] = state_file(tmp_path).read_text()
        if landed:
            rwms.users[name].status = DISABLED
        raise HardKill()

    rwms.on_update = on_update
    with pytest.raises(HardKill):
        asyncio.run(cleaner.run())
    restore(tmp_path, snapshot)
    assert records(tmp_path) == [("disable", "idle", "pending")]
    assert state_entries(tmp_path) == []
    rwms.on_update = None

    # Пользователь оплатил.
    with session_factory() as s:
        s.get(User, ids["idle"]).expire_at = naive(NOW + timedelta(days=30))
        s.commit()
    _, second = run(free_config(tmp_path), session_factory, rwms, now=later(1))
    assert second.exit_code == EXIT_UNCERTAIN
    assert status_of(rwms, "idle") == "ACTIVE"
    (record,) = uncertain_entries(tmp_path)
    assert record["state"] == "uncertain" and record["last_observed"] == "ACTIVE"
    assert second.uncertain_reenabled == (1 if landed else 0)

    if not landed:
        # Запоздавший DISABLE дошёл позже — следующий запуск включает.
        rwms.users["idle"].status = DISABLED
        _, third = run(free_config(tmp_path), session_factory, rwms, now=later(2))
        assert third.uncertain_reenabled == 1
        assert status_of(rwms, "idle") == "ACTIVE"
    assert rwms.delete_calls == []


def test_hard_crash_after_confirmed_disable_is_recovered(tmp_path, session_factory):
    # Убит в паузе между фазами: запись pending уже превращена в запись об
    # отключении — следующий запуск включает подписку обычным
    # восстановлением, без карантина.
    rwms = FakeRwms()
    setup_idle(session_factory, rwms)

    def kill():
        raise HardKill()

    cleaner = new_cleaner(free_config(tmp_path), session_factory, rwms, HookSleeps(kill))

    async def no_finally():  # жёсткое падение: finally не выполняется
        pass

    cleaner._reenable_leftovers = no_finally
    with pytest.raises(HardKill):
        asyncio.run(cleaner.run())
    assert status_of(rwms, "idle") == "DISABLED"
    assert uncertain_entries(tmp_path) == []
    assert [e["username"] for e in state_entries(tmp_path)] == ["idle"]

    _, second = run(make_config(tmp_path, execute=True), session_factory, rwms)
    assert second.recovered == 1 and second.exit_code == EXIT_OK
    assert status_of(rwms, "idle") == "ACTIVE"
    assert state_entries(tmp_path) == []


def test_conversion_failure_reenables_and_keeps_pending(
    tmp_path, session_factory, monkeypatch
):
    rwms = FakeRwms()
    setup_idle(session_factory, rwms, ("idle1", "idle2"))

    def broken_confirm(self, entry):
        raise OSError("disk full")

    monkeypatch.setattr(DisabledState, "confirm_disable", broken_confirm)
    _, summary = run(free_config(tmp_path), session_factory, rwms, HookSleeps())

    assert status_of(rwms, "idle1") == "ACTIVE"
    assert status_of(rwms, "idle2") == "ACTIVE"
    assert [c["status"] for c in rwms.update_calls] == ["DISABLED", "ACTIVE"]
    assert rwms.delete_calls == []
    assert summary.exit_code == EXIT_UNCERTAIN
    assert summary.stop_reason == "state_write_failed"
    assert records(tmp_path) == [("disable", "idle1", "pending")]
    actions = {r["username"]: r["action"] for r in read_report(tmp_path / "report.csv")}
    assert actions == {
        "idle1": "failed:state_write_failed",
        "idle2": "not_run:state_write_failed",
    }


def test_cancel_while_confirming_disable_keeps_uncertain(tmp_path, session_factory):
    rwms = FakeRwms()
    setup_idle(session_factory, rwms)
    rwms.update_results["idle"] = None

    def on_strict(name):
        if rwms.update_calls:
            raise asyncio.CancelledError()

    rwms.on_strict = on_strict
    cleaner = new_cleaner(free_config(tmp_path), session_factory, rwms)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(cleaner.run())
    assert records(tmp_path) == [("disable", "idle", "uncertain")]
    assert state_entries(tmp_path) == []


def test_phase_two_cancelled_delete_reenables_and_keeps_record(
    tmp_path, session_factory
):
    rwms = FakeRwms()
    setup_idle(session_factory, rwms)
    rwms.on_delete = lambda name: (_ for _ in ()).throw(asyncio.CancelledError())
    cleaner = new_cleaner(free_config(tmp_path), session_factory, rwms)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(cleaner.run())

    # Подписка на месте и включена обратно в finally; DELETE ведёт запись.
    assert status_of(rwms, "idle") == "ACTIVE"
    assert state_entries(tmp_path) == []
    assert records(tmp_path) == [("delete", "idle", "uncertain")]


# --- dry-run, файл состояния --------------------------------------------------


def test_dry_run_reports_pending_record(tmp_path, session_factory, caplog):
    rwms = FakeRwms()
    uid = seed(session_factory, rwms, "u1")
    state = DisabledState(state_file(tmp_path))
    state.load()
    state.put_uncertain(
        UncertainEntry("delete", "uuid-u1", "u1", uid, STARTED, state="pending")
    )
    with caplog.at_level(logging.ERROR):
        _, summary = run(make_config(tmp_path), session_factory, rwms)
    assert "state=pending" in caplog.text
    assert summary.exit_code == EXIT_UNCERTAIN
    assert rwms.delete_calls == [] and rwms.strict_calls == []


def test_state_pending_roundtrip_and_confirm_disable(tmp_path):
    path = tmp_path / "state.json"
    state = DisabledState(path)
    state.load()
    pending = UncertainEntry("disable", "uuid-a", "a", 1, STARTED, state="pending")
    state.put_uncertain(pending)
    fresh = DisabledState(path)
    fresh.load()
    assert fresh.uncertain == [pending] and fresh.uncertain[0].pending
    raw = json.loads(path.read_text())
    assert raw["version"] == 3 and raw["uncertain"][0]["state"] == "pending"

    entry = DisabledEntry("uuid-a", "a", 1, STARTED)
    fresh.confirm_disable(entry)
    again = DisabledState(path)
    assert again.load() == [entry]
    assert again.uncertain == []


def test_confirm_disable_failure_leaves_file_and_memory_unchanged(
    tmp_path, monkeypatch
):
    path = tmp_path / "state.json"
    state = DisabledState(path)
    state.load()
    pending = UncertainEntry("disable", "uuid-a", "a", 1, STARTED, state="pending")
    state.put_uncertain(pending)
    before = path.read_text()

    def broken(*args, **kw):
        raise OSError("disk full")

    monkeypatch.setattr(state, "_write", broken)
    with pytest.raises(OSError):
        state.confirm_disable(DisabledEntry("uuid-a", "a", 1, STARTED))
    assert path.read_text() == before
    assert state.uncertain == [pending] and state.entries == []


def test_v2_record_without_state_is_uncertain(tmp_path):
    path = tmp_path / "state.json"
    path.write_text(
        json.dumps(
            {
                "version": 2,
                "entries": [],
                "uncertain": [
                    {"op": "delete", "panel_uuid": "u", "username": "n", "user_id": 1}
                ],
            }
        )
    )
    state = DisabledState(path)
    state.load()
    (record,) = state.uncertain
    assert record.state == "uncertain" and not record.pending


@pytest.mark.parametrize("value", ["done", "", 1])
def test_unknown_record_state_raises(tmp_path, value):
    path = tmp_path / "state.json"
    path.write_text(
        json.dumps(
            {
                "entries": [],
                "uncertain": [
                    {
                        "op": "delete",
                        "panel_uuid": "u",
                        "username": "n",
                        "user_id": 1,
                        "state": value,
                    }
                ],
            }
        )
    )
    with pytest.raises(StateError):
        DisabledState(path).load()
