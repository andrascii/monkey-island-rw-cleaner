"""R1: DeleteUser с неизвестным исходом (подтверждение под блокировкой,
delete_uncertain). R2: двухфазное удаление free_inactive (DISABLED → пауза →
удаление или обратное включение), файл состояния и восстановление после
упавшего запуска. R3: нижний предел free_tier_inactive_days в запуске."""

import asyncio
import json
import logging
import math
from datetime import timedelta

import pytest
from sqlalchemy import event

import proto.rwmanager_pb2 as proto
from common.models.db import User
from common.models.db import UserBlock
from rw_cleaner import cleanup
from rw_cleaner.cleanup import DELETE_CONFIRM_POLL_SECONDS
from rw_cleaner.cleanup import EXIT_AUTO_STOP
from rw_cleaner.cleanup import EXIT_LOCKED
from rw_cleaner.cleanup import EXIT_OK
from rw_cleaner.cleanup import EXIT_UNCERTAIN
from rw_cleaner.cleanup import Cleaner
from rw_cleaner.state import DisabledEntry
from rw_cleaner.state import DisabledState
from rw_cleaner.state import StateError
from tests.helpers import NOW
from tests.helpers import FakeRwms
from tests.helpers import days_ago
from tests.helpers import naive
from tests.helpers import secrets_of
from tests.test_cleaner_run import archives
from tests.test_cleaner_run import enable_free_tier
from tests.test_cleaner_run import make_config
from tests.test_cleaner_run import read_report
from tests.test_cleaner_run import run
from tests.test_cleaner_run import seed
from tests.test_cleaner_run import seed_free

SETTLE = 180.0
ACTIVE = proto.UserStatus.ACTIVE
DISABLED = proto.UserStatus.DISABLED


class HookSleeps:
    """Фейковый sleep: на паузе между фазами (SETTLE) вызывает хук —
    «что случилось за время паузы»."""

    def __init__(self, on_settle=None):
        self.calls = []
        self.on_settle = on_settle

    async def __call__(self, seconds):
        self.calls.append(seconds)
        if seconds == SETTLE and self.on_settle:
            self.on_settle()


def free_config(tmp_path, **overrides):
    values = {
        "mode": "free_inactive",
        "execute": True,
        "max_free_candidate_ratio": 1.0,
        "free_settle_seconds": SETTLE,
    }
    values.update(overrides)
    return make_config(tmp_path, **values)


def setup_idle(session_factory, rwms, names=("idle",)):
    enable_free_tier(session_factory, free_tier_inactive_days="30")
    ids = {}
    for fid, name in enumerate(names, 1):
        ids[name] = seed_free(
            session_factory,
            rwms,
            name,
            online_at=days_ago(45),
            switched_at=days_ago(50),
            fid=fid,
        )
    return ids


def state_file(tmp_path):
    return tmp_path / "state" / "disabled.json"


def state_entries(tmp_path):
    path = state_file(tmp_path)
    if not path.exists():
        return []
    return json.loads(path.read_text())["entries"]


def uncertain_entries(tmp_path):
    path = state_file(tmp_path)
    if not path.exists():
        return []
    return json.loads(path.read_text()).get("uncertain", [])


def status_of(rwms, name):
    return proto.UserStatus.Name(rwms.users[name].status)


def statuses(rwms, status):
    return [c for c in rwms.update_calls if c["status"] == status]


# --- R2: двухфазное удаление -------------------------------------------------


def test_two_phase_happy_path(tmp_path, session_factory):
    rwms = FakeRwms()
    setup_idle(session_factory, rwms, ("idle1", "idle2", "idle3"))
    order = []
    rwms.on_update = lambda name, req: order.append(("update", name))
    rwms.on_delete = lambda name: order.append(("delete", name))
    sleeps = HookSleeps(on_settle=lambda: order.append(("settle", None)))

    _, summary = run(free_config(tmp_path), session_factory, rwms, sleeps)

    assert summary.exit_code == EXIT_OK
    assert summary.disabled == 3 and summary.deleted == 3
    assert summary.reenabled == 0
    # Сначала вся партия в DISABLED, ОДНА пауза, потом удаления.
    assert order == [
        ("update", "idle1"),
        ("update", "idle2"),
        ("update", "idle3"),
        ("settle", None),
        ("delete", "idle1"),
        ("delete", "idle2"),
        ("delete", "idle3"),
    ]
    assert sleeps.calls.count(SETTLE) == 1
    # Фаза 1 меняет ТОЛЬКО статус: ни тега, ни отрядов, ни срока.
    for call in rwms.update_calls:
        assert call["fields"] == ["uuid", "status"]
        assert call["status"] == "DISABLED"
    # Каждый RPC с таймаутом.
    for calls in (rwms.update_calls, rwms.delete_calls, rwms.strict_calls):
        assert calls and all(c["timeout"] for c in calls)
    assert state_entries(tmp_path) == []
    # Архив хранит статус до нашего DISABLED.
    assert {a.panel_status for a in archives(session_factory)} == {"ACTIVE"}
    assert {a.reason for a in archives(session_factory)} == {"free_inactive"}


def test_free_dry_run_never_disables(tmp_path, engine, session_factory):
    rwms = FakeRwms()
    setup_idle(session_factory, rwms)
    _, summary = run(free_config(tmp_path, execute=False), session_factory, rwms)
    assert summary.dry_run == 1 and summary.disabled == 0
    assert rwms.update_calls == [] and rwms.delete_calls == []
    assert rwms.strict_calls == []
    assert not state_file(tmp_path).exists()
    assert status_of(rwms, "idle") == "ACTIVE"


@pytest.mark.parametrize(
    "change",
    ["online_at", "sub_last_opened_at", "used_traffic", "lifetime_traffic"],
)
def test_activity_during_settle_reenables_and_skips(tmp_path, session_factory, change):
    # sub_last_opened_at RWMS сейчас не заполняет (панель 3.x): этот случай
    # проверяет, что поле, если появится, сразу отменяет удаление.
    rwms = FakeRwms()
    setup_idle(session_factory, rwms)

    def connect():
        user = rwms.users["idle"]
        if change == "online_at":
            user.online_at.FromDatetime(naive(NOW))
        elif change == "sub_last_opened_at":
            user.sub_last_opened_at.FromDatetime(naive(NOW))
        elif change == "used_traffic":
            user.used_traffic_bytes = 12345.0
        else:
            user.lifetime_used_traffic_bytes += 1.0

    _, summary = run(
        free_config(tmp_path), session_factory, rwms, HookSleeps(on_settle=connect)
    )

    assert rwms.delete_calls == []
    assert archives(session_factory) == []
    assert status_of(rwms, "idle") == "ACTIVE"
    assert summary.reenabled == 1 and summary.deleted == 0
    assert [c["status"] for c in rwms.update_calls] == ["DISABLED", "ACTIVE"]
    assert statuses(rwms, "ACTIVE")[0]["fields"] == ["uuid", "status"]
    (row,) = read_report(tmp_path / "report.csv")
    assert row["action"] == "skipped:activity_since_disable"
    assert row["reenable"] == "reenabled"
    assert state_entries(tmp_path) == []


def test_activity_seen_only_under_the_lock_reenables(tmp_path, session_factory):
    # Фаза 2: первое перечитывание чистое, активность видна только при
    # перечитывании под блокировкой — архив этого запуска закрывается,
    # подписка включается обратно.
    rwms = FakeRwms()
    setup_idle(session_factory, rwms)
    settled = []
    reads_after = []

    def on_strict(name):
        if settled:
            reads_after.append(1)
            if len(reads_after) == 2:
                rwms.users[name].online_at.FromDatetime(naive(NOW))

    rwms.on_strict = on_strict
    _, summary = run(
        free_config(tmp_path),
        session_factory,
        rwms,
        HookSleeps(on_settle=lambda: settled.append(1)),
    )

    assert rwms.delete_calls == []
    assert status_of(rwms, "idle") == "ACTIVE"
    (row,) = read_report(tmp_path / "report.csv")
    assert row["action"] == "skipped:activity_since_disable_locked"
    (archive,) = archives(session_factory)
    assert archive.restored_by == "rw-cleaner:aborted"
    assert summary.reenabled == 1


def test_reenabled_by_payment_during_settle_is_left_alone(tmp_path, session_factory):
    # Платёж за время паузы продлил users и сам включил подписку (ACTIVE):
    # не удаляем и второй раз не трогаем.
    rwms = FakeRwms()
    ids = setup_idle(session_factory, rwms)

    def pay():
        with session_factory() as s:
            s.get(User, ids["idle"]).expire_at = naive(NOW + timedelta(days=30))
            s.commit()
        rwms.users["idle"].status = ACTIVE

    _, summary = run(
        free_config(tmp_path), session_factory, rwms, HookSleeps(on_settle=pay)
    )

    assert rwms.delete_calls == []
    assert [c["status"] for c in rwms.update_calls] == ["DISABLED"]
    (row,) = read_report(tmp_path / "report.csv")
    assert row["action"] == "skipped:status_changed_since_disable"
    assert row["reenable"] == "not_disabled"
    assert state_entries(tmp_path) == []


def test_rule_failing_after_settle_reenables(tmp_path, session_factory):
    # Продление в БД без изменения панели: правило больше не выполняется —
    # включаем обратно.
    rwms = FakeRwms()
    ids = setup_idle(session_factory, rwms)

    def extend():
        with session_factory() as s:
            s.get(User, ids["idle"]).expire_at = naive(NOW + timedelta(days=30))
            s.commit()

    _, summary = run(
        free_config(tmp_path), session_factory, rwms, HookSleeps(on_settle=extend)
    )

    assert rwms.delete_calls == []
    assert status_of(rwms, "idle") == "ACTIVE"
    (row,) = read_report(tmp_path / "report.csv")
    assert row["action"] == "skipped:no_longer_eligible:db_paid_time_active"
    assert row["reenable"] == "reenabled"


def test_admin_block_during_settle_keeps_disabled(tmp_path, session_factory):
    rwms = FakeRwms()
    ids = setup_idle(session_factory, rwms)

    def block():
        with session_factory() as s:
            s.add(UserBlock(user_id=ids["idle"], reason="abuse"))
            s.commit()

    _, summary = run(
        free_config(tmp_path), session_factory, rwms, HookSleeps(on_settle=block)
    )

    assert rwms.delete_calls == []
    assert status_of(rwms, "idle") == "DISABLED"
    assert [c["status"] for c in rwms.update_calls] == ["DISABLED"]
    assert summary.kept_disabled_blocked == 1
    (row,) = read_report(tmp_path / "report.csv")
    assert row["reenable"] == "kept_disabled_blocked"
    assert state_entries(tmp_path) == []


def test_panel_failure_in_phase_two_reenables(tmp_path, session_factory):
    rwms = FakeRwms()
    setup_idle(session_factory, rwms)
    reads = []

    def flaky(name):
        reads.append(1)
        # 1-2 — фаза 1; 3 — первое перечитывание фазы 2 падает.
        if len(reads) == 3:
            rwms.strict_errors.add(name)
        elif len(reads) == 4:
            rwms.strict_errors.discard(name)

    rwms.on_strict = flaky
    _, summary = run(free_config(tmp_path), session_factory, rwms, HookSleeps())

    assert rwms.delete_calls == []
    assert status_of(rwms, "idle") == "ACTIVE"
    assert summary.failed == 1 and summary.reenabled == 1
    assert state_entries(tmp_path) == []


def test_disable_rpc_failure_is_disable_uncertain(tmp_path, session_factory):
    # UpdateUser(DISABLED) без ответа, панель за время ожидания так и
    # показывает ACTIVE: исход неизвестен (запрос мог застрять и дойти позже)
    # — запись uncertain, остановка, код 6.
    rwms = FakeRwms()
    setup_idle(session_factory, rwms)
    rwms.update_results["idle"] = None

    _, summary = run(free_config(tmp_path), session_factory, rwms, HookSleeps())

    assert summary.exit_code == EXIT_UNCERTAIN
    assert summary.stop_reason == "disable_uncertain"
    assert summary.disable_uncertain == 1 and summary.disabled == 0
    assert rwms.delete_calls == []
    assert status_of(rwms, "idle") == "ACTIVE"
    (row,) = read_report(tmp_path / "report.csv")
    assert row["action"] == "failed:disable_uncertain"
    # Запись об отключении заменена записью uncertain.
    assert state_entries(tmp_path) == []
    (record,) = uncertain_entries(tmp_path)
    assert record["op"] == "disable" and record["username"] == "idle"
    assert record["panel_uuid"] == "uuid-idle"
    assert record["started_at"] == "2026-10-11T12:00:00Z"

    # Следующий запуск: подписка ACTIVE — это не доказывает, что старый
    # запрос не дойдёт; запись остаётся, ничего нового не делается.
    rwms.update_results.clear()
    _, second = run(free_config(tmp_path), session_factory, rwms, HookSleeps())
    assert second.exit_code == EXIT_UNCERTAIN
    assert second.stop_reason == "uncertain_pending"
    assert len(rwms.stream_calls) == 1  # только первый запуск
    assert len(rwms.update_calls) == 1 and rwms.delete_calls == []
    (record,) = uncertain_entries(tmp_path)
    assert record["last_observed"] == "ACTIVE"

    # Карантин прошёл, последнее наблюдение ACTIVE — запись снята, дальше
    # обычная обработка.
    _, third = run(
        free_config(tmp_path),
        session_factory,
        rwms,
        HookSleeps(),
        now=NOW + timedelta(hours=24),
    )
    assert third.uncertain_dropped == 1
    assert third.disabled == 1 and third.deleted == 1
    assert uncertain_entries(tmp_path) == []


def test_disable_applied_but_response_lost_is_confirmed_by_reread(
    tmp_path, session_factory
):
    # Панель применила DISABLE, ответ потерян: перечитывание под блокировкой
    # видит DISABLED — исход определён, фаза 1 продолжается как обычно.
    rwms = FakeRwms()
    setup_idle(session_factory, rwms)
    rwms.update_results["idle"] = "lost"

    _, summary = run(free_config(tmp_path), session_factory, rwms, HookSleeps())

    assert summary.exit_code == EXIT_OK
    assert summary.disabled == 1 and summary.deleted == 1
    assert summary.disable_uncertain == 0 and summary.failed == 0
    assert [c["status"] for c in rwms.update_calls] == ["DISABLED"]
    assert state_entries(tmp_path) == [] and uncertain_entries(tmp_path) == []


def test_disable_target_gone_while_confirming_is_not_uncertain(
    tmp_path, session_factory
):
    rwms = FakeRwms()
    setup_idle(session_factory, rwms)
    rwms.update_results["idle"] = None
    rwms.on_update = lambda name, req: rwms.users.pop(name)

    _, summary = run(free_config(tmp_path), session_factory, rwms, HookSleeps())

    assert summary.disable_uncertain == 0
    (row,) = read_report(tmp_path / "report.csv")
    assert row["action"] == "skipped:gone_after_disable"
    assert state_entries(tmp_path) == [] and uncertain_entries(tmp_path) == []


def test_state_write_failure_prevents_disable(tmp_path, session_factory, monkeypatch):
    # Упреждающая запись (pending) не записалась — UpdateUser не уходит,
    # запуск останавливается.
    rwms = FakeRwms()
    setup_idle(session_factory, rwms, ("idle1", "idle2"))

    def broken_put(self, entry, **kw):
        raise OSError("disk full")

    monkeypatch.setattr(DisabledState, "put_uncertain", broken_put)
    _, summary = run(free_config(tmp_path), session_factory, rwms, HookSleeps())

    assert rwms.update_calls == [] and rwms.delete_calls == []
    assert summary.stop_reason == "state_write_failed"
    assert summary.exit_code == cleanup.EXIT_FAILURES
    actions = {r["username"]: r["action"] for r in read_report(tmp_path / "report.csv")}
    assert actions == {
        "idle1": "failed:state_write_failed",
        "idle2": "not_run:state_write_failed",
    }
    assert state_entries(tmp_path) == [] and uncertain_entries(tmp_path) == []


def test_crash_between_phases_reenables_everything(tmp_path, session_factory):
    rwms = FakeRwms()
    setup_idle(session_factory, rwms, ("idle1", "idle2"))

    def crash():
        raise RuntimeError("killed during settle")

    cleaner = Cleaner(
        free_config(tmp_path),
        session_factory=session_factory,
        rwms=rwms,
        sleep=HookSleeps(on_settle=crash),
        clock=lambda: NOW,
    )
    with pytest.raises(RuntimeError):
        asyncio.run(cleaner.run())

    assert rwms.delete_calls == []
    assert status_of(rwms, "idle1") == "ACTIVE"
    assert status_of(rwms, "idle2") == "ACTIVE"
    assert cleaner.summary.reenabled == 2
    assert state_entries(tmp_path) == []
    actions = {r["username"]: r["action"] for r in read_report(tmp_path / "report.csv")}
    assert actions == {"idle1": "not_run:stopped", "idle2": "not_run:stopped"}


def test_hard_crash_is_recovered_by_the_next_run(tmp_path, session_factory, caplog):
    # Процесс убит между фазами (finally не отработал): в панели DISABLED, в
    # файле — запись. Следующий запуск (любой режим) первым делом включает
    # подписку обратно.
    rwms = FakeRwms()
    ids = setup_idle(session_factory, rwms)
    rwms.users["idle"].status = DISABLED
    state = DisabledState(state_file(tmp_path))
    state.add(DisabledEntry("uuid-idle", "idle", ids["idle"], "2026-10-11T11:00:00Z"))

    # dry-run ничего не меняет, только громко пишет в лог.
    with caplog.at_level(logging.ERROR):
        _, dry = run(make_config(tmp_path), session_factory, rwms)
    assert rwms.update_calls == []
    assert "pending re-enable" in caplog.text
    assert dry.recovered == 0

    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert summary.recovered == 1 and summary.reenabled == 1
    assert status_of(rwms, "idle") == "ACTIVE"
    assert [c["status"] for c in rwms.update_calls] == ["ACTIVE"]
    assert state_entries(tmp_path) == []
    assert summary.exit_code == EXIT_OK


def test_recovery_reenables_even_with_autopay_disallowed(tmp_path, session_factory):
    # autopay_allow=false (например, после /stopautopay) — признак мягкого
    # отключения учитывается только для запоздавшего uncertain DISABLE.
    # Подписку, которую этот инструмент сам отключил в фазе 1, включаем
    # обратно всегда (кроме user_blocks).
    rwms = FakeRwms()
    ids = setup_idle(session_factory, rwms)
    rwms.users["idle"].status = DISABLED
    with session_factory() as s:
        s.get(User, ids["idle"]).autopay_allow = False
        s.commit()
    DisabledState(state_file(tmp_path)).add(
        DisabledEntry("uuid-idle", "idle", ids["idle"], "x")
    )

    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert summary.reenabled == 1
    assert status_of(rwms, "idle") == "ACTIVE"
    assert state_entries(tmp_path) == []


def test_recovery_failure_stops_the_run(tmp_path, session_factory):
    rwms = FakeRwms()
    ids = setup_idle(session_factory, rwms)
    rwms.users["idle"].status = DISABLED
    DisabledState(state_file(tmp_path)).add(
        DisabledEntry("uuid-idle", "idle", ids["idle"], "x")
    )
    rwms.update_results["idle"] = None

    _, summary = run(free_config(tmp_path), session_factory, rwms, HookSleeps())

    assert summary.exit_code == EXIT_UNCERTAIN
    assert summary.stop_reason == "pending_reenable_failed"
    assert summary.reenable_failed == 1
    assert rwms.stream_calls == [] and rwms.delete_calls == []
    assert [e["username"] for e in state_entries(tmp_path)] == ["idle"]


def test_recovery_leaves_admin_blocked_disabled(tmp_path, session_factory):
    rwms = FakeRwms()
    ids = setup_idle(session_factory, rwms)
    rwms.users["idle"].status = DISABLED
    with session_factory() as s:
        s.add(UserBlock(user_id=ids["idle"], reason="abuse"))
        s.commit()
    DisabledState(state_file(tmp_path)).add(
        DisabledEntry("uuid-idle", "idle", ids["idle"], "x")
    )

    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert rwms.update_calls == []
    assert status_of(rwms, "idle") == "DISABLED"
    assert summary.kept_disabled_blocked == 1
    assert state_entries(tmp_path) == []


def test_unreadable_state_file_stops_execute(tmp_path, session_factory):
    rwms = FakeRwms()
    setup_idle(session_factory, rwms)
    state_file(tmp_path).parent.mkdir(parents=True)
    state_file(tmp_path).write_text("{broken")

    _, summary = run(free_config(tmp_path), session_factory, rwms, HookSleeps())

    assert summary.exit_code == EXIT_UNCERTAIN
    assert summary.stop_reason == "state_unreadable"
    assert rwms.stream_calls == [] and rwms.update_calls == []


def test_locked_state_file_stops_execute(tmp_path, session_factory):
    rwms = FakeRwms()
    setup_idle(session_factory, rwms)
    holder = DisabledState(state_file(tmp_path))
    assert holder.acquire()
    try:
        _, summary = run(free_config(tmp_path), session_factory, rwms, HookSleeps())
    finally:
        holder.release()
    assert summary.exit_code == EXIT_LOCKED
    assert rwms.stream_calls == [] and rwms.update_calls == []


def test_phase_one_auto_stop_reenables_and_skips_phase_two(tmp_path, session_factory):
    rwms = FakeRwms()
    setup_idle(session_factory, rwms, ("idle1", "idle2", "idle3"))
    rwms.strict_errors = {"idle2", "idle3"}
    sleeps = HookSleeps()

    _, summary = run(
        free_config(tmp_path, max_consecutive_failures=2), session_factory, rwms, sleeps
    )

    assert summary.exit_code == EXIT_AUTO_STOP
    assert SETTLE not in sleeps.calls
    assert rwms.delete_calls == []
    assert status_of(rwms, "idle1") == "ACTIVE"
    assert [c["status"] for c in rwms.update_calls] == ["DISABLED", "ACTIVE"]
    assert state_entries(tmp_path) == []
    actions = {r["username"]: r["action"] for r in read_report(tmp_path / "report.csv")}
    assert actions["idle1"] == "not_run:consecutive_failures"


def test_phase_two_delete_uncertain_stops_and_reenables_the_rest(
    tmp_path, session_factory
):
    rwms = FakeRwms()
    setup_idle(session_factory, rwms, ("idle1", "idle2"))
    rwms.delete_results["idle1"] = None  # DELETE без ответа, подписка на месте

    _, summary = run(free_config(tmp_path), session_factory, rwms, HookSleeps())

    assert summary.exit_code == EXIT_UNCERTAIN
    assert summary.delete_uncertain == 1
    assert [c["username"] for c in rwms.delete_calls] == ["idle1"]
    # idle2 до фазы 2 не дошла — включена обратно.
    assert status_of(rwms, "idle2") == "ACTIVE"
    # idle1: включена обратно; запоздавший DELETE ещё может дойти — дальше её
    # ведёт запись uncertain (запись об отключении убрана).
    assert status_of(rwms, "idle1") == "ACTIVE"
    assert state_entries(tmp_path) == []
    (record,) = uncertain_entries(tmp_path)
    assert record["op"] == "delete" and record["username"] == "idle1"
    actions = {r["username"]: r["action"] for r in read_report(tmp_path / "report.csv")}
    assert actions == {
        "idle1": "failed:delete_uncertain",
        "idle2": "not_run:delete_uncertain",
    }
    (archive1,) = archives(session_factory, "idle1")
    assert archive1.restored_at is None
    assert record["archive_id"] == archive1.id


def test_no_secrets_in_two_phase_logs_or_state(tmp_path, session_factory, caplog):
    rwms = FakeRwms()
    setup_idle(session_factory, rwms)
    with caplog.at_level(logging.DEBUG):
        run(
            free_config(tmp_path),
            session_factory,
            rwms,
            HookSleeps(
                on_settle=lambda: rwms.users["idle"].online_at.FromDatetime(naive(NOW))
            ),
        )
    text = caplog.text + state_file(tmp_path).read_text()
    for secret in secrets_of("idle"):
        assert secret not in text


# --- R3: нижний предел срока неактивности в запуске --------------------------


def test_inactive_days_below_floor_is_clamped(tmp_path, session_factory, caplog):
    rwms = FakeRwms()
    enable_free_tier(session_factory, free_tier_inactive_days="1")
    seed_free(
        session_factory, rwms, "recent", online_at=days_ago(2), switched_at=days_ago(50), fid=1
    )
    seed_free(
        session_factory, rwms, "old", online_at=days_ago(31), switched_at=days_ago(50), fid=2
    )
    with caplog.at_level(logging.WARNING):
        _, summary = run(free_config(tmp_path), session_factory, rwms, HookSleeps())
    assert rwms.deleted == ["old"]
    assert "clamped" in caplog.text and "effective=30" in caplog.text
    report = {r["username"]: r for r in read_report(tmp_path / "report.csv")}
    assert report["recent"]["reason"] == "free_recently_active"


# --- R1: DeleteUser с неизвестным исходом -----------------------------------


class LockRecorder:
    def __init__(self, monkeypatch, session_factory, rwms):
        self.events = []
        original = cleanup.lock_user_query

        def lock(user_id):
            self.events.append("lock")
            return original(user_id)

        monkeypatch.setattr(cleanup, "lock_user_query", lock)
        event.listen(
            session_factory, "after_commit", lambda _s: self.events.append("commit")
        )
        event.listen(
            session_factory,
            "after_rollback",
            lambda _s: self.events.append("rollback"),
        )
        rwms.on_delete = lambda _n: self.events.append("delete")
        rwms.on_strict = lambda _n: self.events.append("strict")


def test_lost_delete_response_is_confirmed_by_reread(tmp_path, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    rwms.delete_results["u1"] = "lost"  # удалено, ответ потерян

    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert summary.deleted == 1 and summary.exit_code == EXIT_OK
    assert summary.delete_uncertain == 0
    (archive,) = archives(session_factory, "u1")
    assert archive.restored_at is None


def test_late_delete_is_awaited_under_the_lock(tmp_path, session_factory, monkeypatch):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    rwms.delete_results["u1"] = ("late", 3)  # дойдёт на 3-м перечитывании
    rec = LockRecorder(monkeypatch, session_factory, rwms)
    sleeps = HookSleeps()

    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms, sleeps)

    assert summary.deleted == 1 and summary.exit_code == EXIT_OK
    # Блокировка не снимается (ни COMMIT, ни ROLLBACK), пока исход не ясен:
    # платёж не может продлить подписку, которую вот-вот удалит DELETE.
    after_lock = rec.events[rec.events.index("lock") :]
    assert after_lock[:5] == ["lock", "strict", "delete", "strict", "strict"]
    assert after_lock[5:7] == ["strict", "commit"]
    assert sleeps.calls.count(DELETE_CONFIRM_POLL_SECONDS) == 2


def test_uncertain_delete_polls_are_bounded(tmp_path, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    rwms.delete_results["u1"] = None
    sleeps = HookSleeps()
    config = make_config(
        tmp_path, execute=True, rwms_timeout_seconds=10.0, uncertain_confirm_seconds=30.0
    )

    _, summary = run(config, session_factory, rwms, sleeps)

    polls = math.ceil(30.0 / DELETE_CONFIRM_POLL_SECONDS)
    assert summary.delete_uncertain == 1
    # 2 перечитывания до DELETE + polls подтверждений, каждое с таймаутом
    # не больше RWMS_TIMEOUT_SECONDS.
    assert len(rwms.strict_calls) == 2 + polls
    assert all(0 < c["timeout"] <= 10.0 for c in rwms.strict_calls)
    assert sleeps.calls.count(DELETE_CONFIRM_POLL_SECONDS) == polls - 1


def test_uncertain_delete_respects_the_wall_clock_bound(tmp_path, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    rwms.delete_results["u1"] = None
    ticks = iter(range(0, 10_000, 25))
    cleaner = Cleaner(
        make_config(
            tmp_path,
            execute=True,
            rwms_timeout_seconds=30.0,
            uncertain_confirm_seconds=60.0,
        ),
        session_factory=session_factory,
        rwms=rwms,
        sleep=HookSleeps(),
        clock=lambda: NOW,
        monotonic=lambda: next(ticks),
    )
    summary = asyncio.run(cleaner.run())
    assert summary.delete_uncertain == 1
    # Каждый вызов monotonic «съедает» 25 с из 60: не больше 2 перечитываний.
    assert len(rwms.strict_calls) - 2 <= 2


def test_panel_unavailable_while_confirming_is_uncertain(tmp_path, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    rwms.delete_results["u1"] = None
    rwms.on_delete = lambda name: rwms.strict_errors.add(name)

    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert summary.delete_uncertain == 1
    assert summary.exit_code == EXIT_UNCERTAIN


def test_uncertain_delete_stops_the_whole_run(tmp_path, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    seed(session_factory, rwms, "u2")
    rwms.delete_results["u1"] = None

    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert summary.stop_reason == "delete_uncertain"
    assert [c["username"] for c in rwms.delete_calls] == ["u1"]
    actions = {r["username"]: r["action"] for r in read_report(tmp_path / "report.csv")}
    assert actions == {"u1": "failed:delete_uncertain", "u2": "not_run:delete_uncertain"}


# --- файл состояния ---------------------------------------------------------


def test_state_roundtrip_and_atomic_file(tmp_path):
    path = tmp_path / "s" / "state.json"
    state = DisabledState(path)
    assert state.load() == []
    a = DisabledEntry("uuid-a", "a", 1, "t1")
    b = DisabledEntry("uuid-b", "b", 2, "t2")
    state.add(a)
    state.add(b)
    state.add(a)  # повтор не дублирует
    assert DisabledState(path).load() == [a, b]
    state.remove("uuid-a")
    state.remove("uuid-missing")
    assert DisabledState(path).load() == [b]
    assert not path.with_name(path.name + ".tmp").exists()


@pytest.mark.parametrize(
    "content", ["{broken", "[]", '{"entries": [{"username": "x"}]}', '{"x": 1}']
)
def test_bad_state_file_raises(tmp_path, content):
    path = tmp_path / "state.json"
    path.write_text(content)
    with pytest.raises(StateError):
        DisabledState(path).load()


def test_state_lock_is_exclusive(tmp_path):
    path = tmp_path / "state.json"
    first, second = DisabledState(path), DisabledState(path)
    assert first.acquire()
    try:
        assert not second.acquire()
    finally:
        first.release()
    assert second.acquire()
    second.release()
