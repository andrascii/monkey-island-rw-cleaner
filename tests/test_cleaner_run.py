"""Запуск rw-cleaner целиком: SQLite в памяти + поддельный RWMS.

Порядок «архив закоммичен → DeleteUser», идемпотентность после сбоев,
dry-run без записей, ограничители, автоостановка, отчёт и отсутствие
секретов в логах."""

import asyncio
import csv
import dataclasses
import logging
from datetime import timedelta

import pytest
from sqlalchemy import event
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import OperationalError

import proto.rwmanager_pb2 as proto
from common.models.db import Base
from common.models.db import FreeTierAccess
from common.models.db import GraceAccessGrant
from common.models.db import SubscriptionArchive
from common.models.db import SystemSetting
from common.models.db import TemporarySquadBan
from common.models.db import User
from common.models.db import UserBlock
from common.subscription_archive import build_archive
from rw_cleaner import cleanup
from rw_cleaner.cleanup import EXIT_AUTO_STOP
from rw_cleaner.cleanup import EXIT_CONFIG
from rw_cleaner.cleanup import EXIT_FAILURES
from rw_cleaner.cleanup import EXIT_OK
from rw_cleaner.cleanup import EXIT_PANEL_SCAN_FAILED
from rw_cleaner.cleanup import EXIT_UNCERTAIN
from rw_cleaner.cleanup import Cleaner
from rw_cleaner.cleanup import CleanerConfig
from rw_cleaner.report import REPORT_COLUMNS
from tests.helpers import NOW
from tests.helpers import FakeRwms
from tests.helpers import add_user
from tests.helpers import days_ago
from tests.helpers import naive
from tests.helpers import panel_user
from tests.helpers import secrets_of


def make_config(tmp_path, **overrides):
    values = {
        "database_url": "sqlite://",
        "rwms_host": "rwms",
        "rwms_port": 50052,
        "ratio_min_sample": 1,
        "max_candidate_ratio": 1.0,
        "max_orphan_ratio": 1.0,
        "delete_pause_seconds": 0,
        "report_path": str(tmp_path / "report.csv"),
        "state_path": str(tmp_path / "state" / "disabled.json"),
    }
    values.update(overrides)
    return CleanerConfig(**values)


class Sleeps:
    def __init__(self):
        self.calls = []

    async def __call__(self, seconds):
        self.calls.append(seconds)


def run(config, session_factory, rwms, sleep=None, now=NOW):
    cleaner = Cleaner(
        config,
        session_factory=session_factory,
        rwms=rwms,
        sleep=sleep or Sleeps(),
        clock=lambda: now,
    )
    summary = asyncio.run(cleaner.run())
    return cleaner, summary


def seed(session_factory, rwms, username, *, expire_at=None, user_id=None, **panel_kw):
    expire_at = expire_at or days_ago(120)
    with session_factory() as s:
        uid = add_user(s, username, expire_at=expire_at, user_id=user_id).id
        s.commit()
    rwms.users[username] = panel_user(username, expire_at=expire_at, **panel_kw)
    return uid


def archives(session_factory, username=None):
    with session_factory() as s:
        q = select(SubscriptionArchive)
        if username:
            q = q.where(SubscriptionArchive.username == username)
        rows = s.execute(q).scalars().all()
        s.expunge_all()
        return rows


def read_report(path):
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def table_counts(engine):
    counts = {}
    with engine.connect() as conn:
        for table in Base.metadata.sorted_tables:
            counts[table.name] = conn.execute(
                select(func.count()).select_from(table)
            ).scalar()
    return counts


# --- dry-run -----------------------------------------------------------------


def test_dry_run_never_writes_or_deletes(tmp_path, engine, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    seed(session_factory, rwms, "u2")
    before = table_counts(engine)

    _, summary = run(make_config(tmp_path), session_factory, rwms)

    assert summary.exit_code == EXIT_OK
    assert summary.candidates == 2
    assert summary.dry_run == 2
    assert summary.deleted == 0
    assert rwms.delete_calls == []
    assert rwms.strict_calls == []
    assert table_counts(engine) == before
    actions = {
        row["username"]: row["action"] for row in read_report(tmp_path / "report.csv")
    }
    assert actions == {"u1": "dry_run", "u2": "dry_run"}


# --- архив перед удалением ---------------------------------------------------


def test_execute_archives_before_delete(tmp_path, session_factory):
    rwms = FakeRwms()
    uid = seed(
        session_factory,
        rwms,
        "u1",
        traffic_limit_bytes=1024,
        squads=("squad-a", "squad-b"),
    )
    seen_at_delete = []

    def on_delete(username):
        # Архив уже закоммичен: виден из отдельной сессии в момент DeleteUser.
        seen_at_delete.append(len(archives(session_factory, username)))

    rwms.on_delete = on_delete
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert summary.exit_code == EXIT_OK
    assert summary.deleted == 1 and summary.archived == 1
    assert seen_at_delete == [1]
    assert rwms.deleted == ["u1"]
    (archive,) = archives(session_factory, "u1")
    original = panel_user("u1")
    assert archive.user_id == uid
    assert archive.short_uuid == original.short_uuid
    assert archive.vless_uuid == original.vless_uuid
    assert archive.trojan_password == original.trojan_password
    assert archive.ss_password == original.ss_password
    assert archive.reason == "expired_retention"
    assert archive.panel_status == "EXPIRED"
    assert archive.internal_squads == ["squad-a", "squad-b"]
    assert archive.traffic_limit_bytes == 1024
    assert archive.restored_at is None
    assert archive.archived_at == naive(NOW)
    assert rwms.strict_calls[0]["timeout"] == 30.0
    assert rwms.delete_calls[0]["timeout"] == 30.0


def test_execute_changes_only_the_archive_table(tmp_path, engine, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    before = table_counts(engine)
    with session_factory() as s:
        users_before = [
            (u.id, u.username, u.expire_at) for u in s.scalars(select(User))
        ]

    run(make_config(tmp_path, execute=True), session_factory, rwms)

    after = table_counts(engine)
    assert after.pop("subscription_archives") == before.pop("subscription_archives") + 1
    assert after == before
    with session_factory() as s:
        assert [
            (u.id, u.username, u.expire_at) for u in s.scalars(select(User))
        ] == users_before


def test_delete_failure_leaves_archive_open_and_is_retried(tmp_path, session_factory):
    # R1: DeleteUser без ответа, подписка так и осталась в панели — исход
    # неизвестен: delete_uncertain, запуск остановлен, архив открыт.
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    rwms.delete_results["u1"] = None

    _, first = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert first.exit_code == EXIT_UNCERTAIN
    assert first.delete_uncertain == 1 and first.deleted == 0
    assert first.stop_reason == "delete_uncertain"
    assert "u1" in rwms.users
    (archive,) = archives(session_factory, "u1")
    assert archive.restored_at is None
    assert (
        read_report(tmp_path / "report.csv")[0]["action"] == "failed:delete_uncertain"
    )

    rwms.delete_results.clear()
    # Запись uncertain: следующий запуск только перепроверяет её (подписка на
    # месте — ждём конца карантина) и ничего не удаляет.
    _, second = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert second.exit_code == EXIT_UNCERTAIN
    assert second.stop_reason == "uncertain_pending"
    assert second.deleted == 0 and len(rwms.delete_calls) == 1
    assert rwms.stream_calls and len(rwms.stream_calls) == 1  # только 1-й запуск

    # Карантин прошёл, подписка всё ещё в панели — запись снята, обычная
    # обработка: архив переиспользуется.
    _, third = run(
        make_config(tmp_path, execute=True),
        session_factory,
        rwms,
        now=NOW + timedelta(hours=25),
    )

    assert third.exit_code == EXIT_OK
    assert third.uncertain_dropped == 1
    assert third.deleted == 1
    assert third.archive_reused == 1 and third.archived == 0
    assert [a.id for a in archives(session_factory, "u1")] == [archive.id]
    assert rwms.deleted == ["u1"]


def test_delete_returning_false_is_a_failure(tmp_path, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    rwms.delete_results["u1"] = False
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)
    # is_deleted=False — тоже неизвестный исход: подтверждение перечитыванием,
    # подписка на месте — delete_uncertain.
    assert summary.delete_uncertain == 1 and summary.exit_code == EXIT_UNCERTAIN
    # DeleteUser вызывался — архив не закрываем.
    (archive,) = archives(session_factory, "u1")
    assert archive.restored_at is None
    assert summary.archive_closed_aborted == 0


def test_crash_between_archive_commit_and_delete_is_resumed(tmp_path, session_factory):
    # Прошлый запуск закоммитил архив и упал до DeleteUser.
    rwms = FakeRwms()
    uid = seed(session_factory, rwms, "u1")
    with session_factory() as s:
        s.add(build_archive(rwms.users["u1"], user_id=uid, reason="expired_retention"))
        s.commit()

    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert summary.deleted == 1 and summary.archive_reused == 1
    assert len(archives(session_factory, "u1")) == 1
    assert rwms.deleted == ["u1"]


def test_open_archive_without_panel_user_means_nothing_to_do(tmp_path, session_factory):
    rwms = FakeRwms()
    uid = seed(session_factory, rwms, "u1")
    with session_factory() as s:
        s.add(build_archive(rwms.users["u1"], user_id=uid, reason="expired_retention"))
        s.commit()
    del rwms.users["u1"]  # удалён прошлым запуском после архива

    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert summary.exit_code == EXIT_OK
    assert summary.scanned == 0
    assert rwms.delete_calls == []
    assert len(archives(session_factory, "u1")) == 1


def add_stale_archive(session_factory, uid, username):
    stale = panel_user(username, short_uuid="OtherLink123")
    with session_factory() as s:
        s.add(build_archive(stale, user_id=uid, reason="expired_retention"))
        s.commit()


def test_open_archive_mismatch_blocks_deletion(tmp_path, session_factory, caplog):
    rwms = FakeRwms()
    uid = seed(session_factory, rwms, "u1")
    add_stale_archive(session_factory, uid, "u1")

    with caplog.at_level(logging.INFO):
        _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    # Пропуск с CRITICAL (нужен ручной разбор), а не ошибка запуска.
    assert summary.failed == 0 and summary.deleted == 0
    assert summary.archive_mismatch == 1 and summary.skipped_at_execution == 1
    assert summary.exit_code == EXIT_FAILURES
    assert rwms.delete_calls == []
    assert (
        read_report(tmp_path / "report.csv")[0]["action"]
        == "skipped:open_archive_mismatch"
    )
    assert any(
        r.levelno == logging.CRITICAL and "manual review" in r.getMessage()
        for r in caplog.records
    )
    assert "archive_mismatch=1" in caplog.text


def test_open_archive_mismatch_does_not_trigger_auto_stop(tmp_path, session_factory):
    rwms = FakeRwms()
    for i in range(3):
        uid = seed(session_factory, rwms, f"a{i}")
        add_stale_archive(session_factory, uid, f"a{i}")
    seed(session_factory, rwms, "z-good")
    config = make_config(tmp_path, execute=True, max_consecutive_failures=2)

    _, summary = run(config, session_factory, rwms)

    assert summary.stop_reason is None
    assert summary.archive_mismatch == 3 and summary.failed == 0
    assert rwms.deleted == ["z-good"]
    assert summary.exit_code == EXIT_FAILURES


def test_panel_reread_error_blocks_archive_and_delete(tmp_path, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    rwms.strict_errors.add("u1")
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)
    assert summary.failed == 1
    assert archives(session_factory) == []
    assert rwms.delete_calls == []


def test_user_gone_from_panel_before_execution_is_skipped(tmp_path, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    rwms.on_strict = lambda username: rwms.users.pop(username)
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)
    assert summary.skipped_at_execution == 1 and summary.failed == 0
    assert archives(session_factory) == []
    assert rwms.delete_calls == []


def test_panel_uuid_change_is_a_failure(tmp_path, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")

    def recreate(username):
        rwms.users[username] = panel_user(username, uuid="uuid-new")

    rwms.on_strict = recreate
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)
    assert summary.failed == 1
    assert rwms.delete_calls == []


def test_state_change_between_scan_and_delete_is_rechecked(tmp_path, session_factory):
    rwms = FakeRwms()
    uid = seed(session_factory, rwms, "u1")

    def user_paid(_username):
        with session_factory() as s:
            s.get(User, uid).expire_at = naive(NOW + timedelta(days=30))
            s.commit()

    rwms.on_strict = user_paid
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert summary.skipped_at_execution == 1 and summary.deleted == 0
    assert archives(session_factory) == []
    assert rwms.delete_calls == []
    assert read_report(tmp_path / "report.csv")[0]["action"] == (
        "skipped:no_longer_eligible:db_user_active"
    )


def test_block_added_between_scan_and_delete_is_rechecked(tmp_path, session_factory):
    rwms = FakeRwms()
    uid = seed(session_factory, rwms, "u1")

    def blocked(_username):
        with session_factory() as s:
            s.add(UserBlock(user_id=uid))
            s.commit()

    rwms.on_strict = blocked
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)
    assert summary.deleted == 0
    assert rwms.delete_calls == []


# --- выборка и ограничители --------------------------------------------------


def test_stream_uses_expired_filter_timeout_and_page_size(tmp_path, session_factory):
    rwms = FakeRwms()
    for i in range(5):
        seed(session_factory, rwms, f"u{i}")
    rwms.users["active"] = panel_user("active", status=proto.UserStatus.ACTIVE)

    _, summary = run(
        make_config(tmp_path, page_size=2, full_scan=True), session_factory, rwms
    )

    assert summary.scanned == 5  # ACTIVE отфильтрован панелью
    assert [c["cursor"] for c in rwms.stream_calls] == [None, 2, 4]
    assert all(c["status"] == proto.UserStatus.EXPIRED for c in rwms.stream_calls)
    assert all(c["tag"] is None for c in rwms.stream_calls)
    assert all(c["external_squad_uuid"] is None for c in rwms.stream_calls)
    assert all(c["timeout"] == 60.0 and c["size"] == 2 for c in rwms.stream_calls)


def test_max_deletions_cap_and_early_stop(tmp_path, session_factory):
    rwms = FakeRwms()
    for i in range(6):
        seed(session_factory, rwms, f"u{i}")
    config = make_config(tmp_path, execute=True, max_deletions=2, page_size=1)

    _, summary = run(config, session_factory, rwms)

    assert summary.deleted == 2
    assert summary.scanned == 2  # перебор остановлен, как только набран лимит
    assert len(rwms.stream_calls) == 2
    assert len(archives(session_factory)) == 2


def test_min_sample_keeps_scanning_and_marks_over_cap(tmp_path, session_factory):
    rwms = FakeRwms()
    for i in range(6):
        seed(session_factory, rwms, f"u{i}")
    config = make_config(tmp_path, execute=True, max_deletions=2, ratio_min_sample=4)

    _, summary = run(config, session_factory, rwms)

    assert summary.scanned == 4
    assert summary.deleted == 2 and summary.over_cap == 2
    actions = [row["action"] for row in read_report(tmp_path / "report.csv")]
    assert actions.count("deleted") == 2 and actions.count("over_cap") == 2


def test_full_scan_never_deletes_more_than_cap(tmp_path, session_factory):
    rwms = FakeRwms()
    for i in range(6):
        seed(session_factory, rwms, f"u{i}")
    config = make_config(tmp_path, execute=True, max_deletions=2, full_scan=True)
    _, summary = run(config, session_factory, rwms)
    assert summary.scanned == 6
    assert summary.deleted == 2 and summary.over_cap == 4


def test_pause_between_deletions(tmp_path, session_factory):
    rwms = FakeRwms()
    for i in range(3):
        seed(session_factory, rwms, f"u{i}")
    sleeps = Sleeps()
    run(
        make_config(tmp_path, execute=True, delete_pause_seconds=2.5),
        session_factory,
        rwms,
        sleeps,
    )
    assert sleeps.calls == [2.5, 2.5]


def test_consecutive_failures_stop_the_run(tmp_path, session_factory):
    rwms = FakeRwms()
    for i in range(5):
        seed(session_factory, rwms, f"u{i}")
    config = make_config(tmp_path, execute=True, max_consecutive_failures=2)
    rwms.strict_errors = {f"u{i}" for i in range(5)}  # панель не отвечает

    _, summary = run(config, session_factory, rwms)

    assert summary.exit_code == EXIT_AUTO_STOP
    assert summary.stop_reason == "consecutive_failures"
    assert len(rwms.strict_calls) == 2 and rwms.delete_calls == []
    actions = [row["action"] for row in read_report(tmp_path / "report.csv")]
    assert actions.count("not_run:consecutive_failures") == 3


def test_success_resets_the_failure_counter(tmp_path, session_factory):
    rwms = FakeRwms()
    for i in range(4):
        seed(session_factory, rwms, f"u{i}")
    rwms.strict_errors = {"u0", "u2"}
    config = make_config(tmp_path, execute=True, max_consecutive_failures=2)
    _, summary = run(config, session_factory, rwms)
    assert summary.exit_code == EXIT_FAILURES
    assert summary.deleted == 2 and summary.failed == 2


def test_candidate_ratio_auto_stop(tmp_path, session_factory):
    rwms = FakeRwms()
    for i in range(4):
        seed(session_factory, rwms, f"u{i}")
    config = make_config(
        tmp_path, execute=True, max_candidate_ratio=0.5, ratio_min_sample=4
    )

    _, summary = run(config, session_factory, rwms)

    assert summary.exit_code == EXIT_AUTO_STOP
    assert summary.stop_reason == "candidate_ratio"
    assert rwms.delete_calls == [] and archives(session_factory) == []
    actions = {row["action"] for row in read_report(tmp_path / "report.csv")}
    assert actions == {"not_run:candidate_ratio"}


def test_candidate_ratio_guard_ignores_small_samples(tmp_path, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    config = make_config(
        tmp_path, execute=True, max_candidate_ratio=0.5, ratio_min_sample=10
    )
    _, summary = run(config, session_factory, rwms)
    assert summary.deleted == 1


def test_orphan_ratio_auto_stop(tmp_path, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    rwms.users["ghost"] = panel_user("ghost")
    config = make_config(
        tmp_path, execute=True, max_orphan_ratio=0.1, ratio_min_sample=2
    )

    _, summary = run(config, session_factory, rwms)

    assert summary.exit_code == EXIT_AUTO_STOP
    assert summary.stop_reason == "orphan_ratio"
    assert rwms.delete_calls == []


def test_orphans_are_skipped_without_flag(tmp_path, session_factory):
    rwms = FakeRwms()
    rwms.users["ghost"] = panel_user("ghost")
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)
    assert summary.orphans == 1 and summary.candidates == 0
    assert rwms.delete_calls == []


def test_orphans_are_never_deleted(tmp_path, session_factory):
    # R4: сирота без строки users (в т.ч. никогда не подключавшаяся, на
    # самой широкой ступени) не удаляется никогда — только строка в отчёте.
    rwms = FakeRwms()
    rwms.users["ghost"] = panel_user("ghost")
    rwms.users["ghost2"] = panel_user("ghost2", lifetime_traffic=5.0)
    config = make_config(tmp_path, execute=True, tier="all", max_orphan_ratio=1.0)
    _, summary = run(config, session_factory, rwms)
    assert summary.orphans == 2 and summary.candidates == 0
    assert summary.deleted == 0 and rwms.delete_calls == []
    assert rwms.strict_calls == [] and rwms.update_calls == []
    assert archives(session_factory) == []
    report = {row["username"]: row for row in read_report(tmp_path / "report.csv")}
    assert {r["reason"] for r in report.values()} == {"local_user_not_found"}
    assert {r["decision"] for r in report.values()} == {"skip"}


def test_cleaner_config_has_no_orphan_switch():
    assert "include_orphans" not in {
        f.name for f in dataclasses.fields(CleanerConfig)
    }


def test_allowlist_canary(tmp_path, session_factory):
    rwms = FakeRwms()
    for name in ("mine", "other1", "other2"):
        seed(session_factory, rwms, name)
    config = make_config(
        tmp_path,
        execute=True,
        username_allowlist=frozenset({"mine"}),
        ratio_min_sample=200,
        max_candidate_ratio=0.6,
    )
    _, summary = run(config, session_factory, rwms)
    assert summary.exit_code == EXIT_OK
    assert rwms.deleted == ["mine"]
    assert summary.scanned == 1
    assert summary.skip_reasons["not_in_allowlist"] == 2
    assert [row["username"] for row in read_report(tmp_path / "report.csv")] == ["mine"]


def test_stream_failure_aborts_without_deletions(tmp_path, session_factory):
    rwms = FakeRwms()
    for i in range(4):
        seed(session_factory, rwms, f"u{i}")
    rwms.fail_stream_on_call = 2
    _, summary = run(
        make_config(tmp_path, execute=True, page_size=2), session_factory, rwms
    )
    assert summary.exit_code == EXIT_PANEL_SCAN_FAILED
    assert rwms.delete_calls == [] and archives(session_factory) == []
    actions = {row["action"] for row in read_report(tmp_path / "report.csv")}
    assert actions == {"not_run:panel_scan_failed"}


def test_stuck_cursor_aborts(tmp_path, session_factory):
    rwms = FakeRwms()
    for i in range(6):
        seed(session_factory, rwms, f"u{i}")
    rwms.stuck_cursor = True
    _, summary = run(
        make_config(tmp_path, page_size=2, full_scan=True), session_factory, rwms
    )
    assert summary.exit_code == EXIT_PANEL_SCAN_FAILED


def test_tier_never_connected_skips_connected_users(tmp_path, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "fresh")
    seed(
        session_factory,
        rwms,
        "used",
        first_connected=days_ago(300),
        lifetime_traffic=1e6,
    )
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)
    assert rwms.deleted == ["fresh"]
    _, summary = run(
        make_config(tmp_path, execute=True, tier="never_paid"), session_factory, rwms
    )
    assert rwms.deleted == ["fresh", "used"]


# --- режим free_inactive -----------------------------------------------------


def enable_free_tier(session_factory, **extra):
    values = {
        "free_tier_enabled": "1",
        "free_tier_internal_squads": "free-squad",
        "free_tier_external_squad_uuid": "ext-free",
        "paid_external_squad_uuid": "ext-paid",
    }
    values.update(extra)
    with session_factory() as s:
        for key, value in values.items():
            s.add(SystemSetting(key=key, value=value))
        s.commit()


def seed_free(
    session_factory, rwms, username, *, online_at, switched_at, fid, tag=None
):
    with session_factory() as s:
        uid = add_user(s, username, expire_at=days_ago(60)).id
        s.add(
            FreeTierAccess(
                id=fid, user_id=uid, on_free=True, switched_at=naive(switched_at)
            )
        )
        s.commit()
    rwms.users[username] = panel_user(
        username,
        status=proto.UserStatus.ACTIVE,
        tag=tag,
        external_squad_uuid="ext-free",
        expire_at=NOW + timedelta(days=20000),
        first_connected=days_ago(100),
        online_at=online_at,
        lifetime_traffic=1e9,
    )
    return uid


def test_free_inactive_refuses_without_free_squad(tmp_path, session_factory, caplog):
    # Отряд FREE не настроен — отбирать бесплатные подписки не по чему (тег
    # принадлежит владельцу): режим отказывается работать с кодом ошибки.
    enable_free_tier(session_factory, free_tier_external_squad_uuid="")
    rwms = FakeRwms()
    seed(session_factory, rwms, "tagged-free", tag="FREE")
    with caplog.at_level(logging.ERROR):
        _, summary = run(
            make_config(tmp_path, mode="free_inactive", execute=True),
            session_factory,
            rwms,
        )
    assert summary.exit_code == EXIT_CONFIG
    assert summary.stop_reason == "free_squad_not_configured"
    assert rwms.stream_calls == [] and rwms.delete_calls == []
    assert archives(session_factory) == []
    assert "free_tier_external_squad_uuid" in caplog.text


def test_free_inactive_without_any_settings_refuses(tmp_path, session_factory):
    rwms = FakeRwms()
    _, summary = run(
        make_config(tmp_path, mode="free_inactive", execute=True), session_factory, rwms
    )
    assert summary.exit_code == EXIT_CONFIG
    assert rwms.stream_calls == []


def test_free_inactive_disabled_feature_with_squad_does_nothing(tmp_path, session_factory):
    # Отряд FREE задан, но функция выключена — как раньше: пропуск без ошибки.
    enable_free_tier(session_factory, free_tier_enabled="0")
    rwms = FakeRwms()
    _, summary = run(
        make_config(tmp_path, mode="free_inactive", execute=True), session_factory, rwms
    )
    assert summary.exit_code == EXIT_OK
    assert rwms.stream_calls == []


def test_free_inactive_archives_and_deletes(tmp_path, session_factory):
    enable_free_tier(session_factory, free_tier_inactive_days="30")
    rwms = FakeRwms()
    seed_free(
        session_factory,
        rwms,
        "idle",
        online_at=days_ago(45),
        switched_at=days_ago(50),
        fid=1,
    )
    seed_free(
        session_factory,
        rwms,
        "busy",
        online_at=days_ago(2),
        switched_at=days_ago(50),
        fid=2,
    )
    seed(session_factory, rwms, "expired-paid")  # не FREE — в выборку не попадает

    _, summary = run(
        make_config(tmp_path, mode="free_inactive", execute=True), session_factory, rwms
    )

    assert summary.exit_code == EXIT_OK
    assert rwms.deleted == ["idle"]
    assert all(
        c["external_squad_uuid"] == "ext-free"
        and c["tag"] is None
        and c["status"] is None
        for c in rwms.stream_calls
    )
    (archive,) = archives(session_factory)
    assert archive.reason == "free_inactive"
    assert archive.tag is None
    report = {row["username"]: row for row in read_report(tmp_path / "report.csv")}
    assert report["busy"]["reason"] == "free_recently_active"
    assert report["idle"]["tier"] == "-"


def test_free_inactive_streams_by_lowercased_free_squad(tmp_path, session_factory):
    enable_free_tier(
        session_factory,
        free_tier_inactive_days="30",
        free_tier_external_squad_uuid="EXT-FREE",
    )
    rwms = FakeRwms()
    seed_free(
        session_factory, rwms, "idle", online_at=days_ago(45), switched_at=days_ago(50), fid=1
    )
    config = make_config(
        tmp_path, mode="free_inactive", execute=True, max_free_candidate_ratio=1.0
    )
    _, summary = run(config, session_factory, rwms)
    assert rwms.deleted == ["idle"]
    assert {c["external_squad_uuid"] for c in rwms.stream_calls} == {"ext-free"}


def test_free_inactive_never_touches_owner_tagged(tmp_path, session_factory):
    enable_free_tier(session_factory, free_tier_inactive_days="30")
    rwms = FakeRwms()
    seed_free(
        session_factory, rwms, "idle", online_at=days_ago(45), switched_at=days_ago(50), fid=1
    )
    for fid, (name, tag) in enumerate(
        (("friend", "знакомый"), ("forever", "вечная ссылка"), ("legacy", "FREE")), 2
    ):
        seed_free(
            session_factory,
            rwms,
            name,
            online_at=days_ago(45),
            switched_at=days_ago(50),
            fid=fid,
            tag=tag,
        )

    _, summary = run(
        make_config(tmp_path, mode="free_inactive", execute=True), session_factory, rwms
    )

    assert rwms.deleted == ["idle"]
    assert [a.username for a in archives(session_factory)] == ["idle"]
    assert summary.skip_reasons["owner_tagged"] == 3
    report = {row["username"]: row for row in read_report(tmp_path / "report.csv")}
    for name in ("friend", "forever", "legacy"):
        assert report[name]["reason"] == "owner_tagged"
        assert report[name]["decision"] == "skip"


def test_expired_mode_never_touches_owner_tagged_or_free_squad(tmp_path, session_factory):
    enable_free_tier(session_factory)
    rwms = FakeRwms()
    seed(session_factory, rwms, "plain")
    seed(session_factory, rwms, "vip", tag="VIP")
    seed(session_factory, rwms, "old-free", tag="FREE")
    seed(session_factory, rwms, "free-squad", external_squad_uuid="ext-free")

    _, summary = run(
        make_config(tmp_path, execute=True, tier="all"), session_factory, rwms
    )

    assert rwms.deleted == ["plain"]
    assert [a.username for a in archives(session_factory)] == ["plain"]
    report = {row["username"]: row for row in read_report(tmp_path / "report.csv")}
    assert report["vip"]["reason"] == "owner_tagged"
    assert report["old-free"]["reason"] == "owner_tagged"
    assert report["free-squad"]["reason"] == "panel_free_squad"


def test_owner_tag_added_before_the_lock_prevents_deletion(tmp_path, session_factory):
    rwms = FakeRwms()
    uid = seed(session_factory, rwms, "u1")
    calls = []

    def tag_on_second_reread(username):
        calls.append(1)
        if len(calls) == 2:
            rwms.users[username].tag = "вечная ссылка"

    rwms.on_strict = tag_on_second_reread
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert rwms.delete_calls == []
    assert summary.deleted == 0
    assert read_report(tmp_path / "report.csv")[0]["action"] == (
        "skipped:no_longer_eligible_locked:owner_tagged"
    )
    # Архив, созданный этим запуском, закрыт как прерванный (подписка жива).
    (archive,) = archives(session_factory)
    assert archive.user_id == uid
    assert archive.restored_by == "rw-cleaner:aborted"


def test_owner_tag_added_after_scan_prevents_archive(tmp_path, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")

    def tag_now(username):
        rwms.users[username].tag = "VIP"

    rwms.on_strict = tag_now
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert rwms.delete_calls == [] and archives(session_factory) == []
    assert read_report(tmp_path / "report.csv")[0]["action"] == (
        "skipped:no_longer_eligible:owner_tagged"
    )


# --- отчёт и логи ------------------------------------------------------------


def test_report_has_all_columns_and_decisions(tmp_path, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "cand")
    seed(session_factory, rwms, "young", expire_at=days_ago(20))
    _, summary = run(make_config(tmp_path), session_factory, rwms)

    rows = read_report(tmp_path / "report.csv")
    assert summary.report_path == str(tmp_path / "report.csv")
    assert tuple(rows[0].keys()) == REPORT_COLUMNS
    by_name = {row["username"]: row for row in rows}
    assert by_name["cand"]["decision"] == "candidate"
    assert by_name["cand"]["tier"] == "never_connected"
    assert by_name["cand"]["panel_status"] == "EXPIRED"
    assert by_name["cand"]["panel_expire_at"] == "2026-06-13T12:00:00Z"
    assert by_name["cand"]["db_expire_at"] == "2026-06-13T12:00:00Z"
    assert by_name["cand"]["expire_diff_hours"] == "0.0"
    assert by_name["cand"]["ever_paid"] == "no"
    assert by_name["cand"]["lifetime_traffic_bytes"] == "0"
    assert by_name["young"]["decision"] == "skip"
    assert by_name["young"]["reason"] == "panel_not_old_enough"


def test_default_report_path(tmp_path, session_factory, monkeypatch):
    monkeypatch.chdir(tmp_path)
    rwms = FakeRwms()
    _, summary = run(make_config(tmp_path, report_path=None), session_factory, rwms)
    assert summary.report_path == "report/rw-cleaner-expired-20261011T120000Z.csv"
    assert (tmp_path / summary.report_path).exists()


def test_no_secrets_in_logs_or_report(tmp_path, session_factory, caplog):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    seed(session_factory, rwms, "u2")
    rwms.delete_results["u2"] = None
    with caplog.at_level(logging.DEBUG):
        run(make_config(tmp_path, execute=True), session_factory, rwms)

    text = caplog.text + (tmp_path / "report.csv").read_text(encoding="utf-8")
    assert "deleted" in caplog.text and "archived" in caplog.text
    for name in ("u1", "u2"):
        for secret in secrets_of(name):
            assert secret not in text


def test_candidate_and_deletion_logs_are_structured(tmp_path, session_factory, caplog):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    with caplog.at_level(logging.INFO):
        run(make_config(tmp_path, execute=True), session_factory, rwms)
    candidate = next(
        r.getMessage()
        for r in caplog.records
        if r.getMessage().startswith("candidate ")
    )
    deleted = next(
        r.getMessage() for r in caplog.records if r.getMessage().startswith("deleted ")
    )
    for line in (candidate, deleted):
        for key in (
            "username=u1",
            "panel_status=EXPIRED",
            "panel_expire_at=",
            "db_expire_at=",
            "first_connected_at=",
            "online_at=",
            "lifetime_traffic_bytes=0",
            "last_payment_at=",
            "tier=never_connected",
            "reason=expired_retention",
        ):
            assert key in line
    assert "archive_id=" in deleted


# --- блокировка строки users перед DeleteUser --------------------------------


class Recorder:
    """Порядок событий: COMMIT сессий, взятие блокировки, DeleteUser."""

    def __init__(self, monkeypatch, session_factory, rwms):
        self.events = []
        self.locked_ids = []
        original = cleanup.lock_user_query

        def lock(user_id):
            self.locked_ids.append(user_id)
            self.events.append("lock")
            return original(user_id)

        monkeypatch.setattr(cleanup, "lock_user_query", lock)
        event.listen(
            session_factory, "after_commit", lambda _s: self.events.append("commit")
        )
        previous = rwms.on_delete

        def on_delete(username):
            self.events.append("delete")
            if previous:
                previous(username)

        rwms.on_delete = on_delete


def test_lock_query_is_for_update_nowait():
    sql = str(cleanup.lock_user_query(42).compile(dialect=postgresql.dialect())).upper()
    assert "FROM USERS" in sql
    assert "FOR UPDATE NOWAIT" in sql


def test_delete_happens_inside_the_locked_transaction(
    tmp_path, session_factory, monkeypatch
):
    rwms = FakeRwms()
    uid = seed(session_factory, rwms, "u1")
    rec = Recorder(monkeypatch, session_factory, rwms)

    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert summary.deleted == 1
    assert rec.locked_ids == [uid]
    # архив закоммичен → блокировка → DeleteUser → COMMIT снимает блокировку
    assert rec.events == ["commit", "lock", "delete", "commit"]
    # Под блокировкой панель перечитана ещё раз.
    assert len(rwms.strict_calls) == 2


def test_extension_committed_before_the_lock_prevents_deletion(
    tmp_path, session_factory, monkeypatch
):
    # Гонка: продление (платёж) закоммитило users.expire_at уже ПОСЛЕ
    # решения и COMMIT архива, но ДО блокировки. Под блокировкой факты
    # перечитываются — удаления нет.
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

    async def extension_then_lock(row, user_id, archive_id, reused, *rest):
        assert len(archives(session_factory, "u1")) == 1  # архив уже закоммичен
        with session_factory() as s:
            s.get(User, uid).expire_at = naive(NOW + timedelta(days=30))
            s.commit()
        return await original(row, user_id, archive_id, reused, *rest)

    monkeypatch.setattr(cleaner, "_delete_under_lock", extension_then_lock)
    summary = asyncio.run(cleaner.run())

    assert rwms.delete_calls == []
    assert "u1" in rwms.users
    assert summary.deleted == 0 and summary.failed == 0
    assert summary.skipped_at_execution == 1
    assert read_report(tmp_path / "report.csv")[0]["action"] == (
        "skipped:no_longer_eligible_locked:db_user_active"
    )
    # Архив создан этим запуском, удаление прервано до DeleteUser — архив
    # закрыт отдельной транзакцией: рядом с живой подпиской открытого нет.
    (archive,) = archives(session_factory, "u1")
    assert archive.restored_at == naive(NOW)
    assert archive.restored_by == "rw-cleaner:aborted"
    assert summary.archived == 1 and summary.archive_closed_aborted == 1


@pytest.mark.parametrize("blocker", ["ban", "grace"])
def test_ban_or_grace_started_before_the_lock_prevents_deletion(
    tmp_path, session_factory, blocker
):
    rwms = FakeRwms()
    uid = seed(session_factory, rwms, "u1")
    calls = []

    def start_on_second_reread(_username):
        calls.append(1)
        if len(calls) != 2:
            return
        with session_factory() as s:
            if blocker == "ban":
                s.add(TemporarySquadBan(id=1, user_id=uid, unban_at=naive(NOW)))
            else:
                s.add(
                    GraceAccessGrant(
                        id=1, user_id=uid, squad_uuid="g", expires_at=naive(NOW)
                    )
                )
            s.commit()

    rwms.on_strict = start_on_second_reread
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert rwms.delete_calls == []
    reason = "temporary_squad_ban_active" if blocker == "ban" else "grace_access_active"
    assert read_report(tmp_path / "report.csv")[0]["action"] == (
        f"skipped:no_longer_eligible_locked:{reason}"
    )


def test_active_ban_or_grace_is_never_archived(tmp_path, session_factory):
    rwms = FakeRwms()
    banned = seed(session_factory, rwms, "banned")
    graced = seed(session_factory, rwms, "graced")
    with session_factory() as s:
        s.add(TemporarySquadBan(id=1, user_id=banned, unban_at=naive(NOW)))
        s.add(
            GraceAccessGrant(
                id=1, user_id=graced, squad_uuid="g", expires_at=naive(NOW)
            )
        )
        s.commit()

    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert summary.candidates == 0
    assert archives(session_factory) == []
    assert rwms.delete_calls == [] and rwms.strict_calls == []
    assert summary.skip_reasons["temporary_squad_ban_active"] == 1
    assert summary.skip_reasons["grace_access_active"] == 1


def test_panel_credentials_changed_before_lock_is_archive_mismatch(
    tmp_path, session_factory
):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    calls = []

    def regenerate_on_second_reread(username):
        calls.append(1)
        if len(calls) == 2:
            rwms.users[username] = panel_user(username, short_uuid="NewLink4567890")

    rwms.on_strict = regenerate_on_second_reread
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert rwms.delete_calls == []
    assert summary.archive_mismatch == 1 and summary.failed == 0
    # Архив этого запуска хранит СТАРЫЕ ключи рядом с живой подпиской с
    # новыми — закрыт, чтобы будущее восстановление не вернуло старую ссылку.
    (archive,) = archives(session_factory, "u1")
    assert archive.restored_by == "rw-cleaner:aborted"


def test_user_gone_from_panel_under_lock_is_skipped(tmp_path, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    calls = []

    def vanish_on_second_reread(username):
        calls.append(1)
        if len(calls) == 2:
            rwms.users.pop(username)

    rwms.on_strict = vanish_on_second_reread
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)
    assert rwms.delete_calls == []
    assert (
        read_report(tmp_path / "report.csv")[0]["action"] == "skipped:gone_from_panel"
    )
    # Подписки в панели нет — архив единственный способ вернуть ту же ссылку.
    (archive,) = archives(session_factory, "u1")
    assert archive.restored_at is None
    assert summary.archive_closed_aborted == 0


class _PgError(Exception):
    def __init__(self, pgcode):
        super().__init__(pgcode)
        self.pgcode = pgcode


def failing_lock_factory(session_factory, monkeypatch, error):
    """Сессии, у которых выполнение запроса блокировки падает с error."""
    marker = object()
    monkeypatch.setattr(cleanup, "lock_user_query", lambda _user_id: marker)

    def factory():
        session = session_factory()
        execute = session.execute

        def patched(statement, *args, **kwargs):
            if statement is marker:
                raise error
            return execute(statement, *args, **kwargs)

        session.execute = patched
        return session

    return factory


def test_row_locked_by_another_transaction_is_skipped(
    tmp_path, session_factory, monkeypatch
):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    error = OperationalError("SELECT ... FOR UPDATE NOWAIT", {}, _PgError("55P03"))
    factory = failing_lock_factory(session_factory, monkeypatch, error)

    _, summary = run(make_config(tmp_path, execute=True), factory, rwms)

    assert rwms.delete_calls == []
    assert summary.failed == 0 and summary.skipped_at_execution == 1
    assert (
        read_report(tmp_path / "report.csv")[0]["action"] == "skipped:user_row_locked"
    )
    (archive,) = archives(session_factory, "u1")
    assert archive.restored_by == "rw-cleaner:aborted"
    assert archive.restored_at == naive(NOW)
    assert summary.archive_closed_aborted == 1


@pytest.mark.parametrize(
    "error",
    [
        OperationalError("SELECT", {}, _PgError("08006")),
        RuntimeError("boom"),
    ],
)
def test_error_under_lock_fails_safe(tmp_path, session_factory, monkeypatch, error):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    factory = failing_lock_factory(session_factory, monkeypatch, error)

    _, summary = run(make_config(tmp_path, execute=True), factory, rwms)

    assert rwms.delete_calls == []
    assert summary.failed == 1 and summary.exit_code == EXIT_FAILURES
    assert read_report(tmp_path / "report.csv")[0]["action"] == (
        "failed:locked_recheck_failed"
    )
    (archive,) = archives(session_factory, "u1")
    assert archive.restored_by == "rw-cleaner:aborted"
    assert summary.archive_closed_aborted == 1


def test_delete_rpc_exception_is_a_failure(tmp_path, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")

    def explode(_username):
        raise RuntimeError("grpc exploded")

    rwms.on_delete = explode
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)
    assert summary.delete_uncertain == 1 and summary.deleted == 0
    assert summary.exit_code == EXIT_UNCERTAIN
    assert read_report(tmp_path / "report.csv")[0]["action"] == (
        "failed:delete_uncertain"
    )
    # Исход DeleteUser неизвестен — архив остаётся открытым.
    (archive,) = archives(session_factory, "u1")
    assert archive.restored_at is None
    assert summary.archive_closed_aborted == 0


def test_dry_run_and_orphans_take_no_lock(tmp_path, session_factory, monkeypatch):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    rwms.users["ghost"] = panel_user("ghost")
    rec = Recorder(monkeypatch, session_factory, rwms)

    run(make_config(tmp_path), session_factory, rwms)
    assert rec.locked_ids == []

    del rwms.users["u1"]
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)
    # Сирота не удаляется вовсе — блокировать нечего.
    assert summary.deleted == 0 and rwms.delete_calls == []
    assert rec.locked_ids == []


# --- F7: закрытие архива после прерванного до DeleteUser удаления -------------


def test_reused_archive_is_never_closed_on_abort(tmp_path, session_factory, monkeypatch):
    # Архив остался от прошлого запуска (reused); под блокировкой продление
    # отменяет удаление — чужой (не этого запуска) архив не трогаем.
    rwms = FakeRwms()
    uid = seed(session_factory, rwms, "u1")
    with session_factory() as s:
        s.add(build_archive(rwms.users["u1"], user_id=uid, reason="expired_retention"))
        s.commit()
    calls = []

    def extend_on_second_reread(_username):
        calls.append(1)
        if len(calls) == 2:
            with session_factory() as s:
                s.get(User, uid).expire_at = naive(NOW + timedelta(days=30))
                s.commit()

    rwms.on_strict = extend_on_second_reread
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert rwms.delete_calls == []
    assert summary.archive_reused == 1 and summary.archive_closed_aborted == 0
    (archive,) = archives(session_factory, "u1")
    assert archive.restored_at is None and archive.restored_by is None


def test_panel_uuid_change_under_lock_closes_this_runs_archive(tmp_path, session_factory):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    calls = []

    def recreate_on_second_reread(username):
        calls.append(1)
        if len(calls) == 2:
            rwms.users[username] = panel_user(username, uuid="uuid-new")

    rwms.on_strict = recreate_on_second_reread
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert rwms.delete_calls == []
    assert read_report(tmp_path / "report.csv")[0]["action"] == "failed:panel_uuid_changed"
    (archive,) = archives(session_factory, "u1")
    assert archive.restored_by == "rw-cleaner:aborted"


def test_panel_reread_failure_under_lock_closes_this_runs_archive(
    tmp_path, session_factory
):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    calls = []

    def fail_second_reread(username):
        calls.append(1)
        if len(calls) == 2:
            rwms.strict_errors.add(username)

    rwms.on_strict = fail_second_reread
    _, summary = run(make_config(tmp_path, execute=True), session_factory, rwms)

    assert rwms.delete_calls == []
    assert summary.failed == 1
    (archive,) = archives(session_factory, "u1")
    assert archive.restored_by == "rw-cleaner:aborted"


def test_archive_closed_by_someone_else_is_left_as_is(
    tmp_path, session_factory, monkeypatch
):
    # Между созданием архива и закрытием его уже закрыл другой путь —
    # restored_by не перезаписываем.
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

    async def restore_then_lock(row, user_id, archive_id, reused, *rest):
        with session_factory() as s:
            s.get(SubscriptionArchive, archive_id).restored_at = naive(NOW)
            s.get(SubscriptionArchive, archive_id).restored_by = "bot:start"
            s.get(User, uid).expire_at = naive(NOW + timedelta(days=30))
            s.commit()
        return await original(row, user_id, archive_id, reused, *rest)

    monkeypatch.setattr(cleaner, "_delete_under_lock", restore_then_lock)
    summary = asyncio.run(cleaner.run())

    assert rwms.delete_calls == []
    (archive,) = archives(session_factory, "u1")
    assert archive.restored_by == "bot:start"
    assert summary.archive_closed_aborted == 0


def test_archive_close_failure_is_logged_and_keeps_outcome(
    tmp_path, session_factory, monkeypatch, caplog
):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    error = OperationalError("SELECT ... FOR UPDATE NOWAIT", {}, _PgError("55P03"))
    lock_factory = failing_lock_factory(session_factory, monkeypatch, error)

    def factory():
        session = lock_factory()
        get = session.get

        def patched_get(entity, ident, **kwargs):
            if kwargs.get("with_for_update"):
                raise RuntimeError("db down while closing")
            return get(entity, ident, **kwargs)

        session.get = patched_get
        return session

    with caplog.at_level(logging.INFO):
        _, summary = run(make_config(tmp_path, execute=True), factory, rwms)

    assert rwms.delete_calls == []
    assert summary.skipped_at_execution == 1 and summary.failed == 0
    assert summary.archive_close_failed == 1 and summary.archive_closed_aborted == 0
    assert archives(session_factory, "u1")[0].restored_at is None
    assert any(
        r.levelno == logging.ERROR and "failed to close the archive" in r.getMessage()
        for r in caplog.records
    )


def test_aborted_archive_close_logs_have_no_secrets(
    tmp_path, session_factory, monkeypatch, caplog
):
    rwms = FakeRwms()
    seed(session_factory, rwms, "u1")
    error = OperationalError("SELECT ... FOR UPDATE NOWAIT", {}, _PgError("55P03"))
    factory = failing_lock_factory(session_factory, monkeypatch, error)

    with caplog.at_level(logging.DEBUG):
        run(make_config(tmp_path, execute=True), factory, rwms)

    assert "aborted deletion: archive closed" in caplog.text
    assert "aborted_by=skipped:user_row_locked" in caplog.text
    for secret in secrets_of("u1"):
        assert secret not in caplog.text


# --- free_inactive: свой порог доли кандидатов ---------------------------------


def _seed_idle_free(session_factory, rwms, count):
    for i in range(count):
        seed_free(
            session_factory,
            rwms,
            f"idle{i}",
            online_at=days_ago(45),
            switched_at=days_ago(50),
            fid=100 + i,
        )


def test_free_inactive_uses_its_own_candidate_ratio(tmp_path, session_factory):
    # 100% неактивных бесплатных: порог expired (0.5) не применяется,
    # max_free_candidate_ratio=1.0 пропускает.
    enable_free_tier(session_factory, free_tier_inactive_days="30")
    rwms = FakeRwms()
    _seed_idle_free(session_factory, rwms, 4)
    config = make_config(
        tmp_path,
        mode="free_inactive",
        execute=True,
        max_candidate_ratio=0.5,
        max_free_candidate_ratio=1.0,
        ratio_min_sample=4,
    )

    _, summary = run(config, session_factory, rwms)

    assert summary.stop_reason is None
    assert summary.deleted == 4


def test_free_inactive_candidate_ratio_auto_stop(tmp_path, session_factory, caplog):
    enable_free_tier(session_factory, free_tier_inactive_days="30")
    rwms = FakeRwms()
    _seed_idle_free(session_factory, rwms, 4)
    config = make_config(
        tmp_path,
        mode="free_inactive",
        execute=True,
        max_candidate_ratio=1.0,
        max_free_candidate_ratio=0.9,
        ratio_min_sample=4,
    )

    with caplog.at_level(logging.INFO):
        _, summary = run(config, session_factory, rwms)

    assert summary.exit_code == EXIT_AUTO_STOP
    assert summary.stop_reason == "candidate_ratio"
    assert rwms.delete_calls == [] and archives(session_factory) == []
    assert "max_free_candidate_ratio=0.9" in caplog.text


def test_expired_mode_ignores_free_candidate_ratio(tmp_path, session_factory):
    rwms = FakeRwms()
    for i in range(4):
        seed(session_factory, rwms, f"u{i}")
    config = make_config(
        tmp_path,
        execute=True,
        max_candidate_ratio=1.0,
        max_free_candidate_ratio=0.1,
        ratio_min_sample=4,
    )

    _, summary = run(config, session_factory, rwms)

    assert summary.stop_reason is None and summary.deleted == 4
