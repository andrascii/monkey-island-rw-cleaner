"""README: общий порядок выката/отката CloudPayments (R-DOCS, ревью CP, круг 3).

Документ — часть процедуры выката: оператор действует по нему. Тест держит
ключевые факты абзаца R-DOCS (миграцию применяет только бот, после первой
оплаты CP откат на старый common запрещён, аварийный рубильник
CP_CHARGE_ENABLED=false — только вместе с MI_UN_CP_CHARGE_ENABLED=false в
user-notify) и не даёт вернуть прежнее «напоминания им теперь приходят» без
второго рубильника (круг 3, M1/L4).
"""

import re
from pathlib import Path

README = Path(__file__).resolve().parents[1] / "README.md"


def _text():
    return re.sub(r"\s+", " ", README.read_text(encoding="utf-8"))


def _rollback_paragraph():
    text = _text()
    start = text.index("Откат: сначала убрать варианты CP.")
    return text[start:start + 1500]


def test_readme_has_shared_cloudpayments_rollout_block():
    text = _text()
    assert "### Порядок выката CloudPayments (общий для всех сервисов)" in README.read_text(
        encoding="utf-8"
    ) or "**Порядок выката CloudPayments (общий для всех сервисов):**" in text
    assert "Миграцию применяет ТОЛЬКО бот — ни в одном другом сервисе alembic не запускать." in text
    assert "MI_YKP_CP_ORDER_SECRET" in text and "MI_VPN_BOT_CP_ORDER_SECRET" in text
    assert "CP_AUTOPAY_RETRY_DELAY_HOURS = payment MI_YKP_CP_AUTOPAY_RETRY_DELAY_HOURS" in text


def test_rollback_paragraph_requires_user_notify_mirror_switch():
    paragraph = _rollback_paragraph()
    assert "НИКАКОЙ сервис не откатывается на образ со старым common" in paragraph
    assert (
        "CP_CHARGE_ENABLED=false в yk-recurrent — аварийный рубильник; выставлять ТОЛЬКО "
        "вместе с MI_UN_CP_CHARGE_ENABLED=false в user-notify (и перезапуском обоих)"
    ) in paragraph
    # Круг 3: один рубильник yk-recurrent напоминаний НЕ возвращает.
    assert "напоминания им теперь приходят" not in paragraph
    # Рубильник — аварийный, не обычный шаг отката (rw-cleaner, круг 1).
    assert "Откат: сначала убрать варианты CP и" not in _text()


def test_rw_cleaner_old_image_only_dry_run_after_first_cp_payment():
    text = _text()
    start = text.index("### Порядок выката CloudPayments")
    section = text[start:start + 4000]
    assert "только dry-run, без `--execute`" in section


# Общий абзац выката/отката (R-DOCS) — байт-в-байт одинаковый в README
# payment, yk-recurrent, сайта (и docker/website/PRODUCTION.md), бота,
# user-notify, rw-cleaner, ip-guard. Тот же SHA-256 стоит в тестах соседних
# сервисов: правка абзаца — во всех README сразу и новый хэш во всех тестах.
ROLLOUT_BLOCK_START = "1. common сливается первым; бот стартует первым и применяет миграцию e07e3968ff54.\n"
ROLLOUT_BLOCK_END = "горизонте напоминаний (3 дня).\n"
ROLLOUT_BLOCK_SHA256 = "da9900ad46e6604b014772c0798bb40c934a479bd1b5f01539e4d8fde1e081a0"


def test_rollout_block_is_byte_identical_across_services():
    import hashlib

    raw = README.read_text(encoding="utf-8")
    assert raw.count(ROLLOUT_BLOCK_START) == 1
    start = raw.index(ROLLOUT_BLOCK_START)
    end = raw.index(ROLLOUT_BLOCK_END, start) + len(ROLLOUT_BLOCK_END)
    block = raw[start:end]
    assert hashlib.sha256(block.encode("utf-8")).hexdigest() == ROLLOUT_BLOCK_SHA256


def test_rollout_block_names_both_bots_and_actual_alert_behaviour():
    # Ревью CP, круг 4, D1: шаг 1 называет оба бота (VPN и VPS) по одному;
    # фраза об алерте — как ведёт себя yk-recurrent после Y1.
    raw = README.read_text(encoding="utf-8")
    start = raw.index(ROLLOUT_BLOCK_START)
    end = raw.index(ROLLOUT_BLOCK_END, start) + len(ROLLOUT_BLOCK_END)
    flat = " ".join(raw[start:end].split())
    assert "Оба бота (VPN и VPS) — по одному: второй — сразу после миграции первого" in flat
    assert (
        "алерт при старте, если есть цепочки CP (если CP выключен из-за настройки — в любом "
        "случае), и раз в час, "
        "пока есть цепочки CP в горизонте напоминаний (3 дня)."
    ) in flat
    assert "алерт при старте и раз в час." not in flat
    # Круг 7, D1: при явном CP_CHARGE_ENABLED=false алерт при старте —
    # только если цепочки CP есть (как __alert_cp_charge_disabled_at_start).
    assert "алерт при старте (если CP выключен" not in flat


def test_pg_test_example_never_points_at_local_dev_db():
    # Ревью CP, круг 7, N2: 127.0.0.1:5432 — локальная дев-БД (эталон alembic
    # autogenerate); пример PG-тестов — только одноразовый контейнер.
    raw = README.read_text(encoding="utf-8")
    lines = [line for line in raw.splitlines() if "RW_CLEANER_TEST_PG_URL=" in line]
    assert lines
    for line in lines:
        assert "127.0.0.1:5432" not in line and "localhost:5432" not in line
        assert "127.0.0.1:55439/ykrtest" in line
