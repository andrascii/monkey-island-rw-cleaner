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
ROLLOUT_BLOCK_END = "при выключенном CP списатель шлёт алерт при старте и раз в\nчас.\n"
ROLLOUT_BLOCK_SHA256 = "e8b5aed0261eff2263d2138343b5a69e98137dc4901c4dd70a69eaf82b851863"


def test_rollout_block_is_byte_identical_across_services():
    import hashlib

    raw = README.read_text(encoding="utf-8")
    assert raw.count(ROLLOUT_BLOCK_START) == 1
    start = raw.index(ROLLOUT_BLOCK_START)
    end = raw.index(ROLLOUT_BLOCK_END, start) + len(ROLLOUT_BLOCK_END)
    block = raw[start:end]
    assert hashlib.sha256(block.encode("utf-8")).hexdigest() == ROLLOUT_BLOCK_SHA256
