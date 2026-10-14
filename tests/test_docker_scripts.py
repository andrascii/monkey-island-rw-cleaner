"""Docker-обвязка: run.sh не пускает --execute без явного разрешения,
передаёт аргументы в `docker compose run` и возвращает код выхода
rw-cleaner; образ не тащит секреты и тесты."""

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DOCKER_DIR = ROOT / "docker"


@pytest.fixture
def server_dir(tmp_path):
    """Каталог «как на сервере»: run.sh + .env + фейковый docker в PATH,
    который пишет свои аргументы в файл и выходит с кодом FAKE_DOCKER_RC."""
    work = tmp_path / "rw-cleaner"
    work.mkdir()
    shutil.copy(DOCKER_DIR / "run.sh", work / "run.sh")
    (work / ".env").write_text("RWMS_HOST=localhost\n")

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake = bin_dir / "docker"
    fake.write_text(
        '#!/usr/bin/env bash\n'
        'printf "%s\\n" "$@" > "$FAKE_DOCKER_LOG"\n'
        'exit "${FAKE_DOCKER_RC:-0}"\n'
    )
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    return work, bin_dir, tmp_path / "docker-args.txt"


def run_sh(server_dir, *args, rc=0, allow=None):
    work, bin_dir, log = server_dir
    env = {
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "FAKE_DOCKER_LOG": str(log),
        "FAKE_DOCKER_RC": str(rc),
    }
    if allow is not None:
        env["RW_CLEANER_ALLOW_EXECUTE"] = allow
    result = subprocess.run(
        ["bash", str(work / "run.sh"), *args],
        env=env,
        capture_output=True,
        text=True,
    )
    docker_args = log.read_text().splitlines() if log.exists() else None
    return result, docker_args


def test_dry_run_args_passed_to_compose_run(server_dir):
    result, docker_args = run_sh(server_dir, "--full-scan", "--mode", "expired")
    assert result.returncode == 0
    assert docker_args[0] == "compose"
    i = docker_args.index("run")
    assert docker_args[i:] == [
        "run", "--rm", "-T", "rw-cleaner", "--full-scan", "--mode", "expired",
    ]
    work = server_dir[0]
    assert (work / "log").is_dir() and (work / "report").is_dir()


@pytest.mark.parametrize("allow", [None, "", "no", "true", "YES"])
def test_execute_blocked_without_explicit_permission(server_dir, allow):
    result, docker_args = run_sh(server_dir, "--execute", allow=allow)
    assert result.returncode == 2
    assert docker_args is None, "docker must not be called"
    assert "--execute is blocked" in result.stderr


def test_execute_allowed_with_permission(server_dir):
    result, docker_args = run_sh(
        server_dir, "--execute", "--max-deletions", "2", allow="yes"
    )
    assert result.returncode == 0
    assert docker_args[-3:] == ["--execute", "--max-deletions", "2"]


@pytest.mark.parametrize("rc", [1, 4, 6])
def test_exit_code_propagated(server_dir, rc):
    result, _ = run_sh(server_dir, "--full-scan", rc=rc)
    assert result.returncode == rc
    assert f"rw-cleaner exit code: {rc}" in result.stderr


def test_missing_env_refuses(server_dir):
    (server_dir[0] / ".env").unlink()
    result, docker_args = run_sh(server_dir, "--full-scan")
    assert result.returncode == 2
    assert docker_args is None


def test_dockerignore_keeps_secrets_and_tests_out_of_image():
    entries = (ROOT / ".dockerignore").read_text().split()
    for required in (".env", ".git", "tests/", "common/tests/", "log/", "report/", "*.tar"):
        assert required in entries


def test_compose_keeps_state_and_logs_on_host_volumes():
    compose = (DOCKER_DIR / "docker-compose.yml").read_text()
    assert "./log:/app/log" in compose
    assert "./report:/app/report" in compose
    assert 'restart: "no"' in compose
