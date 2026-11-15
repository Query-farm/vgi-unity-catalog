"""Shared fixtures: the strict Unity Catalog replay server."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


def _start(env: dict[str, str]) -> tuple[subprocess.Popen[str], str]:
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "test" / "mock_uc" / "server.py")],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        encoding="utf-8",
        env={k: v for k, v in {**os.environ, **env}.items() if not k.startswith("MOCK_RECORD")},
    )
    assert proc.stdout is not None
    line = proc.stdout.readline().strip()
    assert line.startswith("PORT:"), line
    return proc, f"http://127.0.0.1:{line.removeprefix('PORT:')}"


@pytest.fixture(scope="session")
def mock_uc() -> Iterator[str]:
    """The open-source replay server (flaky: every third distinct request gets one 429)."""
    proc, url = _start({"MOCK_FLAKY": "1"})
    yield url
    proc.terminate()
    proc.wait(timeout=10)


@pytest.fixture(scope="session")
def mock_dbx() -> Iterator[str]:
    """The Databricks-flavoured replay server."""
    proc, url = _start({"MOCK_FLAVOR": "databricks"})
    yield url
    proc.terminate()
    proc.wait(timeout=10)
