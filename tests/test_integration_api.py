# PROMPT: Integration test that runs data/assertions.py against a locally started Go API.
# CHANGES MADE: Added subprocess fixture with random PORT, loads assertions via importlib, and fails on any assertion error.

import importlib.util
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
import requests

ROOT = Path(__file__).resolve().parents[1]


def _load_assertions_module():
    path = ROOT / "data" / "assertions.py"
    spec = importlib.util.spec_from_file_location("challenge_assertions", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def api_base_url():
    if os.getenv("SKIP_API_INTEGRATION") == "1":
        pytest.skip("API integration skipped")

    port = os.getenv("TEST_API_PORT", "18080")
    import tempfile

    data_dir = Path(tempfile.mkdtemp(prefix="apex_api_test_"))

    env = os.environ.copy()
    env["PORT"] = port
    env["DATA_DIR"] = str(data_dir)
    env["IPC_SOCKET_PATH"] = str(data_dir / "test.sock")

    proc = subprocess.Popen(
        ["go", "run", "./app"],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )

    base = f"http://127.0.0.1:{port}"
    deadline = time.time() + 45
    ready = False
    while time.time() < deadline:
        try:
            resp = requests.get(f"{base}/health", timeout=2)
            if resp.status_code == 200:
                ready = True
                break
        except requests.RequestException:
            pass
        if proc.poll() is not None:
            err = proc.stderr.read().decode("utf-8", errors="replace")
            pytest.fail(f"API process exited early: {err}")
        time.sleep(0.5)

    if not ready:
        proc.terminate()
        pytest.fail("API did not become ready in time")

    yield base

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def test_challenge_assertions(api_base_url):
    assertions = _load_assertions_module()
    failures = assertions.run_all(api_base_url)
    if failures:
        pytest.fail("assertions failed:\n" + "\n".join(failures))


if __name__ == "__main__":
    base = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"
    mod = _load_assertions_module()
    failed = mod.run_all(base)
    raise SystemExit(1 if failed else 0)
