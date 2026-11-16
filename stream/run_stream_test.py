"""
stream/run_stream_test.py
-------------------------
Automated Part D2 & D3 harness launching the dispatcher alongside 3 driver consumers.
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

STREAM_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = STREAM_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.settings import get_amqp_url

DURATION_SECONDS = 60
LOG_FILE = STREAM_DIR / "stream_execution.log"
DRIVER_SPECS: tuple[tuple[str, str], ...] = (
    ("Ravi", "bangalore.koramangala.*"),
    ("Arjun", "bangalore.*.ride"),
    ("Control", "bangalore.#"),
)


def _pipe_output(pipe, log_handle) -> None:
    try:
        for line in iter(pipe.readline, ""):
            if not line:
                break
            stamped = f"[{datetime.now().strftime('%H:%M:%S')}] {line.rstrip()}"
            print(stamped, flush=True)
            log_handle.write(stamped + "\n")
            log_handle.flush()
    finally:
        pipe.close()


def _spawn_worker(cmd: list[str], env: dict[str, str], log_handle) -> subprocess.Popen:
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        cwd=str(STREAM_DIR),
        env=env,
    )
    thread = threading.Thread(target=_pipe_output, args=(proc.stdout, log_handle), daemon=True)
    thread.start()
    return proc


def run_test() -> None:
    """Start driver consumers and the dispatcher, stream for DURATION_SECONDS, and log output."""
    amqp_url = get_amqp_url()
    if not amqp_url:
        print("[ERROR] AMQP_URL is not set.")
        sys.exit(1)

    broker_host = amqp_url.split("@")[-1] if "@" in amqp_url else "localhost"
    print("=" * 64)
    print("  STARTING STREAMING TEST (Part D2 & D3)")
    print(f"  Broker: {broker_host}")
    print(f"  Duration: {DURATION_SECONDS} seconds")
    print("=" * 64 + "\n", flush=True)

    worker_env = {**os.environ, "AMQP_URL": amqp_url, "PYTHONUNBUFFERED": "1"}

    with LOG_FILE.open("w", encoding="utf-8") as log_handle:
        log_handle.write(f"=== STREAMING RUN STARTED: {datetime.now()} ===\n\n")
        log_handle.flush()

        processes: list[subprocess.Popen] = []
        for name, pattern in DRIVER_SPECS:
            cmd = [sys.executable, "-u", str(STREAM_DIR / "driver.py"), name, pattern]
            processes.append(_spawn_worker(cmd, worker_env, log_handle))
            time.sleep(1)

        disp_cmd = [sys.executable, "-u", str(STREAM_DIR / "dispatcher.py")]
        processes.append(_spawn_worker(disp_cmd, worker_env, log_handle))

        print(f"\n[INFO] All processes started. Running for {DURATION_SECONDS}s...\n", flush=True)
        started = time.time()
        try:
            while time.time() - started < DURATION_SECONDS:
                time.sleep(2)
        except KeyboardInterrupt:
            print("\n[INFO] Interrupted by user.")

        print("\n[INFO] Test duration completed. Stopping all workers...", flush=True)
        for proc in processes:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()

        time.sleep(1)
        log_handle.write(f"\n=== STREAMING RUN FINISHED: {datetime.now()} ===\n")

    print("\n" + "=" * 64)
    print(f"  TEST COMPLETE! Full execution log saved to: {LOG_FILE}")
    print("=" * 64 + "\n", flush=True)


if __name__ == "__main__":
    run_test()
