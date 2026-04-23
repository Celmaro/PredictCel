from __future__ import annotations

import os
import sys
import time

from .main import main as run_cli


def main() -> None:
    interval_seconds = int(os.getenv("PREDICTCEL_RUN_INTERVAL_SECONDS", "300"))
    run_once = _env_enabled("PREDICTCEL_RUN_ONCE", default=False)

    while True:
        _run_once_from_env()
        if run_once:
            return
        time.sleep(max(interval_seconds, 30))


def _run_once_from_env() -> None:
    config_path = os.getenv("PREDICTCEL_CONFIG", "config/predictcel.example.json")
    db_path = os.getenv("PREDICTCEL_DB", "/data/predictcel.db")

    argv = ["predictcel", "--config", config_path, "--db", db_path]
    if _env_enabled("PREDICTCEL_LIVE_DATA", default=False):
        argv.append("--live-data")
    if _env_enabled("PREDICTCEL_LIVE_TRADING", default=False):
        argv.append("--live-trading")

    previous_argv = sys.argv
    try:
        sys.argv = argv
        run_cli()
    finally:
        sys.argv = previous_argv


def _env_enabled(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


if __name__ == "__main__":
    main()
