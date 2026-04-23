from __future__ import annotations

import json
import os
import sys
import time
import traceback
from datetime import UTC, datetime

from .main import main as run_cli

VALID_MODES = {"paper", "live-data", "dry-run-trading", "live-trading"}


def main() -> None:
    interval_seconds = int(os.getenv("PREDICTCEL_RUN_INTERVAL_SECONDS", "300"))
    run_once = _env_enabled("PREDICTCEL_RUN_ONCE", default=False)

    while True:
        try:
            _run_once_from_env()
        except Exception as exc:  # pragma: no cover - defensive worker boundary
            _log_event(
                "predictcel_run_error",
                {
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                },
            )
        if run_once:
            return
        time.sleep(max(interval_seconds, 30))


def _run_once_from_env() -> None:
    config_path = os.getenv("PREDICTCEL_CONFIG", "config/predictcel.example.json")
    db_path = os.getenv("PREDICTCEL_DB", "/data/predictcel.db")
    mode = os.getenv("PREDICTCEL_MODE", "").strip().lower()

    argv = ["predictcel", "--config", config_path, "--db", db_path]
    if mode:
        if mode not in VALID_MODES:
            raise ValueError(f"Invalid PREDICTCEL_MODE={mode!r}. Expected one of {sorted(VALID_MODES)}.")
        if mode in {"live-data", "dry-run-trading", "live-trading"}:
            argv.append("--live-data")
        if mode in {"dry-run-trading", "live-trading"}:
            argv.append("--live-trading")
    else:
        if _env_enabled("PREDICTCEL_LIVE_DATA", default=False):
            argv.append("--live-data")
        if _env_enabled("PREDICTCEL_LIVE_TRADING", default=False):
            argv.append("--live-trading")

    _log_event("predictcel_run_start", {"mode": mode or "legacy-env", "db_path": db_path})
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


def _log_event(event: str, payload: dict) -> None:
    print(json.dumps({"event": event, "ts": datetime.now(UTC).isoformat(), **payload}, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
