from __future__ import annotations

import json
import os
import sys
import time
import traceback
from datetime import UTC, datetime
from pathlib import PurePosixPath

from .main import main as run_cli

VALID_MODES = {"paper", "live-data", "dry-run-trading", "live-trading"}
LIVE_MODES = {"live-data", "dry-run-trading", "live-trading"}


def main() -> None:
    interval_seconds = int(
        os.getenv("PREDICTCEL_RUN_INTERVAL_SECONDS", str(_default_interval_seconds()))
    )
    run_once = _env_enabled("PREDICTCEL_RUN_ONCE", default=False)
    interval = max(interval_seconds, 30)

    while True:
        started = time.perf_counter()
        try:
            _run_once_from_env()
            _log_event(
                "predictcel_run_complete",
                {
                    "duration_ms": _elapsed_ms(started),
                    "next_interval_seconds": 0 if run_once else interval,
                },
            )
        except Exception as exc:  # pragma: no cover - defensive worker boundary
            _log_event(
                "predictcel_run_error",
                {
                    "duration_ms": _elapsed_ms(started),
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                },
            )
        if run_once:
            return
        time.sleep(interval)


def _run_once_from_env() -> None:
    config_path = os.getenv("PREDICTCEL_CONFIG", "config/predictcel.example.json")
    db_path = _default_db_path()
    mode = os.getenv("PREDICTCEL_MODE", "").strip().lower()

    argv = ["predictcel", "--config", config_path, "--db", db_path]
    if mode:
        if mode not in VALID_MODES:
            raise ValueError(
                f"Invalid PREDICTCEL_MODE={mode!r}. Expected one of {sorted(VALID_MODES)}."
            )
        if mode in LIVE_MODES:
            argv.append("--live-data")
        if mode in {"dry-run-trading", "live-trading"}:
            argv.append("--live-trading")
    else:
        if _env_enabled("PREDICTCEL_LIVE_DATA", default=False):
            argv.append("--live-data")
        if _env_enabled("PREDICTCEL_LIVE_TRADING", default=False):
            argv.append("--live-trading")

    _log_event(
        "predictcel_run_start",
        {
            "mode": mode or "legacy-env",
            "config_path": config_path,
            "db_path": db_path,
            "argv": argv[1:],
        },
    )
    previous_argv = sys.argv
    try:
        sys.argv = argv
        run_cli()
    finally:
        sys.argv = previous_argv


def _default_interval_seconds() -> int:
    mode = os.getenv("PREDICTCEL_MODE", "").strip().lower()
    legacy_live = _env_enabled("PREDICTCEL_LIVE_DATA", default=False) or _env_enabled(
        "PREDICTCEL_LIVE_TRADING", default=False
    )
    return 60 if mode in LIVE_MODES or legacy_live else 300


def _default_db_path() -> str:
    explicit = os.getenv("PREDICTCEL_DB")
    if explicit:
        return explicit
    volume_mount = os.getenv("RAILWAY_VOLUME_MOUNT_PATH")
    if volume_mount:
        return str(PurePosixPath(volume_mount) / "predictcel.db")
    return "/tmp/predictcel.db"


def _env_enabled(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _elapsed_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)


def _log_event(event: str, payload: dict) -> None:
    print(
        json.dumps(
            {"event": event, "ts": datetime.now(UTC).isoformat(), **payload},
            sort_keys=True,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
