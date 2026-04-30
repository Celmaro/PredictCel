from __future__ import annotations

import atexit
import os
import threading
from concurrent.futures import ThreadPoolExecutor

_executor_lock = threading.Lock()
_io_executor: ThreadPoolExecutor | None = None
_compute_executor: ThreadPoolExecutor | None = None
_DEFAULT_IO_WORKERS = 32
_DEFAULT_COMPUTE_WORKERS = max(8, min(32, os.cpu_count() or 8))
_IO_WORKERS_ENV_VAR = "PREDICTCEL_IO_WORKERS"
_COMPUTE_WORKERS_ENV_VAR = "PREDICTCEL_COMPUTE_WORKERS"


def _env_worker_count(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def shared_io_executor() -> ThreadPoolExecutor:
    global _io_executor
    with _executor_lock:
        if _io_executor is None:
            _io_executor = ThreadPoolExecutor(
                max_workers=_env_worker_count(
                    _IO_WORKERS_ENV_VAR,
                    _DEFAULT_IO_WORKERS,
                ),
                thread_name_prefix="predictcel-io",
            )
        return _io_executor


def shared_compute_executor() -> ThreadPoolExecutor:
    global _compute_executor
    with _executor_lock:
        if _compute_executor is None:
            _compute_executor = ThreadPoolExecutor(
                max_workers=_env_worker_count(
                    _COMPUTE_WORKERS_ENV_VAR,
                    _DEFAULT_COMPUTE_WORKERS,
                ),
                thread_name_prefix="predictcel-compute",
            )
        return _compute_executor


def shutdown_shared_executors(
    *,
    wait: bool = True,
    cancel_futures: bool = True,
) -> None:
    global _io_executor, _compute_executor
    with _executor_lock:
        io_executor = _io_executor
        compute_executor = _compute_executor
        _io_executor = None
        _compute_executor = None

    if io_executor is not None:
        io_executor.shutdown(wait=wait, cancel_futures=cancel_futures)
    if compute_executor is not None:
        compute_executor.shutdown(wait=wait, cancel_futures=cancel_futures)


atexit.register(shutdown_shared_executors)
