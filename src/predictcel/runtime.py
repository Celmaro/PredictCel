from __future__ import annotations

import atexit
import threading
from concurrent.futures import ThreadPoolExecutor

_executor_lock = threading.Lock()
_io_executor: ThreadPoolExecutor | None = None
_compute_executor: ThreadPoolExecutor | None = None


def shared_io_executor() -> ThreadPoolExecutor:
    global _io_executor
    with _executor_lock:
        if _io_executor is None:
            _io_executor = ThreadPoolExecutor(
                max_workers=16,
                thread_name_prefix="predictcel-io",
            )
        return _io_executor


def shared_compute_executor() -> ThreadPoolExecutor:
    global _compute_executor
    with _executor_lock:
        if _compute_executor is None:
            _compute_executor = ThreadPoolExecutor(
                max_workers=8,
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
