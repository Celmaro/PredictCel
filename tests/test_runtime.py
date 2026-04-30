from predictcel import runtime


def test_shutdown_shared_executors_uses_explicit_policy(monkeypatch) -> None:
    calls = []

    class FakeExecutor:
        def __init__(self, name: str) -> None:
            self.name = name

        def shutdown(self, *, wait: bool, cancel_futures: bool) -> None:
            calls.append((self.name, wait, cancel_futures))

    monkeypatch.setattr(runtime, "_io_executor", FakeExecutor("io"))
    monkeypatch.setattr(runtime, "_compute_executor", FakeExecutor("compute"))

    runtime.shutdown_shared_executors(wait=True, cancel_futures=True)

    assert calls == [("io", True, True), ("compute", True, True)]
    assert runtime._io_executor is None
    assert runtime._compute_executor is None


def test_shared_executors_honor_env_worker_overrides(monkeypatch) -> None:
    runtime.shutdown_shared_executors(wait=True, cancel_futures=True)
    monkeypatch.setenv("PREDICTCEL_IO_WORKERS", "11")
    monkeypatch.setenv("PREDICTCEL_COMPUTE_WORKERS", "13")

    io_executor = runtime.shared_io_executor()
    compute_executor = runtime.shared_compute_executor()

    try:
        assert io_executor._max_workers == 11
        assert compute_executor._max_workers == 13
    finally:
        runtime.shutdown_shared_executors(wait=True, cancel_futures=True)
