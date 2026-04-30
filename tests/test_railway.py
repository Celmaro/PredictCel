import pytest

from predictcel import railway


def test_default_db_path_prefers_temp_without_env(monkeypatch) -> None:
    monkeypatch.delenv("PREDICTCEL_DB", raising=False)
    monkeypatch.delenv("RAILWAY_VOLUME_MOUNT_PATH", raising=False)

    assert railway._default_db_path() == "/tmp/predictcel.db"


def test_default_db_path_prefers_volume_mount(monkeypatch) -> None:
    monkeypatch.delenv("PREDICTCEL_DB", raising=False)
    monkeypatch.setenv("RAILWAY_VOLUME_MOUNT_PATH", "/data")

    assert railway._default_db_path() == "/data/predictcel.db"


def test_default_db_path_prefers_explicit_env(monkeypatch) -> None:
    monkeypatch.setenv("PREDICTCEL_DB", "/custom/predictcel.db")
    monkeypatch.setenv("RAILWAY_VOLUME_MOUNT_PATH", "/data")

    assert railway._default_db_path() == "/custom/predictcel.db"


def test_run_once_from_env_requires_persistent_db_when_enabled(monkeypatch) -> None:
    monkeypatch.delenv("PREDICTCEL_DB", raising=False)
    monkeypatch.delenv("RAILWAY_VOLUME_MOUNT_PATH", raising=False)
    monkeypatch.setenv("PREDICTCEL_REQUIRE_PERSISTENT_DB", "1")

    with pytest.raises(RuntimeError, match="Persistent DB path is required"):
        railway._run_once_from_env()


def test_sleep_until_next_cycle_exits_when_shutdown_requested(monkeypatch) -> None:
    sleep_calls = []
    railway._shutdown_requested.clear()

    def fake_sleep(seconds: int) -> None:
        sleep_calls.append(seconds)
        railway._shutdown_requested.set()

    monkeypatch.setattr("predictcel.railway.time.sleep", fake_sleep)

    railway._sleep_until_next_cycle(5)

    assert sleep_calls == [1]
    railway._shutdown_requested.clear()
