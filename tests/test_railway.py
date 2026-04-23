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
