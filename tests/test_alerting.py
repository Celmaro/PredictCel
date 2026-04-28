from predictcel.alerting import AlertManager


def test_send_posts_discord_alert(monkeypatch) -> None:
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.example/webhook")
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("SLACK_WEBHOOK", raising=False)
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)

    manager = AlertManager()
    observed_calls: list[tuple[str, dict[str, object]]] = []

    def fake_send_webhook(url: str, payload: dict[str, object]) -> bool:
        observed_calls.append((url, payload))
        return True

    monkeypatch.setattr(manager, "_send_webhook", fake_send_webhook)

    sent = manager.send(
        "warning",
        "Cycle Warning",
        "Something drifted",
        cycle_id="cycle-123",
        metadata={"stage": "evaluate"},
    )

    assert sent is True
    assert len(observed_calls) == 1

    url, payload = observed_calls[0]

    assert url == "https://discord.example/webhook"
    assert payload["content"] == "**Cycle Warning**\nSomething drifted"

    embeds = payload["embeds"]
    assert isinstance(embeds, list)
    assert embeds[0]["title"] == "Cycle Warning"
    assert embeds[0]["description"] == "Something drifted"
    assert embeds[0]["fields"] == [
        {"name": "Severity", "value": "WARNING", "inline": True},
        {"name": "Cycle ID", "value": "cycle-123", "inline": True},
        {"name": "stage", "value": "evaluate", "inline": True},
    ]
