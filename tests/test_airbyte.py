import pytest

from olist_pipeline import airbyte


def test_config_lists_every_missing_variable(monkeypatch):
    for name in ("AIRBYTE_API_URL", "AIRBYTE_CLIENT_ID", "AIRBYTE_CLIENT_SECRET", "AIRBYTE_CONNECTION_ID"):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(airbyte.AirbyteError) as excinfo:
        airbyte.AirbyteConfig.from_env()

    message = str(excinfo.value)
    # The operator should learn everything that is missing from one message, not one per run.
    assert "AIRBYTE_API_URL" in message
    assert "AIRBYTE_CONNECTION_ID" in message
    assert "--skip-sync" in message


def test_config_reads_env_and_trims_trailing_slash(monkeypatch):
    monkeypatch.setenv("AIRBYTE_API_URL", "http://host.docker.internal:8000/")
    monkeypatch.setenv("AIRBYTE_CLIENT_ID", "id")
    monkeypatch.setenv("AIRBYTE_CLIENT_SECRET", "secret")
    monkeypatch.setenv("AIRBYTE_CONNECTION_ID", "conn")

    config = airbyte.AirbyteConfig.from_env()

    assert config.api_url == "http://host.docker.internal:8000"  # no double slash when joined
    assert config.poll_seconds == 20


def test_terminal_statuses_do_not_overlap():
    """A status must never count as both finished and failed."""
    assert not airbyte.SUCCESS_STATUSES & airbyte.FAILURE_STATUSES
    assert "running" not in airbyte.SUCCESS_STATUSES | airbyte.FAILURE_STATUSES