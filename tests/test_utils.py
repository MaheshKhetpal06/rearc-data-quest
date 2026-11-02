import pytest
import requests

from rearc_quest.config.dataset_config_definition import BLS_CONTACT, get_dataset
from rearc_quest.utils.http_utils import HttpClient, SourceAccessError, build_user_agent
from rearc_quest.utils.secret_utils import get_secret


class FakeResponse:
    def __init__(self, status_code, content=b""):
        self.status_code = status_code
        self.content = content
        self.ok = status_code < 400


def client_returning(monkeypatch, response):
    client = HttpClient("agent", pause_seconds=0)
    monkeypatch.setattr(client.session, "get", lambda url, timeout: response)
    return client


def test_get_secret_prefers_environment(monkeypatch):
    monkeypatch.setenv(BLS_CONTACT.env_var, "person@example.com")
    assert get_secret(BLS_CONTACT) == "person@example.com"


def test_user_agent_includes_contact_for_bls(monkeypatch):
    monkeypatch.setenv(BLS_CONTACT.env_var, "person@example.com")
    assert build_user_agent(get_dataset("bls_data")) == "rearc-data-quest/1.0 (contact: person@example.com)"


def test_user_agent_has_no_contact_for_population(monkeypatch):
    monkeypatch.setenv(BLS_CONTACT.env_var, "person@example.com")
    assert "person@example.com" not in build_user_agent(get_dataset("population"))


def test_client_sends_user_agent_and_returns_body(monkeypatch):
    client = client_returning(monkeypatch, FakeResponse(200, b"body"))
    assert client.session.headers["User-Agent"] == "agent"
    assert client.get("https://example.com") == b"body"


def test_client_explains_403(monkeypatch):
    client = client_returning(monkeypatch, FakeResponse(403))
    with pytest.raises(SourceAccessError, match="User-Agent"):
        client.get("https://example.com")


def test_client_reports_other_http_errors(monkeypatch):
    client = client_returning(monkeypatch, FakeResponse(404))
    with pytest.raises(SourceAccessError, match="404"):
        client.get("https://example.com")


def test_client_wraps_connection_errors(monkeypatch):
    client = HttpClient("agent", pause_seconds=0)

    def fail(url, timeout):
        raise requests.ConnectionError("down")

    monkeypatch.setattr(client.session, "get", fail)
    with pytest.raises(SourceAccessError, match="Could not reach"):
        client.get("https://example.com")


def test_client_retries_rate_limits_and_server_errors():
    retry = HttpClient("agent").session.get_adapter("https://example.com").max_retries
    assert {429, 503} <= set(retry.status_forcelist)
    assert retry.total == 5
