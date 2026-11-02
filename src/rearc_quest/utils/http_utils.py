"""HTTP access that identifies itself, retries politely, and explains a BLS block."""

from __future__ import annotations

import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from rearc_quest.config.dataset_config_definition import DatasetConfig
from rearc_quest.utils.secret_utils import get_secret

TIMEOUT_SECONDS = 60


class SourceAccessError(RuntimeError):
    pass


def build_user_agent(dataset: DatasetConfig) -> str:
    if dataset.credential is None:
        return dataset.user_agent_template
    return dataset.user_agent_template.format(contact=get_secret(dataset.credential))


class HttpClient:
    def __init__(self, user_agent: str, pause_seconds: float = 1.0):
        self.pause_seconds = pause_seconds
        self.session = requests.Session()
        self.session.headers["User-Agent"] = user_agent
        retry = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            respect_retry_after_header=True,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    def get(self, url: str) -> bytes:
        try:
            response = self.session.get(url, timeout=TIMEOUT_SECONDS)
        except requests.RequestException as exc:
            raise SourceAccessError(f"Could not reach {url}: {exc}") from exc
        if response.status_code == 403:
            raise SourceAccessError(
                f"403 from {url}. BLS requires contact details in the User-Agent; check the configured "
                "contact. On Free Edition, a 403 can also mean serverless egress blocked the domain: "
                "run ingestion locally with --use-files-api."
            )
        if not response.ok:
            raise SourceAccessError(f"HTTP {response.status_code} from {url}")
        time.sleep(self.pause_seconds)
        return response.content
