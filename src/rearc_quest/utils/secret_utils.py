"""Resolve secrets from the environment locally, or a Databricks secret scope in the workspace."""

from __future__ import annotations

import base64
import logging
import os

from databricks.sdk import WorkspaceClient

from rearc_quest.config.dataset_config_definition import SecretRef

# Only the secret's location is ever logged, never its value.
log = logging.getLogger(__name__)


class MissingSecretError(RuntimeError):
    pass


def get_secret(ref: SecretRef) -> str:
    from_env = os.environ.get(ref.env_var)
    if from_env:
        log.info("Secret %s/%s resolved from environment variable %s", ref.scope, ref.key, ref.env_var)
        return from_env
    try:
        response = WorkspaceClient().secrets.get_secret(scope=ref.scope, key=ref.key)
        log.info("Secret %s/%s resolved from Databricks secret scope", ref.scope, ref.key)
        return base64.b64decode(response.value).decode("utf-8")
    except Exception as exc:
        log.error(
            "Secret %s/%s not found in %s or Databricks (%s)", ref.scope, ref.key, ref.env_var, type(exc).__name__
        )
        raise MissingSecretError(f"Set {ref.env_var}, or create secret {ref.scope}/{ref.key} in Databricks.") from exc
