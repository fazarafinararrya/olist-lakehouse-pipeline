"""Trigger an Airbyte sync from the pipeline and wait for it to finish.

Uses urllib from the standard library rather than an HTTP client package, so this adds no
dependency to an image that already carries Spark, Prefect and dbt.

Access tokens last 15 minutes while a full sync of this dataset takes longer than that, so the
token is refreshed while polling instead of being fetched once.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

log = logging.getLogger(__name__)

SUCCESS_STATUSES = {"succeeded"}
FAILURE_STATUSES = {"failed", "cancelled", "incomplete"}
TOKEN_REFRESH_AFTER_SECONDS = 600  # tokens are valid for 900


class AirbyteError(RuntimeError):
    """Anything that stops the sync from being started or from finishing successfully."""


@dataclass(frozen=True)
class AirbyteConfig:
    api_url: str
    client_id: str
    client_secret: str
    connection_id: str
    poll_seconds: int = 20
    timeout_seconds: int = 3600

    @classmethod
    def from_env(cls) -> AirbyteConfig:
        env = os.environ.get
        required = (
            "AIRBYTE_API_URL",
            "AIRBYTE_CLIENT_ID",
            "AIRBYTE_CLIENT_SECRET",
            "AIRBYTE_CONNECTION_ID",
        )
        missing = [name for name in required if not env(name)]
        if missing:
            raise AirbyteError(
                "cannot trigger an Airbyte sync, missing environment variables: "
                + ", ".join(missing)
                + ". Set them in .env, or run the flow with --skip-sync."
            )
        return cls(
            api_url=env("AIRBYTE_API_URL", "").rstrip("/"),
            client_id=env("AIRBYTE_CLIENT_ID", ""),
            client_secret=env("AIRBYTE_CLIENT_SECRET", ""),
            connection_id=env("AIRBYTE_CONNECTION_ID", ""),
            poll_seconds=int(env("AIRBYTE_POLL_SECONDS", "20")),
            timeout_seconds=int(env("AIRBYTE_TIMEOUT_SECONDS", "3600")),
        )


def _request(
    url: str,
    *,
    method: str = "GET",
    payload: dict | None = None,
    token: str | None = None,
    timeout: int = 60,
) -> dict:
    body = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(url, data=body, method=method)
    request.add_header("Accept", "application/json")
    if body is not None:
        request.add_header("Content-Type", "application/json")
    if token:
        request.add_header("Authorization", f"Bearer {token}")

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode()
    except urllib.error.HTTPError as exc:
        # Airbyte returns a JSON body explaining the refusal; surface it instead of just the code.
        detail = exc.read().decode(errors="replace")[:500]
        raise AirbyteError(f"{method} {url} -> HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise AirbyteError(
            f"{method} {url} is unreachable ({exc.reason}). Is Airbyte running, and is "
            "AIRBYTE_API_URL reachable from inside the container?"
        ) from exc

    return json.loads(raw) if raw.strip() else {}


def fetch_token(config: AirbyteConfig) -> str:
    """Exchange the application's client credentials for a short-lived access token."""
    response = _request(
        f"{config.api_url}/api/public/v1/applications/token",
        method="POST",
        payload={
            "client_id": config.client_id,
            "client_secret": config.client_secret,
            # Airbyte's documented field name uses a hyphen, unlike the OAuth convention.
            "grant-type": "client_credentials",
        },
    )
    token = response.get("access_token")
    if not token:
        raise AirbyteError(f"no access_token in the token response; got keys {sorted(response)}")
    return str(token)


def trigger_sync(config: AirbyteConfig, token: str) -> str:
    response = _request(
        f"{config.api_url}/api/public/v1/jobs",
        method="POST",
        payload={"connectionId": config.connection_id, "jobType": "sync"},
        token=token,
    )
    job_id = response.get("jobId", response.get("id"))
    if job_id is None:
        raise AirbyteError(f"no jobId in the sync response; got {response}")
    return str(job_id)


def job_status(config: AirbyteConfig, token: str, job_id: str) -> dict:
    return _request(f"{config.api_url}/api/public/v1/jobs/{job_id}", token=token)


def run_sync(config: AirbyteConfig | None = None) -> dict:
    """Start a sync and block until it finishes. Raises AirbyteError if it does not succeed."""
    config = config or AirbyteConfig.from_env()

    token = fetch_token(config)
    token_age_from = time.monotonic()

    job_id = trigger_sync(config, token)
    log.info("airbyte: job %s started for connection %s", job_id, config.connection_id)

    started = time.monotonic()
    last_status: str | None = None

    while True:
        elapsed = int(time.monotonic() - started)
        if elapsed > config.timeout_seconds:
            raise AirbyteError(
                f"airbyte job {job_id} was still '{last_status}' after {elapsed}s; giving up. "
                "The job itself keeps running; check the Airbyte UI."
            )

        time.sleep(config.poll_seconds)

        if time.monotonic() - token_age_from > TOKEN_REFRESH_AFTER_SECONDS:
            token = fetch_token(config)
            token_age_from = time.monotonic()

        job = job_status(config, token, job_id)
        status = str(job.get("status", "")).lower()

        if status != last_status:
            log.info("airbyte: job %s is '%s' after %ss", job_id, status, elapsed)
            last_status = status

        if status in SUCCESS_STATUSES:
            return {
                "job_id": job_id,
                "status": status,
                "rows_synced": job.get("rowsSynced"),
                "seconds": elapsed,
            }
        if status in FAILURE_STATUSES:
            raise AirbyteError(f"airbyte job {job_id} ended as '{status}' after {elapsed}s")