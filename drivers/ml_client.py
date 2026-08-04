"""Thin Mercado Livre HTTP client: bearer auth + 429/5xx backoff.

Reused for both the OAuth token endpoint (no bearer token yet) and all
authenticated item/scan/pricing calls.
"""
import logging
import random
import time

import requests

BASE_URL = "https://api.mercadolibre.com"

logger = logging.getLogger(__name__)


class MLApiError(RuntimeError):
    def __init__(self, status_code: int, body):
        self.status_code = status_code
        self.body = body
        super().__init__(f"ML API error {status_code}: {body}")


def _sleep_for_retry(attempt: int, response: requests.Response, url: str, max_retries: int) -> None:
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            delay = float(retry_after)
        except ValueError:
            delay = 2.0 ** attempt
    else:
        delay = 2.0 ** attempt
    delay += random.uniform(0, 0.5)  # jitter to avoid thundering herd
    delay = min(delay, 60.0)
    logger.warning(
        "HTTP %d em %s -- retry %d/%d em %.1fs%s",
        response.status_code, url, attempt + 1, max_retries, delay,
        " (respeitando Retry-After)" if retry_after else "",
    )
    time.sleep(delay)


def request_with_retry(method: str, url: str, *, params=None, data=None,
                        headers=None, timeout=30, max_retries=5) -> requests.Response:
    last_response = None
    for attempt in range(max_retries + 1):
        response = requests.request(
            method, url, params=params, data=data, headers=headers, timeout=timeout,
        )
        if response.status_code == 429 or response.status_code >= 500:
            last_response = response
            if attempt < max_retries:
                _sleep_for_retry(attempt, response, url, max_retries)
                continue
            logger.error(
                "Esgotadas as tentativas (%d) para %s -- último status %d",
                max_retries, url, response.status_code,
            )
            return response
        return response
    return last_response


class MLClient:
    def __init__(self, access_token: str, timeout: int = 30, max_retries: int = 5):
        self.access_token = access_token
        self.timeout = timeout
        self.max_retries = max_retries

    def get(self, path: str, params: dict | None = None) -> dict:
        url = f"{BASE_URL}{path}"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = request_with_retry(
            "GET", url, params=params, headers=headers,
            timeout=self.timeout, max_retries=self.max_retries,
        )
        if response.status_code // 100 != 2:
            try:
                body = response.json()
            except ValueError:
                body = response.text
            raise MLApiError(response.status_code, body)
        return response.json()
