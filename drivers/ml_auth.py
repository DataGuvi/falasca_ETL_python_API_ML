"""OAuth token lifecycle. Access/refresh tokens are persisted in Postgres so
most runs (access_token typically valid ~6h) make zero auth network calls.

ML rotates the refresh_token on every refresh call, so the DB row -- not the
bootstrap env var -- is the source of truth after the first successful run.
Token refresh commits independently of the caller's transaction: if the rest
of the run later fails and rolls back, we must not lose the freshly rotated
refresh_token (the old one is now invalid on ML's side).
"""
import logging
from datetime import datetime, timedelta, timezone

from drivers.ml_client import BASE_URL, request_with_retry

logger = logging.getLogger(__name__)

EXPIRY_BUFFER = timedelta(minutes=5)


class AuthError(RuntimeError):
    pass


def _fetch_stored_token(conn, ml_user_id: str):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT access_token, refresh_token, expires_at "
            "FROM ml_oauth_tokens WHERE ml_user_id = %s",
            (ml_user_id,),
        )
        return cur.fetchone()


def _store_token(conn, ml_user_id: str, access_token: str, refresh_token: str,
                  expires_at: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ml_oauth_tokens (ml_user_id, access_token, refresh_token, expires_at, updated_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (ml_user_id) DO UPDATE SET
                access_token = EXCLUDED.access_token,
                refresh_token = EXCLUDED.refresh_token,
                expires_at = EXCLUDED.expires_at,
                updated_at = now()
            """,
            (ml_user_id, access_token, refresh_token, expires_at),
        )
    conn.commit()


def _refresh(client_id: str, client_secret: str, refresh_token: str,
             timeout: int, max_retries: int) -> dict:
    response = request_with_retry(
        "POST", f"{BASE_URL}/oauth/token",
        data={
            "grant_type": "refresh_token",
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
        },
        headers={"Accept": "application/json"},
        timeout=timeout, max_retries=max_retries,
    )
    if response.status_code // 100 != 2:
        raise AuthError(f"Token refresh failed ({response.status_code}): {response.text}")
    return response.json()


def get_valid_access_token(conn, config) -> str:
    stored = _fetch_stored_token(conn, config.ml_user_id)
    now = datetime.now(timezone.utc)

    if stored is not None:
        access_token, refresh_token, expires_at = stored
        if expires_at - EXPIRY_BUFFER > now:
            return access_token
        refresh_token_to_use = refresh_token
    else:
        refresh_token_to_use = config.ml_refresh_token

    logger.info("Refresh token usado para logar na API do Mercado Livre: %s", refresh_token_to_use)

    payload = _refresh(
        config.ml_client_id, config.ml_client_secret, refresh_token_to_use,
        config.http_timeout, config.max_retries,
    )
    new_expires_at = now + timedelta(seconds=int(payload["expires_in"]))
    _store_token(
        conn, config.ml_user_id,
        payload["access_token"], payload["refresh_token"], new_expires_at,
    )
    return payload["access_token"]
