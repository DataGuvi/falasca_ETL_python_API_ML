"""Postgres access layer: connection, advisory lock.

This module never issues DDL (CREATE SCHEMA/TABLE) at runtime. The schema in
schema.sql must be applied manually, once, by someone with the privileges to
do so -- this worker only ever expects the tables to already exist.
"""
import re
from contextlib import contextmanager

import psycopg2

# Arbitrary fixed key identifying this worker's exclusivity lock. Any distinct
# bigint works; it just must not collide with other advisory-lock users on the
# same database.
_RUN_LOCK_KEY = 847_552_011

_VALID_SCHEMA_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class AlreadyRunningError(RuntimeError):
    """Raised when another run holds the advisory lock."""


def connect(database_url: str, schema: str = "public"):
    if not _VALID_SCHEMA_NAME.match(schema):
        raise ValueError(f"Invalid POSTGRES_SCHEMA: {schema!r}")
    # search_path scopes every unqualified table reference in this codebase
    # to the configured schema.
    return psycopg2.connect(database_url, options=f"-c search_path={schema},public")


@contextmanager
def run_lock(conn):
    """Postgres transaction-scoped advisory lock so overlapping runs abort
    instead of racing each other. Auto-released on commit/rollback."""
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_xact_lock(%s)", (_RUN_LOCK_KEY,))
        acquired = cur.fetchone()[0]
    if not acquired:
        raise AlreadyRunningError("Another sync run is already in progress")
    yield
