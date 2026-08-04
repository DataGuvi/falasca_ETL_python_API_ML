"""Drains the ml_item_changes queue written by the webhook API (API_FALASCA).

Replaces the old full active-listing scan: instead of enumerating every
active item on every run, this only touches the ids the webhook has told us
changed since the last drain.

The DELETE ... RETURNING runs inside the same run_lock transaction as the
rest of the run (see src/pipeline.py). If the run fails and rolls back, the
drained ids go right back into the queue for the next attempt -- nothing is
lost on a failed run.
"""
import logging

from utils.collections import dedupe

logger = logging.getLogger(__name__)


def drain_item_changes(conn) -> list[str]:
    """Deletes and returns every pending mlb_id, deduplicated.

    A single mlb_id can appear more than once (repeated notifications for the
    same listing); duplicates are collapsed here so callers never see the
    same id twice in one run.
    """
    with conn.cursor() as cur:
        cur.execute("DELETE FROM ml_item_changes RETURNING mlb_id")
        rows = [row[0] for row in cur.fetchall()]

    ids = dedupe(rows)
    logger.info(
        "Fila de alterações drenada: %d notificação(ões), %d id(s) único(s)",
        len(rows), len(ids),
    )
    return ids
