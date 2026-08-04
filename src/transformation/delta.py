"""Bronze-vs-staging delta computation: NEW / UPDATED / REACTIVATED / DELETED.

stg_ml_scan now holds only the ids drained from the webhook queue this run
(see src/extraction/changes.py), not a full snapshot of every active
listing. Because of that, "deleted" can no longer be inferred from a
listing's absence from stg_ml_scan (there is no full snapshot to compare
against) -- it's read directly from the status the multiget call returned.
This mirrors the old behavior exactly: previously stg_ml_scan only ever
contained status='active' listings (the scan API filtered for that
server-side), so "active" here plays the same role "present in stg" did.

s.status IS NULL covers a multiget failure for that id (see
src/load/stage.py) -- treated as "assume still active", the same benefit of
the doubt the old code gave those rows, so a transient API error doesn't get
misread as the item having disappeared.
"""

_NEW_SQL = """
    SELECT s.mlb_id FROM stg_ml_scan s
    LEFT JOIN bronze_anuncios_mercado_livre b ON b.mlb = s.mlb_id
    WHERE b.mlb IS NULL AND (s.status IS NULL OR s.status = 'active')
"""

_UPDATED_SQL = """
    SELECT s.mlb_id FROM stg_ml_scan s
    JOIN bronze_anuncios_mercado_livre b ON b.mlb = s.mlb_id
    WHERE b.coluna_deletada = false
      AND (s.status IS NULL OR s.status = 'active')
      AND (s.last_updated IS NULL OR s.last_updated > b.last_updated)
"""

_REACTIVATED_SQL = """
    SELECT s.mlb_id FROM stg_ml_scan s
    JOIN bronze_anuncios_mercado_livre b ON b.mlb = s.mlb_id
    WHERE b.coluna_deletada = true
      AND (s.status IS NULL OR s.status = 'active')
"""

_DELETED_SQL = """
    SELECT s.mlb_id FROM stg_ml_scan s
    JOIN bronze_anuncios_mercado_livre b ON b.mlb = s.mlb_id
    WHERE s.status IS NOT NULL AND s.status != 'active' AND b.coluna_deletada = false
"""


def _fetch_ids(conn, sql: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(sql)
        return [row[0] for row in cur.fetchall()]


def compute_delta(conn) -> dict:
    return {
        "new": _fetch_ids(conn, _NEW_SQL),
        "updated": _fetch_ids(conn, _UPDATED_SQL),
        "reactivated": _fetch_ids(conn, _REACTIVATED_SQL),
        "deleted": _fetch_ids(conn, _DELETED_SQL),
    }
