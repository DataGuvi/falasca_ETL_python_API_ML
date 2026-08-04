"""Loads the drained item-change ids + control info into the transient
staging table."""
from psycopg2.extras import execute_values


def load_stg(conn, changed_ids: list[str], info_by_mlb: dict) -> None:
    """Clear + load stg_ml_scan with this run's batch. Ids with no control
    info (multiget failure) are still loaded with NULL last_updated/status
    so compute_delta gives them the benefit of the doubt rather than reading
    a transient API error as the item having disappeared (see
    src/transformation/delta.py).

    Uses DELETE rather than TRUNCATE: TRUNCATE requires a separate privilege
    (usually only granted to the table owner) that this worker's DB user may
    not have, while DELETE only needs the ordinary DELETE grant."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM stg_ml_scan")
        if not changed_ids:
            return
        rows = [
            (mlb_id, info_by_mlb.get(mlb_id, {}).get("last_updated"),
             info_by_mlb.get(mlb_id, {}).get("status"))
            for mlb_id in changed_ids
        ]
        execute_values(
            cur,
            "INSERT INTO stg_ml_scan (mlb_id, last_updated, status) VALUES %s",
            rows,
        )
