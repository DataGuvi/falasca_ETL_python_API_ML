"""Bronze upsert, deleted-flagging, and bronze -> prata projection."""
from psycopg2.extras import execute_values

# The business fields a listing's manual spreadsheet used to track (SKU,
# frete, classe, comissao, taxa_fixa, tipo_anuncio) -- as opposed to control
# columns like last_updated/status_anuncio/coluna_deletada. Used to tell a
# "real" change apart from a bronze refresh triggered by a notification that
# didn't actually touch any of these (see fetch_bronze_snapshot and
# src/pipeline.py::_process_ids).
TRACKED_FIELDS = ("sku", "frete", "classe", "comissao", "taxa_fixa", "tipo_anuncio", "status_catalogo")

_BRONZE_UPSERT_SQL = """
    INSERT INTO bronze_anuncios_mercado_livre
        (mlb, sku, frete, classe, comissao, taxa_fixa, tipo_anuncio, status_catalogo, status_anuncio, last_updated, coluna_deletada, time_import, updated_at)
    VALUES %s
    ON CONFLICT (mlb) DO UPDATE SET
        sku = EXCLUDED.sku,
        frete = EXCLUDED.frete,
        classe = EXCLUDED.classe,
        comissao = EXCLUDED.comissao,
        taxa_fixa = EXCLUDED.taxa_fixa,
        tipo_anuncio = EXCLUDED.tipo_anuncio,
        status_catalogo = EXCLUDED.status_catalogo,
        status_anuncio = EXCLUDED.status_anuncio,
        last_updated = EXCLUDED.last_updated,
        coluna_deletada = false,
        time_import = EXCLUDED.time_import,
        updated_at = EXCLUDED.updated_at
"""
_BRONZE_UPSERT_TEMPLATE = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, false, now(), now())"


def upsert_bronze(conn, enriched_rows: list[dict]) -> None:
    if not enriched_rows:
        return
    rows = [
        (r["mlb"], r["sku"], r["frete"], r["classe"], r["comissao"],
         r["taxa_fixa"], r["tipo_anuncio"], r["status_catalogo"], r["status_anuncio"], r["last_updated"])
        for r in enriched_rows
    ]
    with conn.cursor() as cur:
        execute_values(cur, _BRONZE_UPSERT_SQL, rows, template=_BRONZE_UPSERT_TEMPLATE)


def fetch_bronze_snapshot(conn, mlb_ids: list[str]) -> dict[str, dict]:
    """Returns {mlb: {field: value}} with each id's *current* TRACKED_FIELDS
    values, read before upsert_bronze overwrites them. Ids not yet in bronze
    (brand-new listings) simply aren't in the result -- callers should treat
    a missing key as "there's nothing to compare against, so it counts as
    changed."""
    if not mlb_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT mlb, {', '.join(TRACKED_FIELDS)} "
            "FROM bronze_anuncios_mercado_livre WHERE mlb = ANY(%s)",
            (mlb_ids,),
        )
        return {row[0]: dict(zip(TRACKED_FIELDS, row[1:])) for row in cur.fetchall()}


def flag_deleted(conn, deleted_ids: list[str]) -> None:
    """Mark rows missing from the current scan as deleted, without touching
    time_import (no business data was re-imported for these).

    Also syncs status_anuncio to the real ML status (e.g. "paused",
    "closed") pulled from stg_ml_scan -- deleted ids never go through
    enrich_item/upsert_bronze (only NEW/UPDATED/REACTIVATED do), so without
    this bronze.status_anuncio would stay stuck at whatever it was the last
    time the listing was active."""
    if not deleted_ids:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE bronze_anuncios_mercado_livre b
            SET coluna_deletada = true, updated_at = now(), status_anuncio = s.status
            FROM stg_ml_scan s
            WHERE b.mlb = s.mlb_id AND b.mlb = ANY(%s)
            """,
            (deleted_ids,),
        )


def project_to_prata(conn, mlb_ids: list[str]) -> None:
    """Upsert the given mlb ids from bronze into prata, dropping control-only
    columns (last_updated, coluna_deletada) -- prata has no deleted-flag
    column at all, deleted listings are removed outright (see
    remove_from_prata). Callers must not pass deleted ids here."""
    if not mlb_ids:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO prata_anuncios_mercado_livre
                (mlb, sku, frete, classe, comissao, taxa_fixa, tipo_anuncio, status_catalogo, status_anuncio, time_import)
            SELECT mlb, sku, frete, classe, comissao, taxa_fixa, tipo_anuncio, status_catalogo, status_anuncio, time_import
            FROM bronze_anuncios_mercado_livre WHERE mlb = ANY(%s)
            ON CONFLICT (mlb) DO UPDATE SET
                sku = EXCLUDED.sku,
                frete = EXCLUDED.frete,
                classe = EXCLUDED.classe,
                comissao = EXCLUDED.comissao,
                taxa_fixa = EXCLUDED.taxa_fixa,
                tipo_anuncio = EXCLUDED.tipo_anuncio,
                status_catalogo = EXCLUDED.status_catalogo,
                status_anuncio = EXCLUDED.status_anuncio,
                time_import = EXCLUDED.time_import
            """,
            (mlb_ids,),
        )


def remove_from_prata(conn, deleted_ids: list[str]) -> None:
    """Deletes the given ids from prata outright -- prata is meant to be a
    current-state view (only what's sellable today), unlike bronze which
    keeps deleted rows (flagged via coluna_deletada) for history."""
    if not deleted_ids:
        return
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM prata_anuncios_mercado_livre WHERE mlb = ANY(%s)",
            (deleted_ids,),
        )
