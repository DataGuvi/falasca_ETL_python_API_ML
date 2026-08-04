"""One-off backfill: pushes every row currently in prata_anuncios_mercado_livre
into the Google Sheets output in a single full insertion. Use this to seed
the sheet when it doesn't yet mirror what's already in prata (e.g. right
after wiring up the Google Sheets output, see drivers/google_sheets_output.py)
-- day-to-day the pipeline keeps the sheet in sync incrementally on its own
via src/pipeline.py::_process_ids.

Not meant to be scheduled -- run once, on demand:

    python main_backfill_google_sheets.py
"""
import logging
from decimal import Decimal

from config.settings import load_config
from drivers import db
from drivers.google_sheets_output import build_google_sheets_client
from src.load.warehouse import TRACKED_FIELDS
from utils.dotenv import load_dotenv
from utils.logging_setup import configure_logging

logger = logging.getLogger(__name__)

_CHUNK_SIZE = 500


def _to_json_safe(value):
    # psycopg2 returns numeric(...) columns as Decimal, which isn't JSON
    # serializable (unlike the plain floats enrich_item pulls straight out
    # of the ML API responses in the regular incremental/full-scan flow).
    return float(value) if isinstance(value, Decimal) else value


def _fetch_prata_rows(conn) -> list[dict]:
    columns = ("mlb",) + TRACKED_FIELDS
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {', '.join(columns)} FROM prata_anuncios_mercado_livre ORDER BY mlb"
        )
        return [
            {col: _to_json_safe(value) for col, value in zip(columns, row)}
            for row in cur.fetchall()
        ]


def _chunks(rows: list[dict], size: int):
    for i in range(0, len(rows), size):
        yield rows[i:i + size]


def run() -> None:
    configure_logging()
    load_dotenv()
    config = load_config()
    conn = db.connect(config.database_url, config.postgres_schema)

    try:
        rows = _fetch_prata_rows(conn)
        logger.info("Backfill: %d linha(s) lida(s) de prata_anuncios_mercado_livre", len(rows))
        if not rows:
            logger.warning("prata_anuncios_mercado_livre está vazia -- nada a gravar no Google Sheets")
            return

        sheets = build_google_sheets_client(config)
        written = 0
        for chunk in _chunks(rows, _CHUNK_SIZE):
            sheets.upsert_rows(chunk)
            written += len(chunk)
            logger.info("Backfill: %d/%d linha(s) gravada(s) no Google Sheets", written, len(rows))

        logger.info("Backfill concluído: %d linha(s) gravada(s) no Google Sheets", written)
    finally:
        conn.close()


if __name__ == "__main__":
    run()
