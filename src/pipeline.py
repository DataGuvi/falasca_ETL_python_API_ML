"""Run orchestration: wires auth, the webhook's item-change queue, control,
delta, enrich, and the bronze/prata upserts together into a single
incremental sync pass.

Two entrypoints share the same processing core (_process_ids): run(), driven
by the webhook queue for day-to-day incremental syncs, and run_full_scan(),
a one-off bootstrap that walks the seller's entire catalog instead (see
src/extraction/full_scan.py). Only *where the id list comes from* differs
between the two -- everything from control-info lookup onward is identical.
"""
import logging
from datetime import datetime, timezone

from config.settings import load_config
from drivers import db
from drivers.email_alert import send_failure_alert
from drivers.google_sheets_output import build_google_sheets_client
from drivers.execution_log import finish_execution_log, start_execution_log
from drivers.ml_auth import get_valid_access_token
from drivers.ml_client import MLClient
from src.extraction.changes import drain_item_changes
from src.extraction.control import fetch_control_info, fetch_items_full
from src.extraction.enrich import EnrichmentError, enrich_item
from src.extraction.full_scan import fetch_all_seller_items
from src.load.stage import load_stg
from src.load.warehouse import (
    TRACKED_FIELDS,
    fetch_bronze_snapshot,
    flag_deleted,
    project_to_prata,
    remove_from_prata,
    upsert_bronze,
)
from src.transformation.delta import compute_delta
from utils.collections import dedupe
from utils.dotenv import load_dotenv
from utils.logging_setup import configure_logging

logger = logging.getLogger(__name__)


def _start_run_log(conn, started_at: datetime) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO etl_run_log (started_at, status) VALUES (%s, 'running') RETURNING id",
            (started_at,),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def _finish_run_log(conn, run_id: int, *, status: str, changes: int, new: int,
                     updated: int, reactivated: int, deleted: int, errors: int,
                     error_detail: str | None = None) -> None:
    # scanned_count predates the webhook queue; it now holds the number of
    # distinct ids drained from ml_item_changes this run (see
    # src/extraction/changes.py), not a count of scanned active listings.
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE etl_run_log SET
                finished_at = now(), status = %s,
                scanned_count = %s, new_count = %s, updated_count = %s,
                reactivated_count = %s, deleted_count = %s, error_count = %s,
                error_detail = %s
            WHERE id = %s
            """,
            (status, changes, new, updated, reactivated, deleted, errors, error_detail, run_id),
        )
    conn.commit()


def _new_metrics() -> dict:
    return {"changes": 0, "new": 0, "updated": 0, "reactivated": 0, "deleted": 0, "errors": 0}


def _format_diff(diffs: dict) -> str:
    """diffs: {field: (old, new)} -> 'campo: old -> new, campo2: old2 -> new2'."""
    return ", ".join(f"{field}: {old!r} -> {new!r}" for field, (old, new) in diffs.items())


def _alert_run_failed(exc: Exception) -> None:
    send_failure_alert(exc)


def _process_ids(conn, client, config, changed_ids: list[str], metrics: dict) -> None:
    """Control-info -> stg -> delta -> enrich -> bronze/prata for a batch of
    ids the caller has already resolved (drain or full scan). Mutates
    `metrics` in place; `metrics["changes"]` is set by the caller before this
    runs, since that count means something different for each entrypoint
    (queue size drained vs. catalog size scanned)."""
    info_by_mlb, multiget_failures = fetch_control_info(client, config, changed_ids)
    metrics["errors"] += len(multiget_failures)

    # --- load (staging) ---
    load_stg(conn, changed_ids, info_by_mlb)

    # --- transformation ---
    delta = compute_delta(conn)
    metrics["new"] = len(delta["new"])
    metrics["updated"] = len(delta["updated"])
    metrics["reactivated"] = len(delta["reactivated"])
    metrics["deleted"] = len(delta["deleted"])
    logger.info(
        "Delta: new=%d updated=%d reactivated=%d deleted=%d",
        metrics["new"], metrics["updated"], metrics["reactivated"], metrics["deleted"],
    )

    # --- extraction (enrichment) ---
    to_enrich = dedupe(delta["new"] + delta["updated"] + delta["reactivated"])
    reactivated = set(delta["reactivated"])

    # Batched item lookup (up to 20/call) instead of one GET /items/{id} per
    # listing -- items missing here (batch failure) fall back to a per-item
    # lookup inside enrich_item, so this isn't counted as an error on its own.
    items_by_mlb, _item_batch_failures = fetch_items_full(client, config, to_enrich)

    enriched_rows = []
    for mlb_id in to_enrich:
        try:
            enriched_rows.append(
                enrich_item(client, config, mlb_id, item=items_by_mlb.get(mlb_id))
            )
        except EnrichmentError as exc:
            logger.error(str(exc))
            metrics["errors"] += 1

    # Snapshot taken *before* the bronze upsert below overwrites it -- this
    # is what "did the tracked fields actually change" gets compared against.
    current_by_mlb = fetch_bronze_snapshot(conn, [row["mlb"] for row in enriched_rows])

    # --- load (bronze) ---
    # Bronze is written for every enriched row unconditionally, even when
    # nothing in TRACKED_FIELDS actually changed: its last_updated has to
    # advance to the fresh value the API returned, or compute_delta would
    # keep seeing this mlb_id as "outdated" forever, on every future
    # notification, including ones unrelated to the tracked fields (e.g. a
    # stock update) -- turning this optimization into a permanent full
    # re-fetch instead of the one-off it's meant to be.
    upsert_bronze(conn, enriched_rows)
    flag_deleted(conn, delta["deleted"])

    # --- load (prata) ---
    # Prata, unlike bronze, only needs a write when something a downstream
    # consumer actually cares about changed: a brand-new listing, a
    # reactivation, or a real change in one of TRACKED_FIELDS. A notification
    # that bumped last_updated without touching any of those is a no-op for
    # prata.
    changed_mlbs = []
    for row in enriched_rows:
        mlb_id = row["mlb"]
        current = current_by_mlb.get(mlb_id)
        is_new = current is None
        is_reactivated = mlb_id in reactivated
        diffs = {} if is_new else {
            field: (current[field], row[field])
            for field in TRACKED_FIELDS
            if current[field] != row[field]
        }

        if is_new:
            changed_mlbs.append(mlb_id)
            logger.info("%s: anúncio novo -- gravado em bronze e prata", mlb_id)
        elif diffs:
            changed_mlbs.append(mlb_id)
            logger.info(
                "%s: %sgravado em bronze e prata -- %s",
                mlb_id, "reativado, " if is_reactivated else "",
                _format_diff(diffs),
            )
        elif is_reactivated:
            changed_mlbs.append(mlb_id)
            logger.info(
                "%s: reativado sem mudança nos campos rastreados -- gravado em bronze e prata "
                "mesmo assim (precisa voltar a existir lá)",
                mlb_id,
            )
        else:
            logger.info(
                "%s: sem mudança em %s -- bronze atualizada (last_updated), prata não",
                mlb_id, "/".join(TRACKED_FIELDS),
            )

    project_to_prata(conn, changed_mlbs)
    # Prata is a current-state view: deleted listings are removed outright
    # (not flagged), unlike bronze which keeps them for history.
    remove_from_prata(conn, delta["deleted"])

    # --- load (saída de negócio no Google Sheets) ---
    # Mesma lista que foi pra prata: só linhas com mudança real (ou
    # novas/reativadas) são upsertadas; deletados têm a linha removida.
    sheets = build_google_sheets_client(config)
    changed_set = set(changed_mlbs)
    sheets.upsert_rows([row for row in enriched_rows if row["mlb"] in changed_set])
    sheets.remove_rows(delta["deleted"])


def run() -> None:
    configure_logging()
    load_dotenv()
    config = load_config()
    conn = db.connect(config.database_url, config.postgres_schema)

    try:
        # Schema/tables (see drivers/schema.sql) must already exist in the DB --
        # this worker never issues CREATE SCHEMA/CREATE TABLE at runtime.
        logger.info("Início da execução da pipeline (incremental)")
        access_token = get_valid_access_token(conn, config)
        logger.info(
            "Access token do Mercado Livre pronto (user_id=%s): %s",
            config.ml_user_id, access_token,
        )
        client = MLClient(access_token, timeout=config.http_timeout, max_retries=config.max_retries)

        started_at = datetime.now(timezone.utc)
        run_id = _start_run_log(conn, started_at)
        execution_log_id = start_execution_log(conn, "incremental", started_at)
        metrics = _new_metrics()

        try:
            with db.run_lock(conn):
                # --- extraction: drain the webhook queue instead of a full scan ---
                changed_ids = drain_item_changes(conn)
                metrics["changes"] = len(changed_ids)

                if not changed_ids:
                    logger.info("Nenhuma notificação pendente; nada a processar neste run")
                else:
                    _process_ids(conn, client, config, changed_ids, metrics)

            conn.commit()
            _finish_run_log(conn, run_id, status="success", **metrics)
            finish_execution_log(conn, execution_log_id, status="success")
            duration = (datetime.now(timezone.utc) - started_at).total_seconds()
            logger.info(
                "Run %s complete in %.1fs: changes=%d new=%d updated=%d reactivated=%d deleted=%d errors=%d",
                run_id, duration, metrics["changes"], metrics["new"], metrics["updated"],
                metrics["reactivated"], metrics["deleted"], metrics["errors"],
            )
        except db.AlreadyRunningError:
            conn.rollback()
            _finish_run_log(conn, run_id, status="skipped",
                             error_detail="another run already in progress", **metrics)
            finish_execution_log(
                conn, execution_log_id, status="skipped",
                error_reason="another run already in progress",
            )
            logger.warning("Skipped: another sync run is already in progress")
        except Exception as exc:
            conn.rollback()
            _finish_run_log(conn, run_id, status="failed", error_detail=str(exc), **metrics)
            finish_execution_log(conn, execution_log_id, status="failed", error_reason=str(exc))
            _alert_run_failed(exc)
            logger.exception("Run failed, all changes rolled back")
            raise
    finally:
        conn.close()


def run_full_scan() -> None:
    """One-off bootstrap: walks the seller's entire active catalog instead of
    draining the webhook queue, so every existing listing (not just the ones
    that happen to get notified after this runs) gets a first pass through
    control/delta/enrich/bronze/prata.

    Not meant to be scheduled -- run this once (see main_full_scan.py), then
    let main.py's incremental drain take over for day-to-day updates.
    """
    configure_logging()
    load_dotenv()
    config = load_config()
    conn = db.connect(config.database_url, config.postgres_schema)

    try:
        logger.info("Início da execução da pipeline (scan completo / bootstrap)")
        access_token = get_valid_access_token(conn, config)
        logger.info(
            "Access token do Mercado Livre pronto (user_id=%s): %s",
            config.ml_user_id, access_token,
        )
        client = MLClient(access_token, timeout=config.http_timeout, max_retries=config.max_retries)

        started_at = datetime.now(timezone.utc)
        run_id = _start_run_log(conn, started_at)
        execution_log_id = start_execution_log(conn, "full_scan", started_at)
        metrics = _new_metrics()

        try:
            with db.run_lock(conn):
                # --- extraction: full catalog instead of the webhook queue ---
                changed_ids = dedupe(fetch_all_seller_items(client, config))
                metrics["changes"] = len(changed_ids)

                if not changed_ids:
                    logger.warning(
                        "Scan completo não retornou nenhum anúncio ativo para o vendedor %s",
                        config.ml_user_id,
                    )
                else:
                    _process_ids(conn, client, config, changed_ids, metrics)

            conn.commit()
            _finish_run_log(conn, run_id, status="success", **metrics)
            finish_execution_log(conn, execution_log_id, status="success")
            duration = (datetime.now(timezone.utc) - started_at).total_seconds()
            logger.info(
                "Scan completo %s finalizado em %.1fs: total=%d new=%d updated=%d reactivated=%d "
                "deleted=%d errors=%d",
                run_id, duration, metrics["changes"], metrics["new"], metrics["updated"],
                metrics["reactivated"], metrics["deleted"], metrics["errors"],
            )
        except db.AlreadyRunningError:
            conn.rollback()
            _finish_run_log(conn, run_id, status="skipped",
                             error_detail="another run already in progress", **metrics)
            finish_execution_log(
                conn, execution_log_id, status="skipped",
                error_reason="another run already in progress",
            )
            logger.warning("Skipped: another sync run is already in progress")
        except Exception as exc:
            conn.rollback()
            _finish_run_log(conn, run_id, status="failed", error_detail=str(exc), **metrics)
            finish_execution_log(conn, execution_log_id, status="failed", error_reason=str(exc))
            _alert_run_failed(exc)
            logger.exception("Scan completo falhou, todas as mudanças foram revertidas")
            raise
    finally:
        conn.close()
