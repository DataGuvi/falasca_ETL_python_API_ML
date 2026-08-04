"""Full-catalog bootstrap: walks every active listing for the configured
seller instead of draining the webhook queue.

Used once by main_full_scan.py to backfill bronze/prata with everything
that already exists before the incremental drain (main.py) takes over.

Uses scan-search mode (search_type=scan + scroll_id) rather than plain
offset/limit pagination: offset-based search on this endpoint caps out at
1000 results total regardless of page size, which would silently truncate
a catalog larger than that. Scan mode has no such cap.

Filters status=active server-side -- paused/closed/under_review listings
aren't useful to bootstrap (compute_delta only ever classifies status=active
ids as NEW, see src/transformation/delta.py), so there's no point spending
multiget/enrich calls on them here.
"""
import logging

from drivers.ml_client import MLApiError

logger = logging.getLogger(__name__)

_SCAN_LIMIT = 100


def fetch_all_seller_items(client, config) -> list[str]:
    """Returns every active mlb_id listed under config.ml_user_id."""
    ids: list[str] = []
    scroll_id = None
    path = f"/users/{config.ml_user_id}/items/search"

    while True:
        params = {"search_type": "scan", "limit": _SCAN_LIMIT, "status": "active"}
        if scroll_id:
            params["scroll_id"] = scroll_id

        try:
            response = client.get(path, params=params)
        except MLApiError as exc:
            logger.error("Scan completo falhou ao buscar uma página de anúncios: %s", exc)
            raise

        results = response.get("results") or []
        ids.extend(results)
        scroll_id = response.get("scroll_id")

        if not results or not scroll_id:
            break

    logger.info(
        "Scan completo: %d anúncio(s) encontrados para o vendedor %s",
        len(ids), config.ml_user_id,
    )
    return ids
