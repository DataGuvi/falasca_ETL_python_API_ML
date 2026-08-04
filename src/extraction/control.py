"""Multiget-based batch fetches: at most 20 ids per call (confirmed ML API
cap on /items multiget). Partial per-item and per-batch failures are logged
and skipped rather than aborting the whole run.
"""
import logging

from drivers.ml_client import MLApiError

logger = logging.getLogger(__name__)

# Full set of fields enrich_item() needs from the item, fetched via multiget
# instead of one GET /items/{id} per item -- up to 20x fewer requests for
# this step versus a per-item lookup.
_FULL_ATTRIBUTES = (
    "id,last_updated,status,price,category_id,shipping,catalog_listing,"
    "seller_custom_field,attributes,variations,listing_type_id"
)


def _multiget(client, config, mlb_ids: list[str], attributes: str) -> tuple[dict[str, dict], list[str]]:
    by_mlb: dict[str, dict] = {}
    failed: list[str] = []
    batch_size = config.multiget_batch_size

    for i in range(0, len(mlb_ids), batch_size):
        batch = mlb_ids[i:i + batch_size]
        try:
            entries = client.get("/items", params={
                "ids": ",".join(batch),
                "attributes": attributes,
            })
        except MLApiError as exc:
            logger.warning("Multiget batch failed (%d ids): %s", len(batch), exc)
            failed.extend(batch)
            continue

        returned_ids = set()
        for entry in entries:
            body = entry.get("body") or {}
            item_id = body.get("id")
            if entry.get("code") == 200 and item_id:
                by_mlb[item_id] = body
                returned_ids.add(item_id)
            elif item_id:
                logger.warning("Multiget item %s failed with code %s", item_id, entry.get("code"))
                failed.append(item_id)
                returned_ids.add(item_id)

        for mlb_id in batch:
            if mlb_id not in returned_ids:
                logger.warning("Multiget response missing id %s", mlb_id)
                failed.append(mlb_id)

    return by_mlb, failed


def fetch_control_info(client, config, mlb_ids: list[str]) -> tuple[dict[str, dict], list[str]]:
    """Returns (info_by_mlb, failed_mlb_ids). info_by_mlb maps mlb_id ->
    {"last_updated": str, "status": str}."""
    by_mlb, failed = _multiget(client, config, mlb_ids, "id,last_updated,status")
    info_by_mlb = {
        mlb_id: {"last_updated": body.get("last_updated"), "status": body.get("status")}
        for mlb_id, body in by_mlb.items()
    }
    return info_by_mlb, failed


def fetch_items_full(client, config, mlb_ids: list[str]) -> tuple[dict[str, dict], list[str]]:
    """Batched equivalent of GET /items/{id} for enrich_item's Passo 2 fields.
    Returns (item_by_mlb, failed_mlb_ids); failed ids simply aren't in the
    dict, so callers should fall back to a per-item lookup for those."""
    return _multiget(client, config, mlb_ids, _FULL_ATTRIBUTES)
