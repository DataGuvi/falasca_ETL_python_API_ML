"""Per-item enrichment: SKU, Classe, Frete, Comissao, Status Catalogo for one mlb_id.

Standalone and reusable: enrich_item(client, config, mlb_id) can be called
directly to reprocess a single listing on demand, independent of the
scan/delta pipeline.

NOTE ON UNVERIFIED RESPONSE FIELDS: shipping_options/free's shape has been
confirmed against live responses -- it only returns a usable cost when the
item has shipping.free_shipping == true, and the value sits at
coverage.<region>.list_cost (no "options" key).

NOTE ON free_shipping == false: verified live that the endpoint still
returns a list_cost for many items with free_shipping == false, tagged with
coverage.<region>.free_shipping_by_meli == true -- Mercado Livre subsidizing
the free shipping itself. Confirmed with the client this cost is NOT paid by
the seller in that case, so frete correctly stays 0/null for those -- the
free_shipping gate below is intentional, do not remove it.

Also confirmed with the client: for logistic_type == "fulfillment" (Full),
list_cost is still the table reference and gets stored as frete same as any
other free_shipping item, but Full's actual invoice can apply its own
discounts/surcharges on top -- so this value is an estimate, not meant to be
reconciled against the Full invoice.

The exact JSON key used for commission (listing_prices ->
"percentage_fee"/"fixed_fee", top-level or nested under "sale_fee_details")
is still based on the endpoint spec provided but hasn't been verified
against a live authenticated response. If bronze ends up with unexpected
nulls for comissao/taxa_fixa, check the logged raw response and adjust
_extract_comissao.

status_catalogo comes from GET /items/{id}/price_to_win?version=v2, called
only when item.catalog_listing is true (non-catalog items simply don't
compete, so status_catalogo stays null without an extra call). The raw
"status" value gets a direct, literal translation to Portuguese via
_CATALOG_STATUS_LABELS -- one label per raw value, no collapsing/
reinterpreting (winning and sharing_first_place are kept distinct, and
competing is NOT relabeled as "losing"): winning -> "Ganhando",
sharing_first_place -> "Compartilhando primeiro lugar", competing ->
"Competindo", listed -> "Listado", not_listed -> "Não listado".

If the shipping/pricing sub-calls fail (network/API error), the whole item is
treated as failed (not upserted) so it naturally retries next run, since
bronze.last_updated stays stale and the item reappears in the UPDATED delta.
A missing SKU or a shipping/pricing call that succeeds but has no applicable
value (e.g. no free-shipping option) is a legitimate business null and is
persisted, not retried.
"""
import logging

from drivers.ml_client import MLApiError

logger = logging.getLogger(__name__)


class EnrichmentError(RuntimeError):
    def __init__(self, mlb_id: str, reason: str):
        self.mlb_id = mlb_id
        self.reason = reason
        super().__init__(f"Enrichment failed for {mlb_id}: {reason}")


def _extract_top_level_sku(item: dict) -> str | None:
    if item.get("seller_custom_field"):
        return item["seller_custom_field"]
    for attr in item.get("attributes") or []:
        if attr.get("id") == "SELLER_SKU":
            return attr.get("value_name")
    return None


def _extract_variation_sku(variation: dict) -> str | None:
    if variation.get("seller_custom_field"):
        return variation["seller_custom_field"]
    for attr in variation.get("attributes") or []:
        if attr.get("id") == "SELLER_SKU":
            return attr.get("value_name")
    return None


def _extract_sku(item: dict, mlb_id: str) -> str | None:
    top_level = _extract_top_level_sku(item)
    if top_level:
        return top_level

    variations = item.get("variations") or []
    if variations:
        skus = [s for s in (_extract_variation_sku(v) for v in variations) if s]
        if skus:
            return ",".join(dict.fromkeys(skus))  # dedupe, preserve order

    logger.info(
        "Item %s has no SKU (no seller_custom_field/SELLER_SKU attribute, "
        "including in variations) - flagging as registration data issue", mlb_id,
    )
    return None


def _extract_frete(shipping_resp: dict, mlb_id: str):
    coverage = shipping_resp.get("coverage") or {}
    if not coverage:
        logger.debug("No coverage in shipping_options/free response for %s: %r", mlb_id, shipping_resp)
        return None
    first = next(iter(coverage.values()))
    value = first.get("list_cost")
    if value is None:
        logger.warning(
            "shipping_options/free for %s has coverage but no 'list_cost' "
            "-- check the field name. Raw coverage entry: %r", mlb_id, first,
        )
    return value


def _extract_comissao(listing_prices_resp, mlb_id: str) -> tuple:

    entries = listing_prices_resp if isinstance(listing_prices_resp, list) else [listing_prices_resp]
    if not entries:
        logger.warning("listing_prices returned no entries for %s: %r", mlb_id, listing_prices_resp)
        return None, None, None
    entry = entries[0]
    fee_details = entry.get("sale_fee_details") or {}

    comissao = entry.get("percentage_fee")
    if comissao is None:
        comissao = fee_details.get("percentage_fee")

    taxa_fixa = entry.get("fixed_fee")
    if taxa_fixa is None:
        taxa_fixa = fee_details.get("fixed_fee")

    tipo_anuncio = entry.get("listing_type_name")

    if comissao is None and taxa_fixa is None:
        logger.warning(
            "listing_prices for %s has no percentage_fee/fixed_fee (top-level "
            "or under sale_fee_details) -- check the field name. Raw entry: %r",
            mlb_id, entry,
        )
    return comissao, taxa_fixa, tipo_anuncio


_CATALOG_STATUS_LABELS = {
    "winning": "Ganhando",
    "sharing_first_place": "Compartilhando primeiro lugar",
    "competing": "Competindo",
    "listed": "Listado",
    "not_listed": "Não listado",
}


def _extract_status_catalogo(price_to_win_resp: dict, mlb_id: str):
    status = price_to_win_resp.get("status")
    if status is None:
        logger.warning("price_to_win for %s has no 'status' field: %r", mlb_id, price_to_win_resp)
        return None
    label = _CATALOG_STATUS_LABELS.get(status)
    if label is None:
        logger.warning("price_to_win for %s returned unknown status %r -- add it to _CATALOG_STATUS_LABELS", mlb_id, status)
        return status
    return label


def enrich_item(client, config, mlb_id: str, item: dict | None = None) -> dict:
    """`item` lets callers pass a pre-fetched item body (e.g. from a batched
    multiget covering many ids at once) to skip the per-item GET below. If
    omitted -- e.g. when reprocessing a single listing on demand -- it's
    fetched here."""
    if item is None:
        try:
            item = client.get(f"/items/{mlb_id}")
        except MLApiError as exc:
            raise EnrichmentError(mlb_id, f"item lookup failed: {exc}") from exc

    classe = item.get("listing_type_id")
    sku = _extract_sku(item, mlb_id)

    shipping = item.get("shipping") or {}
    logistic_type = shipping.get("logistic_type")
    shipping_mode = shipping.get("mode")

    frete = None
    if shipping.get("free_shipping"):
        try:
            shipping_resp = client.get(
                f"/users/{config.ml_user_id}/shipping_options/free",
                params={"item_id": mlb_id},
            )
        except MLApiError as exc:
            raise EnrichmentError(mlb_id, f"frete lookup failed: {exc}") from exc
        frete = _extract_frete(shipping_resp, mlb_id)
    else:
        logger.debug("Item %s does not have free_shipping -- skipping frete lookup", mlb_id)

    if frete is None:
        frete = 0

    comissao = taxa_fixa = tipo_anuncio = None
    price = item.get("price")
    category_id = item.get("category_id")
    if price is not None and classe and category_id and logistic_type and shipping_mode:
        try:
            listing_prices_resp = client.get(
                f"/sites/{config.ml_site_id}/listing_prices",
                params={
                    "price": price,
                    "listing_type_id": classe,
                    "category_id": category_id,
                    "logistic_type": logistic_type,
                    "shipping_mode": shipping_mode,
                },
            )
        except MLApiError as exc:
            raise EnrichmentError(mlb_id, f"comissao lookup failed: {exc}") from exc
        comissao, taxa_fixa, tipo_anuncio = _extract_comissao(listing_prices_resp, mlb_id)
    else:
        logger.warning(
            "Skipping comissao lookup for %s: missing inputs "
            "(price=%s category_id=%s logistic_type=%s shipping_mode=%s)",
            mlb_id, price, category_id, logistic_type, shipping_mode,
        )

    status_catalogo = None
    if item.get("catalog_listing"):
        try:
            price_to_win_resp = client.get(
                f"/items/{mlb_id}/price_to_win",
                params={"version": "v2"},
            )
        except MLApiError as exc:
            raise EnrichmentError(mlb_id, f"status_catalogo lookup failed: {exc}") from exc
        status_catalogo = _extract_status_catalogo(price_to_win_resp, mlb_id)
    else:
        logger.debug("Item %s is not catalog_listing -- skipping status_catalogo lookup", mlb_id)

    result = {
        "mlb": mlb_id,
        "sku": sku,
        "frete": frete,
        "classe": classe,
        "comissao": comissao,
        "taxa_fixa": taxa_fixa,
        "tipo_anuncio": tipo_anuncio,
        "status_catalogo": status_catalogo,
        "status_anuncio": item.get("status"),
        "last_updated": item.get("last_updated"),
    }

    campos = ("sku", "classe", "frete", "comissao", "taxa_fixa", "tipo_anuncio")
    faltando = [c for c in campos if result[c] is None]
    if faltando:
        logger.warning(
            "Item %s enriquecido com campos faltando (%s): sku=%s classe=%s frete=%s "
            "comissao=%s taxa_fixa=%s tipo_anuncio=%s",
            mlb_id, ", ".join(faltando), sku, classe, frete, comissao, taxa_fixa, tipo_anuncio,
        )
    else:
        logger.info(
            "Item %s enriquecido com sucesso: sku=%s classe=%s frete=%s comissao=%s "
            "taxa_fixa=%s tipo_anuncio=%s",
            mlb_id, sku, classe, frete, comissao, taxa_fixa, tipo_anuncio,
        )

    return result
