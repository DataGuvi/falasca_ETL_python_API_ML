"""Google Sheets output -- the real business output (aba MATRIZ da planilha
"EXTRAÇÃO DE ANÚNCIOS MLB"), autenticado via service account.

One row per mlb_id, upserted in place by MLB (column A) or appended when
new; deleted listings have their row removed. Column order matches
TRACKED_FIELDS in src/load/warehouse.py exactly (mlb + the 7 tracked
business fields).
"""
import logging

import gspread
from google.oauth2.service_account import Credentials

from src.load.warehouse import TRACKED_FIELDS

logger = logging.getLogger(__name__)

HEADER = ("MLB", "SKU", "FRETE (R$)", "CLASSE", "COMISSÃO (%)", "TAXA FIXA", "TIPO ANUNCIO", "STATUS CATALOGO")

_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


class GoogleSheetsClient:
    def __init__(self, credentials_path: str, spreadsheet_id: str, worksheet_name: str):
        creds = Credentials.from_service_account_file(credentials_path, scopes=_SCOPES)
        gc = gspread.authorize(creds)
        self._spreadsheet = gc.open_by_key(spreadsheet_id)
        self._worksheet_name = worksheet_name

    def upsert_rows(self, rows: list[dict]) -> None:
        if not rows:
            return
        ws = self._worksheet()
        index = self._mlb_row_index(ws)
        updates = []
        appends = []
        for row in rows:
            values = _row_values(row)
            row_number = index.get(row["mlb"])
            if row_number:
                updates.append({"range": f"A{row_number}", "values": [values]})
            else:
                appends.append(values)
        if updates:
            ws.batch_update(updates, value_input_option="USER_ENTERED")
        if appends:
            ws.append_rows(appends, value_input_option="USER_ENTERED")

    def remove_rows(self, mlb_ids: list[str]) -> None:
        if not mlb_ids:
            return
        ws = self._worksheet()
        index = self._mlb_row_index(ws)
        # Ordem decrescente: apagar de baixo pra cima não invalida os
        # índices das linhas que ainda faltam apagar neste mesmo lote.
        row_numbers = sorted((index[mlb] for mlb in mlb_ids if mlb in index), reverse=True)
        if not row_numbers:
            return
        requests = [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": ws.id,
                        "dimension": "ROWS",
                        "startIndex": row_number - 1,
                        "endIndex": row_number,
                    }
                }
            }
            for row_number in row_numbers
        ]
        self._spreadsheet.batch_update({"requests": requests})

    def _worksheet(self):
        try:
            return self._spreadsheet.worksheet(self._worksheet_name)
        except gspread.WorksheetNotFound:
            ws = self._spreadsheet.add_worksheet(title=self._worksheet_name, rows=1000, cols=len(HEADER))
            ws.append_row(list(HEADER))
            return ws

    @staticmethod
    def _mlb_row_index(ws) -> dict[str, int]:
        return {
            value: i + 1
            for i, value in enumerate(ws.col_values(1))
            if i > 0 and value
        }


class NullGoogleSheetsClient:
    """No-op usado quando as credenciais/spreadsheet do Google Sheets não
    estão configuradas (recurso desativado)."""

    def upsert_rows(self, rows: list[dict]) -> None:
        if rows:
            logger.warning(
                "Google Sheets não configurado; %d linha(s) não gravada(s) em planilha.",
                len(rows),
            )

    def remove_rows(self, mlb_ids: list[str]) -> None:
        pass


def build_google_sheets_client(config):
    if not (config.google_service_account_file and config.google_sheets_spreadsheet_id):
        return NullGoogleSheetsClient()
    return GoogleSheetsClient(
        config.google_service_account_file,
        config.google_sheets_spreadsheet_id,
        config.google_sheets_worksheet_name,
    )


def _row_values(row: dict) -> list:
    return [row["mlb"]] + [row.get(field) for field in TRACKED_FIELDS]
