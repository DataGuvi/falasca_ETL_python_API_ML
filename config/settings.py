"""Environment-driven configuration. No secrets or IDs hardcoded here."""
import os
from dataclasses import dataclass


class ConfigError(RuntimeError):
    pass


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def _int(name: str, default: int) -> int:
    value = os.environ.get(name)
    return int(value) if value else default


@dataclass(frozen=True)
class Config:
    ml_user_id: str
    ml_client_id: str
    ml_client_secret: str
    ml_refresh_token: str
    database_url: str
    postgres_schema: str

    ml_site_id: str
    multiget_batch_size: int

    http_timeout: int
    max_retries: int

    #: Saída de negócio na planilha Google Sheets "EXTRAÇÃO DE ANÚNCIOS MLB"
    #: (ver drivers/google_sheets_output.py). Qualquer um vazio desativa a
    #: escrita (NullGoogleSheetsClient).
    google_service_account_file: str
    google_sheets_spreadsheet_id: str
    google_sheets_worksheet_name: str


def _build_database_url() -> str:
    from urllib.parse import quote_plus

    host = _require("POSTGRES_HOST")
    port = os.environ.get("POSTGRES_PORT", "5432")
    dbname = _require("POSTGRES_DB")
    user = _require("POSTGRES_USER")
    password = _require("POSTGRES_PASSWORD")
    return (
        f"postgresql://{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{dbname}"
    )


def load_config() -> Config:
    multiget_batch_size = _int("ML_MULTIGET_BATCH_SIZE", 20)
    if multiget_batch_size > 20:
        raise ConfigError("ML_MULTIGET_BATCH_SIZE cannot exceed 20 (ML API cap)")

    return Config(
        ml_user_id=_require("ML_USER_ID"),
        ml_client_id=_require("ML_CLIENT_ID"),
        ml_client_secret=_require("ML_CLIENT_SECRET"),
        ml_refresh_token=_require("ML_REFRESH_TOKEN"),
        database_url=_build_database_url(),
        postgres_schema=os.environ.get("POSTGRES_SCHEMA", "public"),
        ml_site_id=os.environ.get("ML_SITE_ID", "MLB"),
        multiget_batch_size=multiget_batch_size,
        http_timeout=_int("ML_HTTP_TIMEOUT", 30),
        max_retries=_int("ML_MAX_RETRIES", 5),
        google_service_account_file=os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE", ""),
        google_sheets_spreadsheet_id=os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID", ""),
        google_sheets_worksheet_name=os.environ.get("GOOGLE_SHEETS_WORKSHEET_NAME", "MATRIZ"),
    )
