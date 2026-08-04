"""Writer para public.etl_execution_logs -- log de execução simples e
portável (independe do POSTGRES_SCHEMA configurado), separado do
etl_run_log interno que guarda as métricas de delta.

Schema definido fora deste repositório, compartilhado com outros processos
de ETL da empresa: id, project_name, operation, status, error_reason,
start_time, end_time. Não inventar colunas novas aqui.
"""
from datetime import datetime

_PROJECT_NAME = "falasca_ETL_python_API_ML"


def start_execution_log(conn, operation: str, start_time: datetime) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO public.etl_execution_logs (project_name, operation, start_time, status) "
            "VALUES (%s, %s, %s, 'running') RETURNING id",
            (_PROJECT_NAME, operation, start_time),
        )
        log_id = cur.fetchone()[0]
    conn.commit()
    return log_id


def finish_execution_log(conn, log_id: int, *, status: str, error_reason: str | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE public.etl_execution_logs SET end_time = now(), status = %s, error_reason = %s "
            "WHERE id = %s",
            (status, error_reason, log_id),
        )
    conn.commit()
