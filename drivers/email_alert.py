"""Alerta por e-mail quando um run falha, enviado via utils/log_mail_message.py
(SMTP da Email em Nuvem -- credenciais em EMAIL_SENDER/SENDER_PASSWORD/
EMAIL_RECEIVER, ver .env.example). O registro em etl_run_log/
etl_execution_logs continua acontecendo independente disso (ver
src/pipeline.py)."""
from utils.log_mail_message import send_email

SUBJECT = "[FALHA ENGENHARIA] Falasca - Python ETL"


def send_failure_alert(exc: Exception) -> None:
    send_email(SUBJECT, f"Falha na {exc}")
