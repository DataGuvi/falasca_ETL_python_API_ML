import os
import smtplib
from email.message import EmailMessage

from dotenv import load_dotenv


def send_email(subject, body):
    load_dotenv()
    sender_email = os.getenv("EMAIL_SENDER")
    app_password = os.getenv("SENDER_PASSWORD")
    to_email = os.getenv("EMAIL_RECEIVER")

    if not sender_email or not app_password:
        print("Error: Please set SENDER_EMAIL and SENDER_PASSWORD environment variables.")
        return

    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = f"Logs <{sender_email}>"
    msg['To'] = to_email

    try:
        # Using Email em Nuvem SMTP server.
        with smtplib.SMTP_SSL('smtp.emailemnuvem.com.br', 465) as smtp:
            smtp.login(sender_email, app_password)
            smtp.send_message(msg)
            print(f"Email sent successfully to {to_email}")
    except Exception as e:
        print(f"Failed to send email. Error: {e}")
