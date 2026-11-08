"""Pipeline success emails and optional email/Slack failure alerts.

No network calls occur at import. Airflow calls notify_task_failure only on a
task's final failure, not while it is waiting to retry.
"""

from __future__ import annotations

import json
import logging
import os
import smtplib
import ssl
from email.message import EmailMessage
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


LOG = logging.getLogger("airflow.pipeline_alerts")
TIMEOUT = 10


def enabled(name: str) -> bool:
    return os.environ.get(name, "false").strip().lower() in {"true", "1", "yes"}


def failure_details(context: dict) -> tuple[str, str]:
    ti = context.get("task_instance") or context.get("ti")
    dag = context.get("dag")
    task = context.get("task")
    run = context.get("dag_run")
    dag_id = getattr(ti, "dag_id", None) or getattr(dag, "dag_id", "unknown")
    task_id = getattr(ti, "task_id", None) or getattr(task, "task_id", "unknown")
    run_id = getattr(ti, "run_id", None) or getattr(run, "run_id", "unknown")
    batch_date = (context.get("params") or {}).get("batch_date")
    interval = context.get("data_interval_start")
    if not batch_date and interval is not None:
        batch_date = interval.astimezone(ZoneInfo("Africa/Lagos")).date().isoformat()
    error = context.get("exception")
    error_type = type(error).__name__ if error is not None else "See task logs"
    log_url = getattr(ti, "log_url", None) or os.environ.get("PIPELINE_ALERT_BASE_URL", "http://localhost:8080")
    subject = f"[Airflow FAILED] {dag_id} / {task_id}"
    body = "\n".join([
        "The task failed after its configured retries; downstream work is blocked.",
        f"DAG: {dag_id}",
        f"Task: {task_id}",
        f"Batch date: {batch_date or 'unknown'}",
        f"Run: {run_id}",
        f"Error type: {error_type}",
        f"Logs/dashboard: {log_url}",
        "Inspect the logs, fix the cause, then retry the failed run before later batches.",
    ])
    # Raw exception text, commands and environment variables are deliberately
    # excluded because they may contain credentials or source data.
    return subject, body


def send_email(subject: str, body: str) -> None:
    host = os.environ.get("PIPELINE_SMTP_HOST", "").strip()
    sender = os.environ.get("PIPELINE_ALERT_EMAIL_FROM", "").strip()
    recipients = [v.strip() for v in os.environ.get("PIPELINE_ALERT_EMAIL_TO", "").split(",") if v.strip()]
    if not host or not sender or not recipients:
        raise ValueError("Email alerts require SMTP host, sender and recipients.")
    security = os.environ.get("PIPELINE_SMTP_SECURITY", "starttls").strip().lower()
    if security not in {"starttls", "ssl"}:
        raise ValueError("PIPELINE_SMTP_SECURITY must be starttls or ssl.")
    port = int(os.environ.get("PIPELINE_SMTP_PORT") or ("465" if security == "ssl" else "587"))
    username = os.environ.get("PIPELINE_SMTP_USERNAME", "").strip()
    password = os.environ.get("PIPELINE_SMTP_PASSWORD", "")
    if username and not password:
        raise ValueError("SMTP username is set but password is missing.")
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    message.set_content(body)
    tls_context = ssl.create_default_context()
    client = (
        smtplib.SMTP_SSL(host, port, timeout=TIMEOUT, context=tls_context)
        if security == "ssl" else smtplib.SMTP(host, port, timeout=TIMEOUT)
    )
    with client as smtp:
        if security == "starttls":
            smtp.starttls(context=tls_context)
        if username:
            smtp.login(username, password)
        smtp.send_message(message, from_addr=sender, to_addrs=recipients)


def send_slack(subject: str, body: str) -> None:
    webhook = os.environ.get("PIPELINE_SLACK_WEBHOOK_URL", "").strip()
    parsed = urlsplit(webhook)
    if parsed.scheme != "https" or parsed.hostname not in {"hooks.slack.com", "hooks.slack-gov.com"} or not parsed.path.startswith("/services/"):
        raise ValueError("Configure an HTTPS Slack incoming webhook URL.")
    payload = json.dumps({"text": subject + "\n\n" + body}).encode("utf-8")
    request = Request(webhook, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(request, timeout=TIMEOUT) as response:
        if response.status != 200 or response.read(1024).strip() != b"ok":
            raise RuntimeError("Slack did not acknowledge the alert.")


def notify_task_failure(context: dict) -> None:
    subject, body = failure_details(context)
    for channel, flag, sender in [
        ("email", "PIPELINE_ALERT_EMAIL_ENABLED", send_email),
        ("slack", "PIPELINE_ALERT_SLACK_ENABLED", send_slack),
    ]:
        if not enabled(flag):
            LOG.info("%s failure alerts disabled", channel)
            continue
        try:
            sender(subject, body)
            LOG.info("%s failure alert sent", channel)
        except Exception as exc:
            # Do not log exception messages/tracebacks: HTTP errors may contain
            # the secret webhook URL and SMTP exceptions can contain addresses.
            LOG.error("%s failure alert was not delivered (%s); inspect alert configuration/connectivity", channel, type(exc).__name__)
            # Continue so the other channel still gets its delivery attempt.


def notify_dag_success(context: dict) -> None:
    """Send one completion email for the whole DAG, rather than every task."""
    if not enabled("PIPELINE_ALERT_EMAIL_ENABLED"):
        return
    run = context.get("dag_run")
    dag = context.get("dag")
    dag_id = getattr(run, "dag_id", None) or getattr(dag, "dag_id", "unknown")
    run_id = getattr(run, "run_id", "unknown")
    batch_date = (getattr(run, "conf", None) or {}).get("batch_date") or (context.get("params") or {}).get("batch_date")
    interval = getattr(run, "data_interval_start", None) or context.get("data_interval_start")
    if not batch_date and interval is not None:
        batch_date = interval.astimezone(ZoneInfo("Africa/Lagos")).date().isoformat()
    subject = f"[Airflow SUCCESS] {dag_id}"
    body = "\n".join([
        "The pipeline completed successfully.",
        f"DAG: {dag_id}",
        f"Batch date: {batch_date or 'unknown'}",
        f"Run: {run_id}",
        f"Started: {getattr(run, 'start_date', None)}",
        f"Finished: {getattr(run, 'end_date', None)}",
        f"Dashboard: {os.environ.get('PIPELINE_ALERT_BASE_URL', 'http://localhost:8080')}",
    ])
    try:
        send_email(subject, body)
        LOG.info("email success notification sent")
    except Exception as exc:
        LOG.error("email success notification was not delivered (%s); inspect alert configuration/connectivity", type(exc).__name__)
