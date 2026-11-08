"""All delivery is mocked: no SMTP, Slack or external messages are sent."""

import importlib.util
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


spec = importlib.util.spec_from_file_location("pipeline_alerts_test", Path(__file__).resolve().parents[1] / "dags/pipeline_alerts.py")
alerts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(alerts)


@pytest.fixture(autouse=True)
def clear_alert_environment(monkeypatch):
    import os

    for name in list(os.environ):
        if name.startswith("PIPELINE_ALERT_") or name.startswith("PIPELINE_SMTP_") or name == "PIPELINE_SLACK_WEBHOOK_URL":
            monkeypatch.delenv(name)


@pytest.fixture
def context():
    return {
        "task_instance": SimpleNamespace(dag_id="chaos_ecommerce_daily", task_id="load_raw", run_id="scheduled_test", log_url="http://localhost:8080/task-logs"),
        "params": {"batch_date": "2026-11-06"},
        "exception": RuntimeError("must-not-send-this-secret"),
    }


def test_alerts_disabled_by_default(monkeypatch, context):
    email, slack = Mock(), Mock()
    monkeypatch.setattr(alerts, "send_email", email)
    monkeypatch.setattr(alerts, "send_slack", slack)
    alerts.notify_task_failure(context)
    email.assert_not_called()
    slack.assert_not_called()


def test_both_channels_attempted_even_if_email_fails(monkeypatch, context, caplog):
    monkeypatch.setenv("PIPELINE_ALERT_EMAIL_ENABLED", "true")
    monkeypatch.setenv("PIPELINE_ALERT_SLACK_ENABLED", "true")
    email = Mock(side_effect=RuntimeError("must-not-log-this-secret"))
    slack = Mock()
    monkeypatch.setattr(alerts, "send_email", email)
    monkeypatch.setattr(alerts, "send_slack", slack)
    with caplog.at_level(logging.ERROR):
        alerts.notify_task_failure(context)
    email.assert_called_once()
    slack.assert_called_once()
    assert "must-not-log-this-secret" not in caplog.text
    assert "must-not-send-this-secret" not in slack.call_args.args[1]


def test_slack_failure_does_not_undo_email(monkeypatch, context, caplog):
    monkeypatch.setenv("PIPELINE_ALERT_EMAIL_ENABLED", "true")
    monkeypatch.setenv("PIPELINE_ALERT_SLACK_ENABLED", "true")
    email = Mock()
    monkeypatch.setattr(alerts, "send_email", email)
    monkeypatch.setattr(alerts, "send_slack", Mock(side_effect=RuntimeError("secret-url")))
    with caplog.at_level(logging.ERROR):
        alerts.notify_task_failure(context)
    email.assert_called_once()
    assert "slack failure alert was not delivered" in caplog.text
    assert "secret-url" not in caplog.text


@pytest.mark.parametrize("security,port", [("starttls", 587), ("ssl", 465)])
def test_smtp_transport_authentication_and_recipients(monkeypatch, security, port):
    settings = {
        "PIPELINE_SMTP_HOST": "smtp.gmail.com",
        "PIPELINE_SMTP_SECURITY": security,
        "PIPELINE_SMTP_USERNAME": "sender@example.test",
        "PIPELINE_SMTP_PASSWORD": "fake-app-password",
        "PIPELINE_ALERT_EMAIL_FROM": "sender@example.test",
        "PIPELINE_ALERT_EMAIL_TO": "first@example.test, second@example.test",
    }
    for name, value in settings.items(): monkeypatch.setenv(name, value)
    smtp = Mock()
    smtp.__enter__ = Mock(return_value=smtp)
    smtp.__exit__ = Mock(return_value=False)
    constructor = Mock(return_value=smtp)
    monkeypatch.setattr(alerts.smtplib, "SMTP" if security == "starttls" else "SMTP_SSL", constructor)
    alerts.send_email("Failed task", "Safe summary")
    assert constructor.call_args.args == ("smtp.gmail.com", port)
    assert smtp.starttls.call_count == (1 if security == "starttls" else 0)
    smtp.login.assert_called_once_with("sender@example.test", "fake-app-password")
    assert smtp.send_message.call_args.kwargs["to_addrs"] == ["first@example.test", "second@example.test"]


def test_slack_payload_and_acknowledgement(monkeypatch):
    monkeypatch.setenv("PIPELINE_SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/EXAMPLE/EXAMPLE/fake")
    response = Mock(status=200)
    response.read.return_value = b"ok"
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    request = Mock(return_value=response)
    monkeypatch.setattr(alerts, "urlopen", request)
    alerts.send_slack("Failed task", "Safe summary")
    payload = json.loads(request.call_args.args[0].data)
    assert payload == {"text": "Failed task\n\nSafe summary"}
    assert request.call_args.kwargs["timeout"] == alerts.TIMEOUT


@pytest.mark.parametrize("url", ["", "http://hooks.slack.com/services/fake", "https://example.test/services/fake"])
def test_invalid_webhook_rejected_before_network(monkeypatch, url):
    monkeypatch.setenv("PIPELINE_SLACK_WEBHOOK_URL", url)
    request = Mock()
    monkeypatch.setattr(alerts, "urlopen", request)
    with pytest.raises(ValueError): alerts.send_slack("Failure", "Summary")
    request.assert_not_called()


def test_details_have_context_without_raw_exception(context):
    subject, body = alerts.failure_details(context)
    assert "load_raw" in subject
    assert "2026-11-06" in body
    assert "scheduled_test" in body
    assert "RuntimeError" in body
    assert "http://localhost:8080/task-logs" in body
    assert "must-not-send-this-secret" not in body


def test_success_email_disabled_by_default(monkeypatch):
    email = Mock()
    monkeypatch.setattr(alerts, "send_email", email)
    alerts.notify_dag_success({})
    email.assert_not_called()


def test_success_email_uses_run_override_and_sends_only_email(monkeypatch):
    monkeypatch.setenv("PIPELINE_ALERT_EMAIL_ENABLED", "true")
    email, slack = Mock(), Mock()
    monkeypatch.setattr(alerts, "send_email", email)
    monkeypatch.setattr(alerts, "send_slack", slack)
    run = SimpleNamespace(dag_id="chaos_ecommerce_daily", run_id="manual_test",
                          conf={"batch_date": "2026-11-07"})
    alerts.notify_dag_success({"dag_run": run, "params": {"batch_date": "2026-11-06"}})
    email.assert_called_once()
    subject, body = email.call_args.args
    assert "SUCCESS" in subject
    assert "2026-11-07" in body
    assert "2026-11-06" not in body
    assert "manual_test" in body
    slack.assert_not_called()


def test_success_email_failure_is_sanitized(monkeypatch, caplog):
    monkeypatch.setenv("PIPELINE_ALERT_EMAIL_ENABLED", "true")
    monkeypatch.setattr(alerts, "send_email", Mock(side_effect=RuntimeError("secret-key")))
    with caplog.at_level(logging.ERROR):
        alerts.notify_dag_success({})
    assert "success notification was not delivered" in caplog.text
    assert "secret-key" not in caplog.text
