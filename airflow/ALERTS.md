# Pipeline email notifications and Slack failure alerts

When `PIPELINE_ALERT_EMAIL_ENABLED=true`, the DAG sends one email after the
whole pipeline succeeds, including its batch date, run ID, start/end times
and dashboard link. This uses the same SMTP settings and recipients as failure
alerts. Task failures still alert only after retries are exhausted. Success
emails apply to future run completions; previous successful runs are not emailed
retroactively. Delivery errors are logged without changing the pipeline result.

## Brevo (current SMTP provider)

The existing SMTP callback also supports Brevo; no new Python dependencies or
image build are needed. The local host/port/security settings use Brevo on port
587 with STARTTLS. Add and verify the From sender in Brevo, and authenticate
your sending domain where required. Copy the SMTP Login from Settings > SMTP
& API; it can differ from your Brevo account email and from the From address.

Fill these entries in ignored `airflow/.env` locally:

```dotenv
PIPELINE_SMTP_HOST=smtp-relay.brevo.com
PIPELINE_SMTP_PORT=587
PIPELINE_SMTP_SECURITY=starttls
PIPELINE_SMTP_USERNAME=your-brevo-smtp-login
PIPELINE_SMTP_PASSWORD=your-brevo-smtp-key
PIPELINE_ALERT_EMAIL_FROM=your-verified-sender-address
PIPELINE_ALERT_EMAIL_TO=kayodeabdulaziz@gmail.com
PIPELINE_ALERT_EMAIL_ENABLED=true
```

Use the SMTP key, not a Brevo API key or account password. A technical SMTP
login such as an address ending in `@smtp-brevo.com` is for authentication;
do not use it as the From address. The recipient can remain Gmail even though
Brevo sends the mail. Do not paste the key into chat or commit it.

After completing the fields, apply settings as described below. Sending a
real delivery test is a separate action; none has been sent automatically.
Reference: [Brevo SMTP setup](https://help.brevo.com/hc/en-us/articles/7924908994450-Send-transactional-emails-using-Brevo-SMTP).

Every task in `chaos_ecommerce_daily` uses `notify_task_failure` from
`dags/pipeline_alerts.py`. It attempts email and Slack independently after a
task's retries are exhausted. A failure in one delivery channel does not block
the other. No connections occur at import, and disabled channels do nothing.

The alert contains DAG/task/run identifiers, batch date, error class and a log
or dashboard link. It intentionally excludes raw exception text, shell commands,
and environment variables. Detailed errors stay in the Airflow logs.

## Gmail

Enable 2-Step Verification in the sending Google account and generate an
App Password for this integration. Use that App Password, not the normal Google
password. Some managed/Advanced Protection accounts cannot issue App Passwords;
if unavailable, configure another SMTP sender or an OAuth-capable integration.
See [Google's App Password instructions](https://support.google.com/accounts/answer/185833).

Add these lines to ignored `airflow/.env`, keeping the existing UID, Airflow
login and encryption keys. Replace placeholders with your own addresses and
App Password locally; do not paste secrets into chat or commit them.

```dotenv
PIPELINE_ALERT_EMAIL_ENABLED=true
PIPELINE_SMTP_HOST=smtp.gmail.com
PIPELINE_SMTP_PORT=587
PIPELINE_SMTP_SECURITY=starttls
PIPELINE_SMTP_USERNAME=your-sending-account@gmail.com
PIPELINE_SMTP_PASSWORD=your-google-app-password
PIPELINE_ALERT_EMAIL_FROM=your-sending-account@gmail.com
PIPELINE_ALERT_EMAIL_TO=recipient@example.com
```

The sender should match the Gmail account used for authentication. Multiple
recipients can be separated by commas. SSL on port 465 is supported by setting
`PIPELINE_SMTP_SECURITY=ssl` and `PIPELINE_SMTP_PORT=465`.

## Slack

Create a Slack app, enable Incoming Webhooks, and authorize a webhook for the
channel that should receive alerts. Follow [Slack's incoming-webhook guide](https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks/).

Add to `airflow/.env`:

```dotenv
PIPELINE_ALERT_SLACK_ENABLED=true
PIPELINE_SLACK_WEBHOOK_URL=your-private-incoming-webhook-url
```

The webhook selects the destination channel; this integration does not override
it. Both channels can be enabled together or one can remain disabled. Store the
webhook only in local secret configuration. It is an authenticated secret URL.

## Apply settings

After any currently running pipeline run finishes, from `airflow/`:

```powershell
docker compose up -d --no-build
```

No image rebuild is needed: callbacks use Python's standard library and the DAG
files are mounted. Compose must recreate services to load changed env files;
restarting a container alone does not refresh its environment. Alert settings
are in `airflow/.env`, shared with Airflow components that execute callbacks,
not in the root Snowflake `.env` supplied only to workers.

Leave `PIPELINE_ALERT_EMAIL_ENABLED=false` or
`PIPELINE_ALERT_SLACK_ENABLED=false` until each channel is configured. No real
messages have been sent during implementation; automated tests mock SMTP and
HTTP delivery. Live delivery needs a separately authorized notification test.

Delivery failures are best-effort and logged without secret exception strings.
Check `airflow/logs/dag_processor/` for callback errors; they may not appear in
the failed task's UI log. Airflow does not guarantee notification delivery, and
there is no durable notification queue in this implementation. Re-running a
task and failing it again may send another alert. Manually marking a task failed
in the UI/CLI does not trigger normal failure callbacks.

Localhost log links work only on the same computer. If a context has no task log
URL, `PIPELINE_ALERT_BASE_URL` supplies a dashboard fallback (default
`http://localhost:8080`). These alerts also cannot run when Docker/Airflow or
the computer is off; monitoring Airflow itself requires an external monitor.
