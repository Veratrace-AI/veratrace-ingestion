# Veratrace Ingestion Service

Standalone service that connects enterprise systems (Amazon Connect, Salesforce, Zendesk, AI model APIs) and produces TwuSignals for the TWU Compiler.

## Architecture

This service owns **vendor API → signal in database**. Joey's control plane owns **signal in database → sealed TWU**.

The boundary is the `twu_signals` table and the `POST /instances/{id}/tasks` trigger.

## Connectors

| Connector | Auth | Real-Time | Polling | Status |
|-----------|------|-----------|---------|--------|
| Amazon Connect | IAM AssumeRole + ExternalId | Kinesis stream (planned) | SearchContacts API | **Live** |
| Salesforce | OAuth 2.0 | Change Data Capture | REST / Bulk API 2.0 | Planned |
| Zendesk | OAuth 2.0 / API token | Webhooks | Incremental Export | Planned |
| OpenAI / Anthropic | Proxy / SDK wrapper | Per request | N/A | Planned |

## Shared Runtime

Every connector uses the same infrastructure:

- **Rate limiter** — token bucket per endpoint, 70% of vendor ceiling
- **Retry engine** — decorrelated jitter, circuit breaker at 50% error rate
- **Cursor manager** — per-stream checkpoint persistence
- **Region router** — parse region from ARN/endpoint, enforce data residency
- **Schema validator** — drift detection, degraded signal flagging
- **Signal writer** — idempotent upsert, PII encryption
- **Task trigger** — fire TWU Compiler via SQS after each sync batch

## HTTP API

Runs on port 8090. Proxied via Caddy at `https://ingestion.veratrace.ai`.

| Endpoint | Method | Auth | Purpose |
|----------|--------|------|---------|
| `/health` | GET | None | Liveness check |
| `/health/warming` | GET | None | Warming cron status (last run, log tail) |
| `/sync` | POST | `X-API-Key` | Trigger immediate sync for an integration account |
| `/test-connection` | POST | `X-API-Key` | Test AWS credentials (STS AssumeRole + DescribeInstance) |

## Signal Extraction (Amazon Connect)

Each CTR produces 2-4 signals depending on what happened:

| Signal | Type | When |
|--------|------|------|
| `contact_initiated` | SYSTEM | Always — customer entered the system |
| `ai_interaction` | AI | When Lex bot processed the contact (bot name, intent, confidence) |
| `agent_connected` | HUMAN | When a human agent took over |
| `contact_completed` | varies | Always — includes Contact Lens sentiment, categories, structured attributes |

## Deployment

Deployed to DigitalOcean via the **veraagents** repo's GitHub Actions workflow. This repo's CI runs tests only — deploy is handled by `veraagents/.github/workflows/deploy.yml`.

- **API:** `https://ingestion.veratrace.ai` (Caddy reverse proxy, auto Let's Encrypt)
- **Cron sync:** every 15 min weekdays (UTC 14:00–02:00)
- **Cron warming:** every hour weekdays (5 contacts/hour, ~$16/month AWS cost)
- **Firewall:** UFW allows ports 22, 80, 443, 8090

## Sandbox Warming

Creates real contacts in a Connect sandbox instance for end-to-end pipeline testing:

```bash
python -m synthetic.warm --platform amazon-connect \
  --instance-arn ARN --role-arn ARN --external-id ID \
  --contacts 5 --sync-after
```

Scenarios model enterprise AI/human hybrid operations (weighted distribution):
- 35% AI auto-resolved (password resets, balance checks)
- 25% AI triage → human resolve (billing disputes, contracts)
- 15% Human-only (compliance, escalations)
- 10% SLA-critical (outages, urgent callbacks)
- 10% Transfers (skill mismatch, language)
- 5% Vendor reconciliation (BPO overclaim detection)

Chat contacts send messages to trigger Lex bot processing. See `sandbox/README.md` for full setup.

## Synthetic Data (DB Seeding)

For demo environments without real vendor access:

```bash
python -m synthetic.seed_rds --instance-id UUID --contacts 150
```

## Running Locally

```bash
export SUPABASE_URL="https://..."
export SUPABASE_SERVICE_ROLE_KEY="..."
export CONTROL_PLANE_URL="https://veratrace-control-plane.onrender.com"
export INGESTION_API_KEY="your-key"  # optional, skipped if empty

python -m src.main          # HTTP API on :8090
python -m src.main --port 9090  # custom port
```

## Testing

```bash
pip install boto3 pytest
python -m pytest tests/ -v
```

**80 tests** across 5 files:
- `test_amazon_connect.py` — CTR signal mapping (basic contacts)
- `test_signal_mapper_ai.py` — AI extraction: Lex bot, Contact Lens, structured attributes
- `test_api_auth.py` — API key enforcement on POST endpoints
- `test_synthetic_shapes.py` — contract tests validating JSONB matches Java entity models
- `test_warmers.py` — base warmer behavior, Connect warmer scenarios + distribution
