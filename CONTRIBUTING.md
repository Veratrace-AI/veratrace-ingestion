# Adding a New Connector

This guide takes you from zero to a deployed, tested, warming connector. Follow these 6 steps — the connector auto-registers, no manual wiring needed.

**Reference implementation:** `src/connectors/amazon_connect/` (80 tests, deployed, warming hourly)

## Step 1: Copy the template

```bash
cp -r src/connectors/_template src/connectors/your_platform
```

Edit `src/connectors/your_platform/__init__.py`:
```python
from src.connectors.your_platform.connector import YourPlatformConnector
CONNECTOR_ID = "your-platform"
CONNECTOR_CLASS = YourPlatformConnector
```

That's it for registration — the scheduler discovers connectors automatically via `src/connectors/__init__.py`.

## Step 2: Implement the connector

Edit `src/connectors/your_platform/connector.py`:

1. **Rename the class** to match your platform
2. **Set CONFIG** — override rate limits, page size, cursor format:
   ```python
   CONFIG = {
       **BaseConnector.CONFIG,
       "rate_limit_rps": 20.0,         # Salesforce: 20 req/sec
       "cursor_format": "id",           # or "iso8601", "offset", "token"
       "max_results_per_page": 200,
   }
   ```
3. **Implement auth** — `validate_credentials()` checks format, `test_connection()` makes a live API call
4. **Implement sync** — `sync_incremental(cursor)` fetches records since last cursor, `sync_backfill(start_date)` fetches historical

Key patterns from Amazon Connect:
- Cache credentials with `threading.Lock()` if using token refresh
- Rate limit between pages: `time.sleep(self._sync_delay)`
- Return `SyncResult(signals=signals, cursor=new_cursor)`

## Step 3: Build the signal mapper

Edit `src/connectors/your_platform/signal_mapper.py`:

Transform each vendor API record into 1-4 TwuSignals:
- `{entity}_initiated` — work item created (actor: SYSTEM)
- `ai_interaction` — AI processed/routed the work (actor: AI, include confidence)
- `agent_connected` — human took over (actor: HUMAN)
- `{entity}_completed` — work item resolved (include duration, resolution, sentiment)

Rules:
- `source_integration` must match your `CONNECTOR_ID`
- Set `degraded=True` if `REQUIRED_FIELDS` are missing
- Preserve raw record in last signal's `payload["_raw"]`
- Parse structured attributes into typed fields (don't pass raw dicts)

## Step 4: Define the schema

Edit `src/connectors/your_platform/schema.py`:

- `EXPECTED_FIELDS` — all top-level fields in the vendor's API response
- `REQUIRED_FIELDS` — subset that must be present for a valid signal
- `PII_FIELDS` — fields containing customer data (encrypted at write time)
- `EXPECTED_SCHEMA_HASH` — auto-computed from field names

## Step 5: Add test fixtures + tests

1. Save a real API response as `test_fixtures/sample_response.json`
2. Create `tests/test_connectors/test_your_platform.py`:
   - Test signal count per record
   - Test actor_type (AI vs HUMAN vs SYSTEM)
   - Test payload structure
   - Test degraded handling for missing fields
   - Test backward compatibility (old records still produce valid signals)

Run: `python -m pytest tests/ -v`

## Step 6: Add metadata to clearline-ui

Create `clearline-ui/src/features/integrations/metadata/yourPlatform.js`:
```javascript
export const yourPlatformMetadata = {
    label: "Your Platform",
    category: "crm",  // or "support", "ai", etc.
    logo: yourPlatformLogo,
    catalog: {
        category: "CRM",
        tagline: "One sentence about what data flows in.",
        setupTime: "~5 min",
        setupMethod: "OAuth",
        available: false,  // true when connector is production-ready
        detail: { dataSources: [...], permissions: [...], setupSteps: [...] },
    },
    core: [...],       // form fields (name, etc.)
    credentials: [...], // auth fields
    metadata: [...],    // optional fields
};
```

Import in `metadata/index.js` — the catalog, filters, and detail drawer auto-wire.

## Optional: Add a warmer

Create `synthetic/warmers/your_platform.py`:
```python
from synthetic.warmers.base import BaseWarmer

class YourPlatformWarmer(BaseWarmer):
    def validate_access(self): ...
    def create_activity(self, scenario_config): ...
    def verify_activity(self, activity_id): ...

WARMER_ID = "your-platform"
WARMER_CLASS = YourPlatformWarmer
```

Auto-registers via `synthetic/warmers/__init__.py`. Run: `python -m synthetic.warm --platform your-platform --contacts 5`

## Optional: Add vendor knowledge

Create `src/connectors/your_platform/VENDOR.md` — rate limits, known quirks, sandbox setup, PII fields. See `_template/VENDOR.md`.
