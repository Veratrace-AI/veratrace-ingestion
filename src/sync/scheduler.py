"""
Sync scheduler — orchestrates incremental syncs for all active integration accounts.

Called by cron every 15 minutes during business hours.
For each active account: fetch cursor → sync incremental → write signals → save cursor → trigger compiler.

Usage:
  python3 -m src.sync.scheduler                    # sync all active accounts
  python3 -m src.sync.scheduler --account ACCT_ID  # sync one account
  python3 -m src.sync.scheduler --backfill ACCT_ID # backfill one account
  python3 -m src.sync.scheduler --diagnose FILE    # validate_credentials() only on account dict in FILE
"""
import base64
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from src.config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, CONTROL_PLANE_URL
from src.connectors import CONNECTOR_MAP
from src.runtime.cursor_manager import get_cursor, save_cursor
from src.runtime.signal_writer import write_signals
from src.runtime.task_trigger import trigger_compilation

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("sync")
logger.info("Registered connectors: %s", list(CONNECTOR_MAP.keys()))


# Sync outcome statuses — emitted via _logfmt and grepped by the contract test.
# Constants prevent silent typo regressions.
STATUS_OK = "ok"
STATUS_SKIPPED_NO_CONNECTOR = "skipped_no_connector"
STATUS_INVALID_CREDENTIALS = "invalid_credentials"
STATUS_NO_NEW_SIGNALS = "no_new_signals"
STATUS_ERROR = "error"


class ControlPlaneFetchError(Exception):
    """Distinguishes 'auth failed / API down' from 'instance has zero accounts'.

    Avoids the silent-failure cascade pattern (feedback_silent_failure_cascade.md):
    if `fetch_active_accounts_via_control_plane` returned `[]` on auth failure, callers
    couldn't tell empty-by-config from broken. Raising forces the call site to handle.
    """


def _logfmt(event, **fields):
    """Emit a single logfmt-style line: event=foo key=val key2="val with spaces"."""
    parts = [f"event={event}"]
    for k, v in fields.items():
        if v is None:
            continue
        s = str(v)
        if any(c in s for c in (" ", "=", '"')):
            s = '"' + s.replace('\\', '\\\\').replace('"', '\\"') + '"'
        parts.append(f"{k}={s}")
    return " ".join(parts)


def _supabase_headers():
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }


def fetch_active_accounts(instance_id=None):
    """Fetch all ACTIVE integration accounts from the control plane DB."""
    url = f"{SUPABASE_URL}/rest/v1/integration_accounts?status=eq.ACTIVE&select=*"
    if instance_id:
        url += f"&instance_id=eq.{instance_id}"

    req = urllib.request.Request(url, headers=_supabase_headers())
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except Exception as e:
        logger.error("Failed to fetch integration accounts: %s", e)
        return []


# Dormant: Joey's Spring Security rejects M2M tokens (verified 2026-04-16).
_m2m_token_cache = {"token": None, "expires_at": 0.0}


def _get_m2m_token():
    """Cognito client_credentials token, cached until ~60s before expiry. Returns None on failure."""
    now = time.time()
    if _m2m_token_cache["token"] and _m2m_token_cache["expires_at"] > now + 60:
        return _m2m_token_cache["token"]

    client_id = os.environ.get("M2M_CLIENT_ID", "")
    client_secret = os.environ.get("M2M_CLIENT_SECRET", "")
    endpoint = os.environ.get("M2M_TOKEN_ENDPOINT", "")
    scope = os.environ.get("M2M_SCOPE", "")

    if not all([client_id, client_secret, endpoint, scope]):
        logger.error(_logfmt(
            "m2m_token_unavailable",
            reason="env_not_configured",
            missing=",".join(k for k, v in [
                ("M2M_CLIENT_ID", client_id),
                ("M2M_CLIENT_SECRET", client_secret),
                ("M2M_TOKEN_ENDPOINT", endpoint),
                ("M2M_SCOPE", scope),
            ] if not v),
        ))
        return None

    auth = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    body = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "scope": scope,
    }).encode()
    req = urllib.request.Request(
        endpoint,
        data=body,
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        token = data["access_token"]
        expires_in = int(data.get("expires_in", 3600))
        _m2m_token_cache["token"] = token
        _m2m_token_cache["expires_at"] = now + expires_in
        logger.info(_logfmt("m2m_token_minted", expires_in=expires_in, scope=scope))
        return token
    except urllib.error.HTTPError as e:
        # Cognito's body distinguishes invalid_client / invalid_scope / invalid_grant.
        # Without it, "HTTP Error 400: Bad Request" can't tell misconfig from outage.
        body = e.read()[:300].decode("utf-8", "replace") if e.fp else ""
        logger.error(_logfmt("m2m_token_fetch_failed", status=e.code, body=body))
        return None
    except (urllib.error.URLError, KeyError, ValueError) as e:
        logger.error(_logfmt("m2m_token_fetch_failed", error=str(e)[:200]))
        return None


def fetch_active_accounts_via_control_plane(instance_id):
    """Fetch integration accounts from Joey's control plane API for one instance.

    Returns the API's camelCase shape; caller must normalize to snake_case before
    passing to sync_account(). Raises ControlPlaneFetchError when auth or HTTP fails —
    NEVER returns [] on a failure path (that would resurrect the silent-cascade pattern).
    An empty list means the instance genuinely has no accounts.
    """
    token = _get_m2m_token()
    if not token:
        raise ControlPlaneFetchError("M2M token unavailable; see prior log")

    url = f"{CONTROL_PLANE_URL}/instances/{instance_id}/integration-accounts"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read()[:300].decode("utf-8", "replace") if e.fp else ""
        logger.error(_logfmt(
            "control_plane_fetch_failed",
            instance_id=(instance_id or "")[:8],
            status=e.code,
            body=body,
        ))
        raise ControlPlaneFetchError(f"HTTP {e.code} from control plane: {body[:100]}") from e
    except (urllib.error.URLError, ValueError) as e:
        logger.error(_logfmt(
            "control_plane_fetch_failed",
            instance_id=(instance_id or "")[:8],
            error=str(e)[:200],
        ))
        raise ControlPlaneFetchError(str(e)) from e


def _parse_account(account):
    """Normalize an account dict from either Supabase (JSONB strings) or the control plane.

    Returns a dict with integration_id, account_id, instance_id, credentials, external_identity.
    """
    credentials = account.get("auth_credentials", {}) or {}
    external_identity = account.get("external_identity", {}) or {}
    if isinstance(credentials, str):
        credentials = json.loads(credentials)
    if isinstance(external_identity, str):
        external_identity = json.loads(external_identity)
    return {
        "integration_id": account.get("integration_id", ""),
        "account_id": account.get("integration_account_id", ""),
        "instance_id": account.get("instance_id", ""),
        "credentials": credentials,
        "external_identity": external_identity,
    }


def sync_account(account, backfill=False):
    """Run a sync for a single integration account."""
    parsed = _parse_account(account)
    integration_id = parsed["integration_id"]
    account_id = parsed["account_id"]
    instance_id = parsed["instance_id"]
    credentials = parsed["credentials"]
    external_identity = parsed["external_identity"]

    started_at = time.time()
    signals_written = 0
    status = STATUS_OK
    error = None

    logger.info(_logfmt(
        "sync_account_start",
        account_id=account_id[:8] or "?",
        integration_id=integration_id or "?",
        instance_id=instance_id[:8] or "?",
        backfill=backfill,
    ))

    try:
        connector_cls = CONNECTOR_MAP.get(integration_id)
        if not connector_cls:
            logger.warning("No connector for integration_id=%s, skipping", integration_id)
            status = STATUS_SKIPPED_NO_CONNECTOR
            return

        logger.info("Instantiating %s connector for account %s (cred_keys=%s, identity_keys=%s)",
                    integration_id, account_id[:8], list(credentials.keys()), list(external_identity.keys()))

        connector = connector_cls(
            instance_id=instance_id,
            integration_account_id=account_id,
            credentials=credentials,
            external_identity=external_identity,
        )

        if not connector.validate_credentials():
            logger.error("Invalid credentials for account %s (keys present: %s), skipping",
                         account_id[:8], list(credentials.keys()))
            status = STATUS_INVALID_CREDENTIALS
            return

        stream = f"{integration_id}:contacts"

        if backfill:
            logger.info("Starting backfill for %s (account=%s)", integration_id, account_id[:8])
            result = connector.sync_backfill()
        else:
            cursor = get_cursor(account_id, stream)
            logger.info(
                "Incremental sync for %s (account=%s, cursor=%s)",
                integration_id, account_id[:8], cursor[:20] if cursor else "none",
            )
            result = connector.sync_incremental(cursor)

        if not result.signals:
            logger.info("No new signals (account=%s)", account_id[:8])
            status = STATUS_NO_NEW_SIGNALS
            return

        signals_written = write_signals(result.signals)
        logger.info("Wrote %d signals (account=%s)", signals_written, account_id[:8])

        # Save cursor AFTER successful write
        if result.cursor:
            save_cursor(account_id, stream, result.cursor, records_synced=signals_written)

        task = trigger_compilation(instance_id, [account_id])
        if task:
            logger.info("Triggered compiler task %s", task.get("taskId", "?")[:8])
        else:
            logger.warning("Compiler task trigger failed (non-fatal)")

    except Exception as e:
        status = STATUS_ERROR
        error = str(e)[:200]
        raise
    finally:
        logger.info(_logfmt(
            "sync_account_end",
            account_id=account_id[:8] or "?",
            integration_id=integration_id or "?",
            status=status,
            signals_written=signals_written,
            duration_ms=int((time.time() - started_at) * 1000),
            error=error,
        ))


def diagnose_account(account):
    """Validate credentials only — no sync, no writes. Returns True if valid."""
    parsed = _parse_account(account)
    integration_id = parsed["integration_id"]
    account_id = parsed["account_id"] or "diagnose"
    instance_id = parsed["instance_id"] or "diagnose"
    credentials = parsed["credentials"]
    external_identity = parsed["external_identity"]

    connector_cls = CONNECTOR_MAP.get(integration_id)
    if not connector_cls:
        logger.error(_logfmt("diagnose_result", account_id=account_id[:8], integration_id=integration_id, valid=False, reason="no_connector"))
        return False

    connector = connector_cls(
        instance_id=instance_id,
        integration_account_id=account_id,
        credentials=credentials,
        external_identity=external_identity,
    )

    valid = connector.validate_credentials()
    logger.info(_logfmt(
        "diagnose_result",
        account_id=account_id[:8],
        integration_id=integration_id,
        valid=valid,
        cred_keys=",".join(sorted(credentials.keys())),
    ))
    return valid


def run_all(backfill=False):
    """Sync all active integration accounts."""
    accounts = fetch_active_accounts()
    logger.info("Found %d active integration accounts", len(accounts))

    for account in accounts:
        try:
            sync_account(account, backfill=backfill)
        except Exception as e:
            logger.error(
                "Sync failed for account %s: %s",
                account.get("integration_account_id", "?")[:8],
                str(e)[:200],
            )


if __name__ == "__main__":
    args = sys.argv[1:]

    if "--diagnose" in args:
        idx = args.index("--diagnose")
        if idx + 1 >= len(args):
            logger.error("--diagnose requires a path to an account-dict JSON file")
            sys.exit(2)
        path = args[idx + 1]
        try:
            with open(path) as f:
                acct = json.load(f)
        except FileNotFoundError:
            logger.error("--diagnose: file not found: %s", path)
            sys.exit(2)
        except json.JSONDecodeError as e:
            logger.error("--diagnose: %s is not valid JSON: %s", path, e)
            sys.exit(2)
        sys.exit(0 if diagnose_account(acct) else 1)
    elif "--backfill" in args:
        idx = args.index("--backfill")
        if idx + 1 < len(args):
            acct_id = args[idx + 1]
            accounts = fetch_active_accounts()
            acct = next((a for a in accounts if a.get("integration_account_id") == acct_id), None)
            if acct:
                sync_account(acct, backfill=True)
            else:
                logger.error("Account not found: %s", acct_id)
        else:
            logger.error("--backfill requires an account ID")
    elif "--account" in args:
        idx = args.index("--account")
        if idx + 1 < len(args):
            acct_id = args[idx + 1]
            accounts = fetch_active_accounts()
            acct = next((a for a in accounts if a.get("integration_account_id") == acct_id), None)
            if acct:
                sync_account(acct)
            else:
                logger.error("Account not found: %s", acct_id)
        else:
            logger.error("--account requires an account ID")
    else:
        run_all()
