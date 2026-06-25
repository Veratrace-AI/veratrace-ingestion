#!/usr/bin/env bash
# Refresh Salesforce access token via SF CLI and push to DO server
# Run locally on Mac — SF CLI handles token refresh internally
set -euo pipefail

# cron strips PATH, so `sf` (installed via nvm) isn't found. Explicitly add it.
export PATH="/Users/vincentgraham/.nvm/versions/node/v22.22.2/bin:/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin"

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
echo "[$(ts)] Starting SF token refresh"

# sf writes update-available warnings to stderr; those break JSON parsing if
# captured. Keep stdout (JSON) and stderr (warnings) separate.
SF_STDERR=$(mktemp)
SF_OUTPUT=$(sf org display --target-org sf-sandbox --json 2>"$SF_STDERR")
SF_EXIT=$?

TOKEN=$(printf "%s" "$SF_OUTPUT" | python3 -c "
import json, sys
try:
    print(json.load(sys.stdin)['result']['accessToken'])
except Exception as e:
    sys.stderr.write(f'parse error: {e}\n')
    sys.exit(1)
" 2>>"$SF_STDERR")

if [ -z "$TOKEN" ]; then
  echo "[$(ts)] ERROR: SF CLI returned no token (exit=$SF_EXIT). Stderr:" >&2
  cat "$SF_STDERR" >&2
  echo "[$(ts)] Stdout (first 200 chars):" >&2
  echo "$SF_OUTPUT" | head -c 200 >&2
  echo >&2
  rm -f "$SF_STDERR"
  exit 1
fi
rm -f "$SF_STDERR"

# Update on server
ssh -o ConnectTimeout=10 vera@159.203.133.76 "sed -i 's|^SF_ACCESS_TOKEN=.*|SF_ACCESS_TOKEN=${TOKEN}|' ~/veratrace-ingestion/.env && echo 'SF token refreshed on server'"

# Update GHA secret (so deploys get the fresh token too)
echo -n "$TOKEN" | gh secret set SF_ACCESS_TOKEN --repo Veratrace-AI/veraagents 2>/dev/null && echo "GHA secret updated" || echo "GHA update skipped"
