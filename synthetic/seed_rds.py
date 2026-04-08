"""
Seed RDS with synthetic TWUs + signals for demo/testing.

Usage:
  python3 -m synthetic.seed_rds --instance-id UUID --integration-account-id UUID --contacts 150

Requires: AWS credentials for Secrets Manager + psycopg2
"""
import json
import uuid
import sys
import os
import random
from datetime import datetime, timedelta, timezone

import boto3
import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from synthetic.generator import generate_scenario, SCENARIOS


def get_db_credentials():
    """Fetch RDS credentials from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name="us-east-1")
    resp = client.get_secret_value(SecretId="DatabaseSecret86DBB7B3-YkIOsfQ8GOtS")
    return json.loads(resp["SecretString"])


def connect_rds():
    creds = get_db_credentials()
    return psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["dbname"],
        user=creds["username"],
        password=creds["password"],
        sslmode="require",
    )


def seed(instance_id, integration_account_id, twu_model_id, scenario="bpo_contact_center", num_contacts=150):
    """Generate contacts → group into TWUs → insert into RDS."""
    print(f"Generating {num_contacts} contacts for scenario '{scenario}'...")
    signals = generate_scenario(instance_id, integration_account_id, scenario, num_contacts)

    # Group signals by contact_id into TWUs
    contacts = {}
    for sig in signals:
        contact_id = sig.payload.get("contact_id", sig.signal_id)
        if contact_id not in contacts:
            contacts[contact_id] = []
        contacts[contact_id].append(sig)

    print(f"Grouped into {len(contacts)} TWUs")

    conn = connect_rds()
    cur = conn.cursor()
    twu_count = 0
    signal_count = 0

    for contact_id, contact_signals in contacts.items():
        twu_id = str(uuid.uuid4())

        # Derive TWU metadata from signals
        first_signal = contact_signals[0]
        last_signal = contact_signals[-1]
        channel = first_signal.payload.get("channel", "VOICE")
        resolved_by = last_signal.payload.get("resolved_by", "HUMAN")
        agent_name = ""
        for s in contact_signals:
            if s.name == "agent_connected":
                agent_name = s.payload.get("agent_name", "")
                break
            if s.name == "ai_processing" and s.payload.get("resolved_by_ai"):
                agent_name = s.payload.get("ai_agent", "AI Bot")
                break

        title = f"{channel.title()} contact — {agent_name or resolved_by}"
        description = f"Contact {contact_id[:8]}... via {channel}, handled by {resolved_by.lower()}"

        # Insert TWU
        try:
            cur.execute("""
                INSERT INTO twus (instance_id, twu_id, twu_model_id, twu_model_version,
                    title, description, status, started_at, ended_at,
                    correlation, outcomes, policy, usage, integrity, actor_attribution, explanation)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                instance_id, twu_id, twu_model_id, 1,
                title, description, "COMPLETED",
                first_signal.occurred_at, last_signal.occurred_at,
                json.dumps({"contact_id": contact_id, "channel": channel}),
                json.dumps([{"type": "RESOLUTION", "name": "contact_resolution", "value": "completed", "confidence": round(random.uniform(0.8, 0.99), 2)}]),
                json.dumps({"status": "COMPLIANT", "overallScore": round(random.uniform(0.85, 0.99), 2), "evaluations": []}),
                json.dumps({"resources": {"cpu": 0, "memory": 0, "tokens": 0}, "cost": {"amount": 0.0, "currency": "USD"}, "duration": {"totalSeconds": last_signal.payload.get("duration_seconds", 300), "activeSeconds": last_signal.payload.get("handle_time_seconds", 250)}}),
                json.dumps({"hash": uuid.uuid4().hex[:16], "verified": True, "method": "SHA-256", "verifiedAt": datetime.now(timezone.utc).isoformat(), "tamperDetected": False}),
                json.dumps({"derived": False, "primary": {"type": resolved_by.upper(), "agentId": str(uuid.uuid4()), "contributions": 1, "score": 1.0}, "secondary": []}),
                json.dumps({"text": f"{channel.title()} contact handled by {agent_name or resolved_by.lower()}."}),
            ))
            twu_count += 1
        except Exception as e:
            print(f"  TWU insert error: {e}")
            conn.rollback()
            continue

        # Insert signals for this TWU
        for sig in contact_signals:
            sig_type = "EVENT" if sig.type == "INTEGRATION_EVENT" else "ACTION"
            try:
                cur.execute("""
                    INSERT INTO twu_signals (instance_id, twu_id, signal_id,
                        type, name, occurred_at, processed_at, source, actor, payload, explanation)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    instance_id, twu_id, sig.signal_id,
                    sig_type, sig.name, sig.occurred_at, sig.processed_at,
                    json.dumps({"integration_account_id": integration_account_id, "integration": sig.source_integration}),
                    json.dumps({"type": sig.actor_type, "agent_id": sig.actor_agent_id}),
                    json.dumps(sig.payload),
                    json.dumps({}),
                ))
                signal_count += 1
            except Exception as e:
                print(f"  Signal insert error: {e}")
                conn.rollback()
                continue

        if twu_count % 50 == 0:
            conn.commit()
            print(f"  Committed {twu_count} TWUs, {signal_count} signals...")

    conn.commit()
    cur.close()
    conn.close()
    print(f"\nDone: {twu_count} TWUs, {signal_count} signals loaded into RDS")


if __name__ == "__main__":
    args = sys.argv[1:]

    instance_id = None
    integration_account_id = None
    twu_model_id = str(uuid.uuid4())
    num_contacts = 150
    scenario = "bpo_contact_center"

    if "--instance-id" in args:
        instance_id = args[args.index("--instance-id") + 1]
    if "--integration-account-id" in args:
        integration_account_id = args[args.index("--integration-account-id") + 1]
    if "--twu-model-id" in args:
        twu_model_id = args[args.index("--twu-model-id") + 1]
    if "--contacts" in args:
        num_contacts = int(args[args.index("--contacts") + 1])
    if "--scenario" in args:
        scenario = args[args.index("--scenario") + 1]

    if not instance_id:
        print("Usage: python3 -m synthetic.seed_rds --instance-id UUID [--integration-account-id UUID] [--contacts N]")
        sys.exit(1)

    if not integration_account_id:
        integration_account_id = str(uuid.uuid4())
        print(f"No integration account ID provided, using generated: {integration_account_id}")

    seed(instance_id, integration_account_id, twu_model_id, scenario, num_contacts)
