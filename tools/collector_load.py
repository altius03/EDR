"""Prepare a local k6 run and reconcile its event IDs without changing stored data."""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import clickhouse_connect
import psycopg

from backend.settings import get_settings

ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = ROOT / "runtime" / "load"
RULE_CODE = "PROC_POWERSHELL_ENCODED"
RULE_VERSION = 2
MAX_EVENTS = 10_000
AGENT_ID_RE = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}\Z")


def make_manifest(iterations: int, agent_id: str) -> dict[str, object]:
    if not 1 <= iterations <= MAX_EVENTS:
        raise ValueError(f"iterations must be between 1 and {MAX_EVENTS}")
    if AGENT_ID_RE.fullmatch(agent_id) is None:
        raise ValueError("agent ID must match [a-z0-9][a-z0-9._-]{0,63}")
    return {
        "schemaVersion": 1,
        "runId": str(uuid4()),
        "createdAt": datetime.now(UTC).isoformat(),
        "agentId": agent_id,
        "ruleCode": RULE_CODE,
        "ruleVersion": RULE_VERSION,
        "events": [{"eventId": str(uuid4()), "batchId": str(uuid4())} for _ in range(iterations)],
    }


def read_manifest(path: Path) -> dict[str, object]:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or manifest.get("schemaVersion") != 1:
        raise ValueError("invalid run manifest")
    events = manifest.get("events")
    if not isinstance(events, list) or not 1 <= len(events) <= MAX_EVENTS:
        raise ValueError("manifest event count is invalid")
    if manifest.get("ruleCode") != RULE_CODE or manifest.get("ruleVersion") != RULE_VERSION:
        raise ValueError("manifest rule does not match the active load-test rule")
    if not isinstance(manifest.get("agentId"), str) or AGENT_ID_RE.fullmatch(manifest["agentId"]) is None:
        raise ValueError("manifest agent ID is invalid")
    UUID(str(manifest["runId"]))
    event_ids = [UUID(str(event["eventId"])) for event in events]
    batch_ids = [UUID(str(event["batchId"])) for event in events]
    if len(set(event_ids)) != len(events) or len(set(batch_ids)) != len(events):
        raise ValueError("manifest contains duplicate IDs")
    return manifest


def _local_dsn(dsn: str, *, schemes: set[str]) -> str:
    parsed = urlsplit(dsn)
    if parsed.scheme not in schemes or parsed.hostname not in {"127.0.0.1", "localhost"}:
        raise ValueError("verification only supports localhost database DSNs")
    return dsn


def query_ids(manifest: dict[str, object]) -> tuple[set[str], set[str], set[str]]:
    settings = get_settings()
    postgres_dsn = _local_dsn(settings.postgres_dsn.get_secret_value(), schemes={"postgres", "postgresql"})
    clickhouse_dsn = _local_dsn(settings.clickhouse_dsn.get_secret_value(), schemes={"http", "https"})
    agent_id = str(manifest["agentId"])
    events = manifest["events"]
    registry: set[str] = set()
    stored: set[str] = set()
    matched_alerts: set[str] = set()
    clickhouse = clickhouse_connect.get_client(
        dsn=clickhouse_dsn,
        autogenerate_session_id=False,
        connect_timeout=5,
        send_receive_timeout=20,
    )
    try:
        with psycopg.connect(postgres_dsn, connect_timeout=5, options="-c default_transaction_read_only=on") as pg:
            for start in range(0, len(events), 500):
                ids = [UUID(str(event["eventId"])) for event in events[start : start + 500]]
                registry.update(
                    str(row[0])
                    for row in pg.execute(
                        "SELECT event_id FROM event_ingest_registry WHERE agent_id = %s AND event_id = ANY(%s::uuid[])",
                        (agent_id, ids),
                    ).fetchall()
                )
                stored.update(
                    str(row[0])
                    for row in clickhouse.query(
                        """SELECT DISTINCT toString(event_id) FROM edr_events FINAL
                           WHERE agent_id = {agent_id:String} AND event_id IN {ids:Array(UUID)} AND is_delete = 0""",
                        parameters={"agent_id": agent_id, "ids": [str(event_id) for event_id in ids]},
                    ).result_rows
                )
                matched_alerts.update(
                    str(row[0])
                    for row in pg.execute(
                        """SELECT event_id FROM alerts
                           WHERE agent_id = %s AND rule_code = %s AND rule_version = %s
                             AND NOT is_delete AND event_id = ANY(%s::uuid[])""",
                        (agent_id, RULE_CODE, RULE_VERSION, ids),
                    ).fetchall()
                )
    finally:
        clickhouse.close()
    return registry, stored, matched_alerts


def summarize(manifest: dict[str, object], results: tuple[set[str], set[str], set[str]]) -> dict[str, object]:
    expected = {str(event["eventId"]) for event in manifest["events"]}
    registry, stored, alerts = results
    gaps = {
        "missingRegistry": expected - registry,
        "missingStorage": expected - stored,
        "storedWithoutAlert": stored - alerts,
        "alertWithoutStorage": alerts - stored,
    }
    complete = all(not values for values in gaps.values()) and len(alerts) == len(expected)
    return {
        "runId": manifest["runId"],
        "checkedAt": datetime.now(UTC).isoformat(),
        "status": "COMPLETE" if complete else "INCOMPLETE",
        "expectedEvents": len(expected),
        "registryEvents": len(registry),
        "storedEvents": len(stored),
        "matchingAlerts": len(alerts),
        "gapCounts": {name: len(values) for name, values in gaps.items()},
        "gapExamples": {name: sorted(values)[:20] for name, values in gaps.items()},
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    prepare = subcommands.add_parser("prepare", help="Create a new local run manifest; no data is sent")
    prepare.add_argument("--iterations", type=int, default=1)
    prepare.add_argument("--agent-id", default="edr-load-agent")
    verify = subcommands.add_parser("verify", help="Read-only ClickHouse/PostgreSQL event-ID reconciliation")
    verify.add_argument("--manifest", type=Path, required=True)
    verify.add_argument("--wait-seconds", type=int, default=0)
    args = parser.parse_args(argv)
    try:
        if args.command == "prepare":
            manifest = make_manifest(args.iterations, args.agent_id)
            RUNS_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
            path = RUNS_DIR / f"{manifest['runId']}.json"
            with path.open("x", encoding="utf-8") as output:
                json.dump(manifest, output, indent=2)
                output.write("\n")
            print(json.dumps({"manifest": str(path), "runId": manifest["runId"], "iterations": args.iterations}))
            return 0
        if not 0 <= args.wait_seconds <= 300:
            raise ValueError("wait-seconds must be between 0 and 300")
        manifest = read_manifest(args.manifest)
        deadline = time.monotonic() + args.wait_seconds
        while True:
            summary = summarize(manifest, query_ids(manifest))
            if summary["status"] == "COMPLETE" or time.monotonic() >= deadline:
                break
            time.sleep(min(3, max(0, deadline - time.monotonic())))
        print(json.dumps(summary, indent=2))
        return 0 if summary["status"] == "COMPLETE" else 2
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"collector load {args.command} failed: {error}", file=sys.stderr)
        return 1
    except (psycopg.Error, clickhouse_connect.driver.exceptions.ClickHouseError) as error:
        print(f"collector load verification failed: {type(error).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
