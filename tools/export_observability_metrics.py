"""Read-only, local-only snapshots for the Grafana reproduction dashboard."""

import os
from http.server import BaseHTTPRequestHandler, HTTPServer

import clickhouse_connect
import psycopg


def _sample(name: str, value: int | float, *, status: str | None = None) -> str:
    labels = "" if status is None else '{status="' + status.replace("\\", "\\\\").replace('"', '\\"') + '"}'
    return f"{name}{labels} {value}\n"


def collect() -> str:
    lines = [
        "# HELP edr_events_stored_current Distinct non-deleted ClickHouse event IDs at scrape time.\n",
        "# TYPE edr_events_stored_current gauge\n",
        "# HELP edr_events_stored_15m Distinct ClickHouse event IDs ingested in the last 15 minutes.\n",
        "# TYPE edr_events_stored_15m gauge\n",
        "# HELP edr_alerts_current Non-deleted PostgreSQL alerts at scrape time.\n",
        "# TYPE edr_alerts_current gauge\n",
        "# HELP edr_alerts_created_15m Non-deleted alerts created in the last 15 minutes.\n",
        "# TYPE edr_alerts_created_15m gauge\n",
        "# HELP edr_endpoints_current Non-deleted endpoints by status.\n",
        "# TYPE edr_endpoints_current gauge\n",
        "# HELP edr_incidents_open_current Open, non-deleted incidents.\n",
        "# TYPE edr_incidents_open_current gauge\n",
        "# HELP edr_rollup_freshness_seconds Seconds since the latest rollup refresh.\n",
        "# TYPE edr_rollup_freshness_seconds gauge\n",
        "# HELP edr_archive_buckets_current Non-deleted ingest metadata buckets by storage status.\n",
        "# TYPE edr_archive_buckets_current gauge\n",
        "# HELP edr_failures_current Latest failure records by reprocessing status.\n",
        "# TYPE edr_failures_current gauge\n",
    ]

    with psycopg.connect(os.environ["EDR_POSTGRES_DSN"], connect_timeout=5) as connection:
        alerts, recent_alerts = connection.execute(
            """SELECT count(*) FILTER (WHERE NOT is_delete),
                      count(*) FILTER (WHERE NOT is_delete AND created_at >= now() - interval '15 minutes')
               FROM alerts"""
        ).fetchone()
        lines.extend((_sample("edr_alerts_current", alerts), _sample("edr_alerts_created_15m", recent_alerts)))

        endpoints = dict(connection.execute(
            "SELECT status, count(*) FROM endpoints WHERE NOT is_delete GROUP BY status"
        ).fetchall())
        for status in ("ONLINE", "OFFLINE", "RETIRED"):
            lines.append(_sample("edr_endpoints_current", endpoints.get(status, 0), status=status))

        (open_incidents,) = connection.execute(
            "SELECT count(*) FROM incidents WHERE NOT is_delete AND status = 'OPEN'"
        ).fetchone()
        lines.append(_sample("edr_incidents_open_current", open_incidents))

        (rollup_age,) = connection.execute(
            "SELECT extract(epoch FROM now() - max(refreshed_at)) FROM dashboard_rollup_state"
        ).fetchone()
        if rollup_age is not None:
            lines.append(_sample("edr_rollup_freshness_seconds", float(rollup_age)))

        buckets = dict(connection.execute(
            "SELECT storage_status, count(*) FROM ingest_metadata WHERE NOT is_delete GROUP BY storage_status"
        ).fetchall())
        for status in ("HOT", "ARCHIVED", "RESTORE_REQUESTED", "RESTORED", "RESTORE_FAILED", "EXPIRED"):
            lines.append(_sample("edr_archive_buckets_current", buckets.get(status, 0), status=status))

    clickhouse = clickhouse_connect.get_client(
        dsn=os.environ["EDR_CLICKHOUSE_DSN"],
        autogenerate_session_id=False,
        connect_timeout=5,
        send_receive_timeout=20,
    )
    try:
        stored, recent_stored = clickhouse.query(
            """SELECT uniqExactIf(event_id, is_delete = 0),
                      uniqExactIf(event_id, is_delete = 0 AND ingested_at >= now() - INTERVAL 15 MINUTE)
               FROM edr_events FINAL"""
        ).result_rows[0]
        lines.extend((_sample("edr_events_stored_current", stored), _sample("edr_events_stored_15m", recent_stored)))

        failures = dict(clickhouse.query(
            "SELECT status, count() FROM event_failures FINAL GROUP BY status"
        ).result_rows)
        for status in ("FAILED", "REPLAY_PUBLISHED", "REPROCESSED", "REPROCESS_FAILED"):
            lines.append(_sample("edr_failures_current", failures.get(status, 0), status=status))
    finally:
        clickhouse.close()

    return "".join(lines)


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path != "/metrics":
            self.send_error(404)
            return
        try:
            payload = collect().encode("utf-8")
        except Exception as error:
            # A failed source must not be rendered as a valid zero-valued sample.
            print(f"metrics snapshot failed: {type(error).__name__}", flush=True)
            self.send_error(503, "snapshot unavailable")
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 9400), MetricsHandler).serve_forever()
