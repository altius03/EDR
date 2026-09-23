import json
from pathlib import Path
from types import SimpleNamespace

from tools import export_observability_metrics as exporter


def test_notion_panels_are_first_and_queries_use_real_sources() -> None:
    path = Path(__file__).parents[1] / "deploy/observability/local/dashboards/edr-local.json"
    panels = json.loads(path.read_text())["panels"]
    rows = [panel["title"] for panel in panels if panel["type"] == "row"]
    assert rows == [
        "핵심 지표",
        "Kafka 처리 현황",
        "ClickHouse·Worker 상태",
        "서비스 운영 지표",
        "미계측 지표",
    ]
    assert [panel["title"] for panel in panels[1:5]] == [
        "Storage Worker Consumer Lag",
        "Storage Worker Offset Commit Rate",
        "ClickHouse 저장 이벤트 수",
        "PostgreSQL Alert 수",
    ]
    assert [panel["targets"][0]["expr"] for panel in panels[1:5]] == [
        'sum(kminion_kafka_consumer_group_topic_lag{group_id="edr-event-storage-v1",topic_name="telemetry.raw"})',
        'sum(rate(kminion_kafka_consumer_group_offset_commits_total{group_id="edr-event-storage-v1"}[5m]))',
        "edr_events_stored_current",
        "edr_alerts_current",
    ]
    assert all("Notion" not in panel["title"] for panel in panels)
    assert "owlby_events_stored_total" not in " ".join(
        target.get("expr", "") for panel in panels for target in panel.get("targets", [])
    )


def test_snapshot_metrics_are_gauges_and_missing_rollup_is_not_zero(monkeypatch) -> None:
    class FakePostgres:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def execute(self, sql):
            if "FROM alerts" in sql:
                rows = [(8, 3)]
            elif "FROM endpoints" in sql:
                rows = [("ONLINE", 2)]
            elif "FROM incidents" in sql:
                rows = [(1,)]
            elif "FROM dashboard_rollup_state" in sql:
                rows = [(None,)]
            else:
                rows = [("HOT", 2)]
            return SimpleNamespace(fetchone=lambda: rows[0], fetchall=lambda: rows)

    class FakeClickHouse:
        def query(self, sql):
            rows = [(10, 4)] if "FROM edr_events" in sql else [("FAILED", 1)]
            return SimpleNamespace(result_rows=rows)

        def close(self):
            pass

    monkeypatch.setenv("EDR_POSTGRES_DSN", "postgresql://unused")
    monkeypatch.setenv("EDR_CLICKHOUSE_DSN", "http://unused")
    monkeypatch.setattr(exporter.psycopg, "connect", lambda *_args, **_kwargs: FakePostgres())
    monkeypatch.setattr(exporter.clickhouse_connect, "get_client", lambda **_kwargs: FakeClickHouse())

    metrics = exporter.collect()
    assert "# TYPE edr_events_stored_current gauge\n" in metrics
    assert "edr_events_stored_current 10\n" in metrics
    assert "edr_alerts_current 8\n" in metrics
    assert 'edr_failures_current{status="FAILED"} 1\n' in metrics
    assert "edr_rollup_freshness_seconds 0" not in metrics
