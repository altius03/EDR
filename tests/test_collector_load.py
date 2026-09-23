import json
from types import SimpleNamespace
from uuid import UUID

import pytest

from tools import collector_load as load
from tools.collector_load import _local_dsn, make_manifest, read_manifest, summarize


def test_manifest_and_reconciliation_are_scoped_to_exact_event_ids(tmp_path) -> None:
    manifest = make_manifest(2, "edr-load-agent")
    assert len({UUID(event["eventId"]) for event in manifest["events"]}) == 2
    path = tmp_path / "run.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    assert read_manifest(path) == manifest

    first, second = (event["eventId"] for event in manifest["events"])
    incomplete = summarize(manifest, ({first, second}, {first, second}, {first}))
    assert incomplete["status"] == "INCOMPLETE"
    assert incomplete["gapCounts"]["storedWithoutAlert"] == 1
    assert incomplete["gapExamples"]["storedWithoutAlert"] == [second]

    complete = summarize(manifest, ({first, second}, {first, second}, {first, second}))
    assert complete["status"] == "COMPLETE"


def test_manifest_rejects_duplicate_ids_and_remote_database(tmp_path) -> None:
    manifest = make_manifest(2, "edr-load-agent")
    manifest["events"][1]["eventId"] = manifest["events"][0]["eventId"]
    path = tmp_path / "run.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate IDs"):
        read_manifest(path)
    with pytest.raises(ValueError, match="localhost"):
        _local_dsn("postgresql://edr:secret@example.com:5432/edr", schemes={"postgresql"})


def test_query_uses_uuid_strings_for_clickhouse_and_read_only_postgres(monkeypatch) -> None:
    manifest = make_manifest(1, "edr-load-agent")
    event_id = manifest["events"][0]["eventId"]

    class FakePostgres:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def execute(self, _sql, _parameters):
            return SimpleNamespace(fetchall=lambda: [(UUID(event_id),)])

    class FakeClickHouse:
        def query(self, _sql, parameters):
            assert parameters == {"agent_id": "edr-load-agent", "ids": [event_id]}
            return SimpleNamespace(result_rows=[(event_id,)])

        def close(self):
            pass

    def secret(value):
        return SimpleNamespace(get_secret_value=lambda: value)

    def fake_connect(*_args, **kwargs):
        assert kwargs["options"] == "-c default_transaction_read_only=on"
        return FakePostgres()

    monkeypatch.setattr(
        load,
        "get_settings",
        lambda: SimpleNamespace(
            postgres_dsn=secret("postgresql://edr:secret@127.0.0.1:55432/edr"),
            clickhouse_dsn=secret("http://edr:secret@127.0.0.1:58123/edr"),
        ),
    )
    monkeypatch.setattr(load.psycopg, "connect", fake_connect)
    monkeypatch.setattr(load.clickhouse_connect, "get_client", lambda **_kwargs: FakeClickHouse())

    assert load.query_ids(manifest) == ({event_id}, {event_id}, {event_id})
