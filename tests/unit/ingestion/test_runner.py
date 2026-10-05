from datetime import UTC, datetime

import pytest

from edp.ingestion.base import ExtractionContext, Extractor
from edp.ingestion.models import ExtractionStats, RejectedFile
from edp.ingestion.runner import IngestionRunner
from edp.ingestion.state import SqliteStateStore

T0 = datetime(2026, 1, 5, 10, 0, tzinfo=UTC)


class FakeExtractor(Extractor):
    source_system, dataset = "fake", "things"

    def __init__(self, rows=(), watermark="w1", fail_with=None, rejected=()):
        self.rows, self.watermark, self.fail_with, self.rejected = (
            rows,
            watermark,
            fail_with,
            rejected,
        )
        self.seen_since: list = []
        self.closed = False

    def extract(self, ctx: ExtractionContext) -> ExtractionStats:
        self.seen_since.append(ctx.since)
        ctx.sink.write_rows(self.rows)
        if self.fail_with:
            raise self.fail_with
        return ExtractionStats(
            records_extracted=len(self.rows),
            next_watermark=self.watermark,
            rejected_files=list(self.rejected),
        )

    def close(self):
        self.closed = True


@pytest.fixture
def env(tmp_path):
    state = SqliteStateStore(tmp_path / "state.db")
    runner = IngestionRunner(state=state, landing_root=tmp_path / "landing", clock=lambda: T0)
    return runner, state, tmp_path / "landing" / "fake" / "things"


def batches(folder):
    return sorted(p.name for p in folder.glob("batch_id=*")) if folder.exists() else []


def test_success_lands_batch_advances_watermark_and_audits(env):
    runner, state, folder = env
    result = runner.run(FakeExtractor(rows=[{"id": 1}, {"id": 2}]))

    assert (result.status.value, result.records_extracted, result.records_processed) == (
        "success",
        2,
        2,
    )
    assert len(batches(folder)) == 1 and result.landing_path.exists()
    assert state.get_watermark("fake", "things") == "w1"
    audit = state.list_runs()[0]
    assert audit["status"] == "success" and audit["records_processed"] == 2
    assert audit["finished_at"] and audit["error_type"] is None


def test_second_run_reads_from_stored_watermark(env):
    runner, _, _ = env
    extractor = FakeExtractor(rows=[{"id": 1}], watermark="w1")
    runner.run(extractor)
    extractor.watermark = "w2"
    runner.run(extractor)
    assert extractor.seen_since == [None, "w1"]


def test_full_refresh_ignores_stored_watermark(env):
    runner, _, _ = env
    extractor = FakeExtractor(rows=[{"id": 1}])
    runner.run(extractor)
    runner.run(extractor, full_refresh=True)
    assert extractor.seen_since == [None, None]


def test_failure_keeps_watermark_discards_partial_batch_and_reraises(env):
    runner, state, folder = env
    runner.run(FakeExtractor(rows=[{"id": 1}], watermark="good"))
    failing = FakeExtractor(rows=[{"id": 2}], watermark="bad", fail_with=ConnectionError("db down"))

    with pytest.raises(ConnectionError, match="db down"):
        runner.run(failing)

    assert state.get_watermark("fake", "things") == "good"  # did NOT advance
    assert len(batches(folder)) == 1  # only the first batch; partial one discarded
    assert not list(folder.glob(".tmp-*"))
    failed = [r for r in state.list_runs() if r["status"] == "failed"]
    assert len(failed) == 1
    assert failed[0]["error_type"] == "ConnectionError" and failed[0]["error_message"] == "db down"
    assert failing.closed  # resources released even on failure


def test_no_new_rows_publishes_nothing_but_advances_watermark(env):
    runner, state, folder = env
    result = runner.run(FakeExtractor(rows=[], watermark="w-idle"))
    assert result.records_extracted == 0 and result.landing_path is None
    assert batches(folder) == []
    assert state.get_watermark("fake", "things") == "w-idle"
    assert state.list_runs()[0]["status"] == "success"


def test_rejected_files_make_the_run_partial_not_success(env):
    runner, state, _ = env
    result = runner.run(
        FakeExtractor(rows=[{"id": 1}], rejected=[RejectedFile("bad.json", "malformed JSON")])
    )
    assert result.status.value == "partial"
    assert result.rejected_files[0].file_name == "bad.json"
    assert state.list_runs()[0]["status"] == "partial"
    assert "bad.json" in state.list_runs()[0]["details"]


def test_bookkeeping_failure_after_publish_removes_the_batch(env, monkeypatch):
    runner, state, folder = env

    def explode(**kwargs):
        raise RuntimeError("state db unavailable")

    monkeypatch.setattr(state, "commit_extraction", explode)
    with pytest.raises(RuntimeError):
        runner.run(FakeExtractor(rows=[{"id": 1}]))
    assert batches(folder) == []  # landing and state stay consistent
    assert state.get_watermark("fake", "things") is None


def test_audit_failure_does_not_mask_the_original_error(env, monkeypatch):
    runner, state, _ = env
    original_finish = state.finish_run

    def broken_finish(**kwargs):
        raise OSError("audit store down")

    monkeypatch.setattr(state, "finish_run", broken_finish)
    with pytest.raises(ConnectionError, match="source down"):
        runner.run(FakeExtractor(rows=[{"id": 1}], fail_with=ConnectionError("source down")))
    monkeypatch.setattr(state, "finish_run", original_finish)


def test_each_run_gets_its_own_run_id_and_batch(env):
    runner, state, folder = env
    first = runner.run(FakeExtractor(rows=[{"id": 1}]))
    second = runner.run(FakeExtractor(rows=[{"id": 2}]))
    assert first.run_id != second.run_id and first.batch_id != second.batch_id
    assert len(batches(folder)) == 2 and len(state.list_runs()) == 2
