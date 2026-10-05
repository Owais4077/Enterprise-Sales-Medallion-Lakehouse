import json
from datetime import UTC, datetime

import pytest

from edp.ingestion.config import FileDatasetConfig
from edp.ingestion.errors import ConfigurationError, InvalidSourceFile
from edp.ingestion.files import FileExtractor, validate_csv, validate_json
from edp.ingestion.runner import IngestionRunner
from edp.ingestion.state import SqliteStateStore

CSV_CONFIG = FileDatasetConfig("countries", "countries", "*.csv", "csv", ("code", "name"))
JSON_CONFIG = FileDatasetConfig("reviews", "reviews", "reviews_*.json", "json")


@pytest.fixture
def raw(tmp_path):
    root = tmp_path / "raw"
    (root / "countries").mkdir(parents=True)
    (root / "reviews").mkdir()
    return root


@pytest.fixture
def make_runner(tmp_path):
    state = SqliteStateStore(tmp_path / "state.db")
    runner = IngestionRunner(
        state=state, landing_root=tmp_path / "landing", clock=lambda: datetime.now(UTC)
    )
    return runner, state


def write_reviews(raw, name, n):
    path = raw / "reviews" / name
    path.write_text(json.dumps([{"review_id": f"{name}-{i}"} for i in range(n)]))
    return path


def test_lands_new_files_unchanged_with_record_counts(raw, make_runner):
    runner, _ = make_runner
    source = write_reviews(raw, "reviews_2025-01.json", 3)
    write_reviews(raw, "reviews_2025-02.json", 2)

    result = runner.run(FileExtractor(dataset=JSON_CONFIG, root=raw))

    assert (result.records_extracted, result.records_processed) == (5, 5)
    landed = result.landing_path / "reviews_2025-01.json"
    assert landed.read_bytes() == source.read_bytes()
    manifest = json.loads((result.landing_path / "_manifest.json").read_text())
    assert [f["file_name"] for f in manifest["source_files"]] == [
        "reviews_2025-01.json",
        "reviews_2025-02.json",
    ]


def test_only_new_files_are_landed_on_the_next_run(raw, make_runner):
    runner, state = make_runner
    write_reviews(raw, "reviews_2025-01.json", 3)
    runner.run(FileExtractor(dataset=JSON_CONFIG, root=raw))

    write_reviews(raw, "reviews_2025-02.json", 4)
    second = runner.run(FileExtractor(dataset=JSON_CONFIG, root=raw))
    assert second.records_extracted == 4
    assert [f.name for f in second.landing_path.glob("reviews_*.json")] == ["reviews_2025-02.json"]

    idle = runner.run(FileExtractor(dataset=JSON_CONFIG, root=raw))
    assert idle.records_extracted == 0 and idle.landing_path is None


def test_file_with_changed_content_is_landed_again(raw, make_runner):
    runner, _ = make_runner
    write_reviews(raw, "reviews_2025-01.json", 3)
    runner.run(FileExtractor(dataset=JSON_CONFIG, root=raw))
    write_reviews(raw, "reviews_2025-01.json", 5)  # same name, new content
    again = runner.run(FileExtractor(dataset=JSON_CONFIG, root=raw))
    assert again.records_extracted == 5


def test_full_refresh_relands_everything(raw, make_runner):
    runner, _ = make_runner
    write_reviews(raw, "reviews_2025-01.json", 3)
    runner.run(FileExtractor(dataset=JSON_CONFIG, root=raw))
    again = runner.run(FileExtractor(dataset=JSON_CONFIG, root=raw), full_refresh=True)
    assert again.records_extracted == 3


def test_malformed_file_is_rejected_but_good_files_still_land(raw, make_runner):
    runner, state = make_runner
    write_reviews(raw, "reviews_2025-01.json", 3)
    (raw / "reviews" / "reviews_2025-02.json").write_text("{not json")

    result = runner.run(FileExtractor(dataset=JSON_CONFIG, root=raw))

    assert result.status.value == "partial"
    assert result.records_extracted == 3
    assert [r.file_name for r in result.rejected_files] == ["reviews_2025-02.json"]
    # Not marked processed, so it is reported again until someone fixes it.
    second = runner.run(FileExtractor(dataset=JSON_CONFIG, root=raw))
    assert second.status.value == "partial" and second.records_extracted == 0


def test_missing_directory_is_a_loud_error(tmp_path, make_runner):
    runner, state = make_runner
    with pytest.raises(ConfigurationError, match="not found"):
        runner.run(FileExtractor(dataset=JSON_CONFIG, root=tmp_path / "nowhere"))
    assert state.list_runs()[0]["status"] == "failed"


def test_csv_dataset_end_to_end(raw, make_runner):
    runner, _ = make_runner
    (raw / "countries" / "c.csv").write_text("code,name\nUS,United States\nDE,Germany\n")
    result = runner.run(FileExtractor(dataset=CSV_CONFIG, root=raw))
    assert result.records_extracted == 2


# --------------------------------------------------------------------------- validators


def test_validate_csv(tmp_path):
    good = tmp_path / "g.csv"
    good.write_text("﻿code,name\nUS,United States\n\nDE,Germany\n", encoding="utf-8")
    assert validate_csv(good, ("code", "name")) == 2  # BOM and blank line tolerated


@pytest.mark.parametrize(
    "content, message",
    [
        ("", "empty"),
        ("code\nUS\n", "missing required columns"),
        ("code,name\nUS\n", "expected 2"),
        ("code,name\nUS,a,b\n", "expected 2"),
    ],
)
def test_validate_csv_rejects(tmp_path, content, message):
    path = tmp_path / "bad.csv"
    path.write_text(content)
    with pytest.raises(InvalidSourceFile, match=message):
        validate_csv(path, ("code", "name"))


def test_validate_csv_rejects_non_utf8(tmp_path):
    path = tmp_path / "latin.csv"
    path.write_bytes(b"code,name\nFR,Fran\xe7ais\n")
    with pytest.raises(InvalidSourceFile, match="UTF-8"):
        validate_csv(path, ())


@pytest.mark.parametrize(
    "content, message",
    [("not json", "malformed"), ('{"a": 1}', "array"), ("[1, 2]", "object"), ("", "malformed")],
)
def test_validate_json_rejects(tmp_path, content, message):
    path = tmp_path / "bad.json"
    path.write_text(content)
    with pytest.raises(InvalidSourceFile, match=message):
        validate_json(path)


def test_validate_json_accepts_empty_array(tmp_path):
    path = tmp_path / "e.json"
    path.write_text("[]")
    assert validate_json(path) == 0
