"""Full flow against a real PostgreSQL: controller, loader, report, compare and verify."""
import psycopg
import pytest

from etl import compare, controller, report, verify
from etl.pipeline import BatchResult, QueryResults, ResultError, validate_results
from reference_pipeline import ReferencePipeline

Q3_SQL = ("SELECT log_date::text, log_hour, error_request_count, total_request_count, error_rate, "
          "distinct_error_hosts FROM q3_hourly_errors WHERE run_id = %s AND batch_id = %s ORDER BY 1, 2")


def run_reference(files, dsn, tmp_path, batch_size=4):
    return controller.run(batch_size=batch_size, inputs=files, dsn=dsn,
                          work_root=tmp_path / "work", pipeline=ReferencePipeline(), log=lambda _: None)


def test_run_stores_metadata_and_batch_provenance(sample_files, pg_dsn, tmp_path):
    run_id = run_reference(sample_files, pg_dsn, tmp_path)
    with psycopg.connect(pg_dsn) as conn:
        run = conn.execute(
            "SELECT status, total_records, parsed_records, malformed_records, batch_count, "
            "avg_batch_size, runtime_seconds FROM runs WHERE run_id = %s", (run_id,)).fetchone()
        assert run[:6] == ("succeeded", 11, 7, 4, 3, 11 / 3)
        assert run[6] > 0
        batches = conn.execute(
            "SELECT batch_id, source_file, first_line, last_line, record_count, malformed_count "
            "FROM batches WHERE run_id = %s ORDER BY batch_id", (run_id,)).fetchall()
        assert batches == [(1, str(sample_files[0]), 1, 4, 4, 0),
                           (2, str(sample_files[0]), 5, 7, 3, 2),   # no-bytes line + alyssa.p
                           (3, str(sample_files[1]), 1, 4, 4, 2)]   # bad timestamp + empty line
    assert not (tmp_path / "work" / run_id).exists()   # batch files removed after the run


def test_whole_run_and_per_batch_results_are_both_stored(sample_files, pg_dsn, tmp_path):
    run_id = run_reference(sample_files, pg_dsn, tmp_path)
    with psycopg.connect(pg_dsn) as conn:
        assert conn.execute(Q3_SQL, (run_id, 0)).fetchall() == [
            ("1995-07-01", 0, 0, 1, 0.0, 0),
            ("1995-07-01", 13, 0, 1, 0.0, 0),
            ("1995-07-02", 8, 1, 3, 1 / 3, 1),
            ("1995-08-04", 10, 1, 2, 0.5, 1)]
        assert conn.execute(Q3_SQL, (run_id, 1)).fetchall() == [
            ("1995-07-01", 0, 0, 1, 0.0, 0),
            ("1995-07-01", 13, 0, 1, 0.0, 0),
            ("1995-07-02", 8, 0, 2, 0.0, 0)]
        assert conn.execute(Q3_SQL, (run_id, 2)).fetchall() == [("1995-07-02", 8, 1, 1, 1.0, 1)]
        assert conn.execute(Q3_SQL, (run_id, 3)).fetchall() == [("1995-08-04", 10, 1, 2, 0.5, 1)]
        top = conn.execute(
            "SELECT batch_id, resource_path, request_count, distinct_host_count FROM q2_top_resources "
            "WHERE run_id = %s AND resource_path = '/index.html' ORDER BY batch_id", (run_id,)).fetchall()
        # whole run: 3 requests from 3 hosts; not the sum of per-batch distinct counts
        assert top == [(0, "/index.html", 3, 3), (1, "/index.html", 1, 1), (3, "/index.html", 2, 2)]


def test_report_shows_required_metadata(sample_files, pg_dsn, tmp_path):
    run_id = run_reference(sample_files, pg_dsn, tmp_path)
    text = report.report(run_id, dsn=pg_dsn)
    for expected in ("Pipeline", "Runtime", "Batch ID", "Batch size", "Average batch size",
                     "Q1 Daily traffic summary", "Q2 Top requested resources", "Q3 Hourly error analysis",
                     "Malformed records", "0 (whole run", "sha256"):
        assert expected in text
    assert "2 (only this batch: jul.log lines 5-7)" in report.report(run_id, batch=2, dsn=pg_dsn)
    with pytest.raises(ValueError):
        report.report(run_id, batch=9, dsn=pg_dsn)
    assert report.report(latest=True, dsn=pg_dsn).startswith(f"Run {run_id}")


def test_verify_detects_a_changed_source_file(sample_files, pg_dsn, tmp_path):
    run_id = run_reference(sample_files, pg_dsn, tmp_path)
    ok, text = verify.verify(run_id, dsn=pg_dsn)
    assert ok, text
    sample_files[1].write_bytes(sample_files[1].read_bytes().replace(b"404", b"405"))
    ok, text = verify.verify(run_id, dsn=pg_dsn)
    assert not ok and "MISMATCH" in text


def test_compare_identical_runs(sample_files, pg_dsn, tmp_path):
    a = run_reference(sample_files, pg_dsn, tmp_path)
    b = run_reference(sample_files, pg_dsn, tmp_path)
    identical, text = compare.compare(a, b, dsn=pg_dsn)
    assert identical, text


def test_compare_reports_differences(sample_files, pg_dsn, tmp_path):
    a = run_reference(sample_files, pg_dsn, tmp_path)
    b = run_reference(sample_files, pg_dsn, tmp_path, batch_size=3)
    identical, text = compare.compare(a, b, dsn=pg_dsn)
    assert not identical
    assert "batch sizes differ" in text
    assert "Q1 whole run rows" in text and "DIFFERENT" in text


class MiscountingPipeline(ReferencePipeline):
    def process_batch(self, run_id, batch_id, batch_path):
        r = super().process_batch(run_id, batch_id, batch_path)
        return BatchResult(r.record_count - 1, r.parsed_count - 1, r.malformed_count)


def test_count_mismatch_fails_run_and_is_recorded(sample_files, pg_dsn, tmp_path):
    with pytest.raises(controller.BatchMismatch):
        controller.run(batch_size=4, inputs=sample_files, dsn=pg_dsn, work_root=tmp_path / "work",
                       pipeline=MiscountingPipeline(), log=lambda _: None)
    with psycopg.connect(pg_dsn) as conn:
        status, error = conn.execute("SELECT status, error_message FROM runs").fetchone()
    assert status == "failed" and "controller sent 4 lines" in error


def test_rounded_error_rate_is_rejected():
    bad = QueryResults(q3=[("1995-07-02", 8, 1, 3, 0.3333, 1)])
    with pytest.raises(ResultError, match="unrounded"):
        validate_results(bad, "batch 2")


def test_more_than_20_q2_rows_is_rejected():
    bad = QueryResults(q2=[(f"/p{i}", 1, 0, 1) for i in range(21)])
    with pytest.raises(ResultError):
        validate_results(bad)


def test_unbuilt_pipeline_fails_before_creating_a_run(sample_files, pg_dsn, monkeypatch):
    def unbuilt(name):  # stands in for any pipeline that is not built yet
        raise NotImplementedError(f"the {name} pipeline is not built yet")

    monkeypatch.setattr(controller, "load_pipeline", unbuilt)
    with pytest.raises(NotImplementedError):
        controller.run("hive", 4, inputs=sample_files, dsn=pg_dsn)
    with psycopg.connect(pg_dsn) as conn:
        assert conn.execute("SELECT count(*) FROM runs").fetchone()[0] == 0
