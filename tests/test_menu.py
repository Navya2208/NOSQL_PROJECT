"""The interactive menu (`etl` with no arguments), with scripted answers and a fake run."""
from etl import menu


def drive(answers, monkeypatch, env=None, run_error=None, pig_missing=False):
    for key in ("HADOOP_HOME", "ETL_HADOOP_STREAMING_JAR"):
        monkeypatch.delenv(key, raising=False)
    for key, value in (env or {}).items():
        monkeypatch.setenv(key, value)
    answers = iter(answers)
    printed, runs, reports = [], [], []

    def run_fn(name, batch_size, inputs=None):
        runs.append((name, batch_size, inputs))
        if run_error:
            raise run_error
        return "RUN-1"

    def report_fn(run_id, max_rows):
        reports.append((run_id, max_rows))
        return "REPORT"

    def input_fn(_prompt):
        try:
            return next(answers)
        except StopIteration:
            raise EOFError

    def load_fn(name):
        if name == "hive":
            raise NotImplementedError(name)
        if name == "pig" and pig_missing:
            raise RuntimeError("Pig not found. Install it with: scripts/dev_pig.sh install")

    code = menu.run_menu(input_fn=input_fn, print_fn=printed.append,
                         run_fn=run_fn, report_fn=report_fn, load_fn=load_fn)
    return code, "\n".join(printed), runs, reports


def test_defaults_full_data_then_report(monkeypatch):
    code, out, runs, reports = drive(["1", "", "", "", ""], monkeypatch)
    assert code == 0
    assert runs == [("mongodb", 500_000, None)]
    assert reports == [("RUN-1", 10)] and "REPORT" in out
    assert "1. MongoDB" in out and "3. Pig" in out and "4. Hive" in out


def test_golden_sample_and_custom_batch_size(monkeypatch):
    code, out, runs, _ = drive(["1", "1,000", "2", "n", "n"], monkeypatch)
    assert runs == [("mongodb", 1000, [menu.GOLDEN_SAMPLE])]


def test_bad_answers_are_asked_again(monkeypatch):
    code, out, runs, _ = drive(["9", "x", "1", "0", "1", "50", "3", "1", "50", "1", "n", "n"], monkeypatch)
    assert "Please type a number from 1 to 4." in out
    assert "at least 1" in out and "Please type 1 or 2." in out
    assert runs == [("mongodb", 50, None)]


def test_unbuilt_pipeline_says_phase_2_at_once(monkeypatch):
    code, out, runs, _ = drive(["4", "q"], monkeypatch)
    assert "Hive is not built yet" in out
    assert runs == [] and "Bye." in out and code == 0


def test_mapreduce_needs_hadoop_settings(monkeypatch):
    code, out, runs, _ = drive(["2", "q"], monkeypatch)
    assert "source scripts/mapreduce_env.sh" in out and runs == []
    code, out, runs, _ = drive(["2", "", "2", "n", "n"], monkeypatch, env={"HADOOP_HOME": "/opt/hadoop"})
    assert runs == [("mapreduce", 500_000, [menu.GOLDEN_SAMPLE])]


def test_failed_run_is_reported_not_crashing(monkeypatch):
    code, out, runs, _ = drive(["1", "", "", "q"], monkeypatch, run_error=RuntimeError("boom"))
    assert "The run failed: RuntimeError: boom" in out and code == 0


def test_quit_and_end_of_input(monkeypatch):
    assert drive(["q"], monkeypatch)[0] == 0
    assert drive([], monkeypatch)[0] == 0   # EOF counts as quit


def test_engine_not_installed_is_explained(monkeypatch):
    code, out, runs, _ = drive(["3", "q"], monkeypatch, pig_missing=True)
    assert "scripts/dev_pig.sh install" in out and runs == []
    code, out, runs, _ = drive(["3", "", "2", "n", "n"], monkeypatch)
    assert runs == [("pig", 500_000, [menu.GOLDEN_SAMPLE])]
