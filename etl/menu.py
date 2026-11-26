"""Interactive menu: run `etl` with no arguments (or `etl menu`).

The user picks exactly one of the four pipelines, a batch size and the input,
then the run starts and the report can be shown straight after. It calls the
same controller as `etl run`, so results are identical either way.
"""
from __future__ import annotations

import os
from typing import Callable

import psycopg

from etl import config

CHOICES = [
    ("mongodb", "MongoDB"),
    ("mapreduce", "MapReduce (Hadoop)"),
    ("pig", "Pig"),
    ("hive", "Hive"),
]
DEFAULT_BATCH_SIZE = 500_000
GOLDEN_SAMPLE = config.ROOT / "tests" / "golden" / "sample.log"


class _Quit(Exception):
    pass


def run_menu(input_fn: Callable[[str], str] = input, print_fn: Callable[[str], None] = print,
             run_fn: Callable | None = None, report_fn: Callable | None = None,
             load_fn: Callable | None = None) -> int:
    """Runs the menu until the user quits. The callables are replaceable for tests."""
    if run_fn is None:
        from etl.controller import run as run_fn
    if report_fn is None:
        from etl.report import report as report_fn
    if load_fn is None:
        from etl.pipeline import load_pipeline as load_fn

    def ask(prompt: str) -> str:
        try:
            answer = input_fn(prompt).strip()
        except EOFError:
            raise _Quit
        if answer.lower() in ("q", "quit", "exit"):
            raise _Quit
        return answer

    print_fn("")
    print_fn("Multi-Pipeline ETL and Reporting Tool")
    print_fn("NASA HTTP web server logs, July and August 1995")
    try:
        while True:
            print_fn("")
            print_fn("Choose one pipeline:")
            for i, (_, label) in enumerate(CHOICES, start=1):
                print_fn(f"  {i}. {label}")
            choice = ask(f"Pipeline [1-{len(CHOICES)}, q to quit]: ")
            if not choice.isdigit() or not 1 <= int(choice) <= len(CHOICES):
                print_fn(f"Please type a number from 1 to {len(CHOICES)}.")
                continue
            name, label = CHOICES[int(choice) - 1]

            if name != "mapreduce":  # MapReduce is checked below, after its settings
                try:
                    load_fn(name)
                except NotImplementedError:
                    print_fn(f"{label} is not built yet. It is planned for Phase 2.")
                    continue
                except RuntimeError as exc:  # e.g. the engine is not installed on this machine
                    print_fn(str(exc))
                    continue

            if name == "mapreduce" and not (os.environ.get("HADOOP_HOME")
                                            or os.environ.get("ETL_HADOOP_STREAMING_JAR")):
                print_fn("MapReduce needs the Hadoop settings. Quit, run "
                         "`source scripts/mapreduce_env.sh`, then start `etl` again.")
                continue

            size = ask(f"Batch size [{DEFAULT_BATCH_SIZE}]: ") or str(DEFAULT_BATCH_SIZE)
            if not size.replace(",", "").replace("_", "").isdigit() or int(size.replace(",", "").replace("_", "")) < 1:
                print_fn("The batch size must be a whole number of at least 1.")
                continue
            batch_size = int(size.replace(",", "").replace("_", ""))

            source = ask("Input: 1 = full NASA logs, 2 = golden sample (346 lines) [1]: ") or "1"
            if source not in ("1", "2"):
                print_fn("Please type 1 or 2.")
                continue
            inputs = None if source == "1" else [GOLDEN_SAMPLE]

            print_fn("")
            print_fn(f"Running {label}, batch size {batch_size:,}, "
                     f"{'full NASA logs' if inputs is None else 'golden sample'}...")
            try:
                run_id = run_fn(name, batch_size, inputs=inputs)
            except NotImplementedError:
                print_fn(f"{label} is not built yet. It is planned for Phase 2.")
                continue
            except FileNotFoundError as exc:
                print_fn(str(exc))
                continue
            except psycopg.OperationalError:
                print_fn("Cannot reach PostgreSQL. Start it with: scripts/dev_postgres.sh start")
                continue
            except KeyboardInterrupt:
                print_fn("Run stopped.")
                continue
            except Exception as exc:  # the controller has already recorded the failed run
                print_fn(f"The run failed: {type(exc).__name__}: {exc}")
                continue

            if ask("Show the report now? [Y/n]: ").lower() not in ("n", "no"):
                print_fn("")
                print_fn(report_fn(run_id, max_rows=10))
            if ask("Run another pipeline? [y/N]: ").lower() not in ("y", "yes"):
                return 0
    except (_Quit, KeyboardInterrupt):
        print_fn("")
        print_fn("Bye.")
        return 0
