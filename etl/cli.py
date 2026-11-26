"""Command line: etl (menu) | menu | init-db | run | report | runs | compare | verify."""
from __future__ import annotations

import argparse
import sys

import psycopg

from etl import config


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="etl",
        description="Multi-pipeline ETL and reporting tool for the NASA HTTP web server logs. "
                    "Run `etl` with no arguments to choose a pipeline from a menu.")
    sub = p.add_subparsers(dest="command")

    sub.add_parser("menu", help="choose a pipeline from a menu (also what plain `etl` does)")

    i = sub.add_parser("init-db", help="create the PostgreSQL tables (safe to repeat)")
    i.add_argument("--reset", action="store_true",
                   help="drop and recreate the tables, deleting every stored run")

    r = sub.add_parser("run", help="run the full ETL flow with one pipeline")
    r.add_argument("--pipeline", required=True, choices=config.PIPELINE_NAMES)
    r.add_argument("--batch-size", required=True, type=int, metavar="N",
                   help="number of input log records per batch")
    r.add_argument("--input", nargs="+", metavar="FILE",
                   help="input files in read order (default: the two NASA files in data/)")
    r.add_argument("--keep-work", action="store_true",
                   help="keep the per-batch files in work/<run_id> after the run")

    rep = sub.add_parser("report", help="show a run's results and execution metadata")
    which = rep.add_mutually_exclusive_group(required=True)
    which.add_argument("--run", metavar="RUN_ID")
    which.add_argument("--latest", action="store_true", help="the most recent successful run")
    rep.add_argument("--rows", type=int, default=25, metavar="N",
                     help="rows shown per query; 0 shows all (default 25)")
    rep.add_argument("--batch", type=int, default=0, metavar="K",
                     help="show results of batch K only (default 0: the whole run)")

    ls = sub.add_parser("runs", help="list recent runs")
    ls.add_argument("--limit", type=int, default=20)

    c = sub.add_parser("compare", help="check that two runs give identical results")
    c.add_argument("--runs", nargs=2, required=True, metavar=("RUN_A", "RUN_B"))

    v = sub.add_parser("verify", help="prove every batch is an unchanged slice of the raw files")
    v.add_argument("--run", required=True, metavar="RUN_ID")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command in (None, "menu"):
        from etl import menu
        return menu.run_menu()
    try:
        if args.command == "init-db":
            from etl import db
            db.init_schema(reset=args.reset)
            print("Tables recreated (all runs deleted)." if args.reset else "Tables ready.")
        elif args.command == "run":
            from etl import controller
            controller.run(args.pipeline, args.batch_size, inputs=args.input, keep_work=args.keep_work)
        elif args.command == "report":
            from etl import report
            print(report.report(args.run, latest=args.latest, max_rows=args.rows, batch=args.batch))
        elif args.command == "runs":
            from etl import db, report
            with db.connect() as conn:
                print(report.list_runs(conn, args.limit))
        elif args.command == "compare":
            from etl import compare
            identical, text = compare.compare(*args.runs)
            print(text)
            return 0 if identical else 1
        elif args.command == "verify":
            from etl import verify
            ok, text = verify.verify(args.run)
            print(text)
            return 0 if ok else 1
    except (FileNotFoundError, ValueError, NotImplementedError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except psycopg.OperationalError as exc:
        print(f"error: cannot reach PostgreSQL ({config.pg_dsn()}). Start it with "
              f"scripts/dev_postgres.sh start.\n{exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
