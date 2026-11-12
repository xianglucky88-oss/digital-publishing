import argparse
import json
from dataclasses import asdict

from .adjudication import Adjudication, TimelineSegment
from .application import (
    adjudicate_availability,
    availability_timeline,
    migrate_legacy_observations,
    run_fixture,
    run_url,
)


def _priority(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]


def _emit(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest observations and adjudicate availability")
    parser.add_argument("--database", required=True, help="SQLite database path")
    sub = parser.add_subparsers(dest="command")

    ingest = sub.add_parser("ingest", help="ingest fixture or HTTP JSON records")
    source = ingest.add_mutually_exclusive_group(required=True)
    source.add_argument("--fixture", help="local JSON list fixture")
    source.add_argument("--url", help="HTTP(S) JSON endpoint")
    ingest.add_argument("--timeout", type=float, default=30.0)
    ingest.add_argument("--raw-dir", help="directory for immutable raw HTTP responses")
    ingest.add_argument("--records-key", help="dot-separated JSON key containing the record list")

    judge = sub.add_parser("adjudicate", help="adjudicate availability at a cutoff moment")
    judge.add_argument("--isbn", required=True)
    judge.add_argument("--edition", required=True)
    judge.add_argument("--as-of", required=True, help="declaration cutoff moment, timezone-aware ISO 8601")
    judge.add_argument("--priority", help="comma-separated source order, earlier entries rank higher")

    line = sub.add_parser("timeline", help="list status timeline segments for a product edition")
    line.add_argument("--isbn", required=True)
    line.add_argument("--edition", required=True)
    line.add_argument("--priority", help="comma-separated source order, earlier entries rank higher")

    sub.add_parser("migrate", help="migrate legacy observations into append-only claims")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if not args.command:
        parser.error("choose one of: ingest, adjudicate, timeline, migrate")

    if args.command == "ingest":
        if args.fixture:
            result = run_fixture(args.fixture, args.database)
        else:
            result = run_url(
                args.url,
                args.database,
                timeout=args.timeout,
                raw_dir=args.raw_dir,
                records_key=args.records_key,
            )
        _emit(result)
    elif args.command == "adjudicate":
        decision: Adjudication = adjudicate_availability(
            args.database, args.isbn, args.edition, args.as_of, _priority(args.priority)
        )
        _emit(asdict(decision))
    elif args.command == "timeline":
        segments: list[TimelineSegment] = availability_timeline(
            args.database, args.isbn, args.edition, _priority(args.priority)
        )
        _emit([asdict(segment) for segment in segments])
    elif args.command == "migrate":
        _emit(migrate_legacy_observations(args.database))


if __name__ == "__main__":
    main()
