import argparse
import json
from typing import Any

from .application import run_fixture, run_url


def main() -> None:
    parser = argparse.ArgumentParser(description="Run fixture or HTTP JSON ingestion")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--fixture", help="local JSON list fixture")
    source.add_argument("--url", help="HTTP(S) JSON endpoint")
    parser.add_argument("--database", required=True)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--raw-dir", help="directory for immutable raw HTTP responses")
    parser.add_argument("--records-key", help="dot-separated JSON key containing the record list")
    parser.add_argument("--cache-dir", help="directory for the conditional-download cache")
    parser.add_argument("--access-scope", default="", help="caller-declared access scope bound into the cache identity")
    args = parser.parse_args()
    if args.fixture:
        result = run_fixture(args.fixture, args.database)
    else:
        report: list[dict[str, Any]] = []
        result = run_url(
            args.url,
            args.database,
            timeout=args.timeout,
            raw_dir=args.raw_dir,
            records_key=args.records_key,
            cache_dir=args.cache_dir,
            access_scope=args.access_scope,
            report=report,
        )
        result["source_reads"] = report
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
