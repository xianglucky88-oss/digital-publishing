import argparse
import json

from .application import (
    run_fixture,
    run_onix_correct,
    run_onix_export,
    run_onix_import,
    run_onix_plan,
    run_url,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run JSON ingestion or ONIX 3.0 message workflows")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--fixture", help="local JSON list fixture")
    source.add_argument("--url", help="HTTP(S) JSON endpoint")
    source.add_argument("--onix-import", metavar="PATH",
                        help="import an ONIX 3.0 reference-name XML message")
    source.add_argument("--onix-plan", metavar="MESSAGE_KEY",
                        help="list correction candidates for a field")
    source.add_argument("--onix-correct", metavar="MESSAGE_KEY",
                        help="apply a single-field correction")
    source.add_argument("--onix-export", metavar="MESSAGE_KEY",
                        help="export the current message version")
    parser.add_argument("--database", required=True)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--raw-dir", help="directory for immutable raw HTTP responses")
    parser.add_argument("--records-key", help="dot-separated JSON key containing the record list")
    parser.add_argument("--message-key", help="override message identity for --onix-import")
    parser.add_argument("--product-ref", help="RecordReference of the target product")
    parser.add_argument("--product-path", help="element path of the target product")
    parser.add_argument("--field", choices=["title", "publisher", "language"],
                        help="correction field")
    parser.add_argument("--value", help="new value for --onix-correct")
    parser.add_argument("--base-fingerprint",
                        help="fingerprint of the message version the correction was prepared against")
    parser.add_argument("--node-path",
                        help="explicit candidate node path when roles are ambiguous")
    parser.add_argument("--rationale", default="", help="订正依据，随订正记录保存")
    parser.add_argument("--output", help="output path for --onix-export")
    args = parser.parse_args()

    if args.fixture:
        result = run_fixture(args.fixture, args.database)
    elif args.url:
        result = run_url(
            args.url,
            args.database,
            timeout=args.timeout,
            raw_dir=args.raw_dir,
            records_key=args.records_key,
        )
    elif args.onix_import:
        result = run_onix_import(
            args.onix_import,
            args.database,
            message_key=args.message_key,
        )
    elif args.onix_plan:
        if not args.field:
            parser.error("--onix-plan requires --field")
        if not args.product_ref and not args.product_path:
            parser.error("--onix-plan requires --product-ref or --product-path")
        result = run_onix_plan(
            args.onix_plan,
            args.database,
            field=args.field,
            product_ref=args.product_ref,
            product_path=args.product_path,
        )
    elif args.onix_correct:
        missing = [
            name for name in ("field", "value", "base_fingerprint")
            if not getattr(args, name)
        ]
        if missing:
            parser.error(
                "--onix-correct requires "
                + ", ".join("--" + n.replace("_", "-") for n in missing)
            )
        if not args.product_ref and not args.product_path:
            parser.error("--onix-correct requires --product-ref or --product-path")
        result = run_onix_correct(
            args.onix_correct,
            args.database,
            field=args.field,
            value=args.value,
            base_fingerprint=args.base_fingerprint,
            product_ref=args.product_ref,
            product_path=args.product_path,
            node_path=args.node_path,
            rationale=args.rationale,
        )
    else:
        if not args.output:
            parser.error("--onix-export requires --output")
        result = run_onix_export(args.onix_export, args.database, args.output)

    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    if result.get("status") in frozenset({"ambiguous", "failed", "conflict"}):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
