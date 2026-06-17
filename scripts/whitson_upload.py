#!/usr/bin/env python3
"""
Upload production from PCE_Production to Whitson+.

  python scripts/whitson_upload.py --well-name "2-01-85-26W6M - T3 - PnP - S"
  python scripts/whitson_upload.py --all-wells
  python scripts/whitson_upload.py --all-wells --start 2024-01-01 --end 2024-12-31

Credentials: settings.ini [WHITSON] or WHITSON_* environment variables (see whitson_credentials.py).
"""

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from whitson_credentials import load_whitson_credentials  # noqa: E402
from whitson_imperial_units import load_whitson_imperial_factors  # noqa: E402
from whitson_production_push import (  # noqa: E402
    list_production_wells,
    make_whitson_connection,
    push_all_wells,
    push_well,
)
from db_connection import get_sql_conn  # noqa: E402


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--well-name", help="Single well (PCE_Production [Well Name])")
    parser.add_argument("--all-wells", action="store_true", help="Push all wells in range")
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD")
    parser.add_argument(
        "--project-id",
        type=int,
        default=None,
        help="Whitson+ project id (default: from settings.ini / env)",
    )
    parser.add_argument("--replace", action="store_true")
    parser.add_argument(
        "--no-prodview-cap",
        action="store_true",
        help="Do not cap end date at prodview_effective_end_date()",
    )
    args = parser.parse_args()

    if not args.all_wells and not args.well_name:
        parser.error("Provide --well-name or --all-wells")
    if args.all_wells and args.well_name:
        parser.error("Use only one of --well-name or --all-wells")

    _client, _client_id, _client_secret, default_project_id = load_whitson_credentials()
    project_id = args.project_id if args.project_id is not None else default_project_id

    append_only = not args.replace
    apply_cap = not args.no_prodview_cap
    factors = load_whitson_imperial_factors()

    if args.all_wells:
        start = _parse_date(args.start) if args.start else None
        end = _parse_date(args.end) if args.end else None
        if (args.start and not args.end) or (args.end and not args.start):
            parser.error("Provide both --start and --end, or neither for full range")
        summary = push_all_wells(
            start_date=start,
            end_date=end,
            append_only=append_only,
            apply_prodview_cap=apply_cap,
            factors=factors,
            project_id=project_id,
            log_cb=print,
        )
        return 0 if summary["failed"] == 0 else 1

    conn = get_sql_conn()
    try:
        if args.start and args.end:
            start, end = _parse_date(args.start), _parse_date(args.end)
        else:
            from whitson_production_push import query_production_date_bounds

            start, end = query_production_date_bounds(conn)
        wells = list_production_wells(conn, start=start, end=end)
        uwi = None
        for name, wu in wells:
            if name == args.well_name:
                uwi = wu
                break
        whitson = make_whitson_connection()
        status, n, err, _attrs_ok = push_well(
            whitson,
            conn,
            well_name=args.well_name,
            uwi_api=uwi,
            start=start,
            end=end,
            factors=factors,
            project_id=project_id,
            append_only=append_only,
            log_cb=print,
        )
    finally:
        conn.close()

    if status == "ok":
        print(f"Uploaded {n} point(s)")
        return 0
    print(f"Upload failed: {err or status}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
