import os
import sys

from pipeline import run_pipeline
from db import init_db
from config import DB_PATH

_HERE = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_CSV = os.path.normpath(os.path.join(_HERE, "..", "..", "data", "challenge_contacts.csv"))
if not os.path.isfile(_DEFAULT_CSV):
    _DEFAULT_CSV = os.path.join(_HERE, "data", "challenge_contacts.csv")


if __name__ == "__main__":
    args = sys.argv[1:]

    csv_file      = args[0] if len(args) >= 1 else _DEFAULT_CSV
    limit_raw     = args[1] if len(args) >= 2 else None
    force_refresh = "--refresh" in args or "--force" in args

    try:
        limit = int(limit_raw) if limit_raw and limit_raw not in ("--refresh", "--force") else None
    except ValueError:
        print(f"Warning: invalid limit '{limit_raw}', ignoring.")
        limit = None

    if not os.path.isfile(csv_file):
        print(f"Error: CSV not found at '{csv_file}'")
        sys.exit(1)

    print(f"CSV  : {csv_file}")
    print(f"DB   : {os.path.abspath(DB_PATH)}")
    print(f"Limit: {limit or 'all rows'}")
    print(f"Force refresh: {force_refresh}\n")

    run_id, costs = run_pipeline(
        csv_file,
        db_path=DB_PATH,
        limit=limit,
        force_refresh=force_refresh,
    )
    print(f"\nRun {run_id} complete | Cost: ${costs['total_cost']:.4f}")