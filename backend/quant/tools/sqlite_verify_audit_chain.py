#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db_store import SQLiteStore


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify audit hash chain integrity")
    parser.add_argument("--db-path", default="logs/quant_api.db", help="SQLite DB path")
    parser.add_argument("--owner", default="", help="optional owner filter")
    parser.add_argument("--start-id", type=int, default=0, help="optional lower bound id")
    parser.add_argument("--end-id", type=int, default=0, help="optional upper bound id")
    parser.add_argument("--limit", type=int, default=5000, help="max rows to verify")
    args = parser.parse_args()

    db_path = Path(args.db_path).expanduser()
    if not db_path.is_absolute():
        db_path = (Path.cwd() / db_path).resolve()
    store = SQLiteStore(db_path)
    store.initialize()

    result = store.verify_audit_hash_chain(
        owner=(args.owner.strip() or None),
        start_id=(args.start_id or None),
        end_id=(args.end_id or None),
        limit=args.limit,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if not bool(result.get("ok")):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
