#!/usr/bin/env python3
"""Branded vs pre-warmed inboxes per client (READ-ONLY).

    python inbox_split.py          # one line
    python inbox_split.py --json   # {counts, line, errors} (the /domains Fleet & cost screen)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))
except Exception:
    pass

from smartlead import inbox_split as sp


def main() -> int:
    ap = argparse.ArgumentParser(description="Branded vs pre-warmed inboxes per client")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    errors: list[str] = []
    counts = asyncio.run(sp.collect(errors))
    line = sp.split_line(counts)
    print(json.dumps({"counts": counts, "line": line, "errors": errors}) if args.json
          else line + ("\n(could not read: " + "; ".join(errors) + ")" if errors else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
