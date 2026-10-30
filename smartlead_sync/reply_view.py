#!/usr/bin/env python3
"""Fetch one campaign's replies for a "Show replies" click in the stats post.

index.js runs this when someone clicks the button and posts the result only
to them (chat.postEphemeral). The button's value is the payload.

    python reply_view.py '<button value JSON>'

Prints one JSON line, {"text", "blocks"}, on stdout; logs go to stderr.
"""
from __future__ import annotations

import asyncio
import json
import sys

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from smartlead.reply_view import fetch_and_render, render


def main() -> int:
    try:
        payload = json.loads(sys.argv[1])
    except (IndexError, ValueError):
        print(json.dumps(render("Replies", "Email", [], "",
                                note="⚠️ The button carried no campaign.")))
        return 1
    print(json.dumps(asyncio.run(fetch_and_render(payload)), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
