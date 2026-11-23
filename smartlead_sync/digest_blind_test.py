"""A 7-day blind test of the daily digest: the approved-sentence post against an AI-written one (t409, 23 Nov 2026).

Manveen, 23 Nov 2026: "i want output to be readable so maybe code can't do this then maybe render provides all data
then calls webhook that goes to t3 code and make sure it readable and easy to follow". Agreed: templates first, and a
blind test of an AI version for 7 days; the AI design wins only with 4 or more of 7 (then the parent proposes the
switch; this script switches nothing).

Every day (a PC scheduled task, ~11:10 IST, Manveen's own Claude Code login; no API key, no OpenRouter):

  1. read the answers to earlier days' DMs (Slack DM history, his "1" or "2") and log them;
  2. read today's facts, the structured items the 11:00 post was made from (Mongo digest_facts, written by Render's
     digest_post.py --post), waiting up to WAIT_MIN minutes for them;
  3. ask headless Claude (``claude -p``) to write the digest from those facts, with the approved sentences as a style
     guide;
  4. check the AI text against the facts (smartlead.digest_sentences.check: every item there, no number, domain or
     date the facts don't hold, the post's limits);
  5. DM Manveen both posts as "Version 1" and "Version 2" in random order: "Which would you act on faster? Reply 1 or
     2." Which one is the AI is kept in Mongo (digest_blind), never shown;
  6. once 7 days are answered (or on --end), one summary DM: template wins / AI wins, with the count.

    python digest_blind_test.py --env-file <.env> --log-dir <folder> [--claude wsl]  # the daily run
    python digest_blind_test.py --env-file <.env> --dry-run --facts-local       # render today's DM, send nothing,
                                                                                # facts built read-only from Mongo
Safety: reads Mongo and Slack; writes only Mongo digest_blind and the DMs to Manveen. Never posts to a channel.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

IST = timezone(timedelta(hours=5, minutes=30))
FACTS = "digest_facts"
BLIND = "digest_blind"
DAYS = 7
WIN = 4                                   # AI wins with this many of DAYS or more (agreed 23 Nov 2026)
WAIT_MIN = 50
MODEL = "sonnet"
CLAUDE_TIMEOUT_S = 240
ASK = "Which would you act on faster? Reply 1 or 2."
SUMMARY_ID = "summary"


def prompt(facts: dict) -> str:
    """What headless Claude gets: the job, the approved sentences as a style guide, the hard rules, the day's facts."""
    from smartlead import digest_sentences as ds
    guide = "\n".join(f"  {k}: {v}" for k, v in ds.TEMPLATES.items() if k != "2-")
    guide += "\n  P5's {question} is one of (f.question names which): " + "; ".join(
        f"{k}: \"{many}\"" for k, (many, _) in ds.QUESTIONS.items())
    data = {k: facts.get(k) for k in ("day", "n", "items", "watch", "thread")}
    return f"""You write one Slack message: today's inbox digest for Manveen, who runs cold-email infrastructure for
three clients. He reads it on his phone and must know at a glance what to do, for which client, by when.

Facts for today (JSON; "items" are the things he must do, in priority order; "watch" are things to watch with nothing
to do; "thread" are things to do with no fixed wording yet; "f" holds each item's values; "detail" and "steps" are
the lists and how-tos that go in the message's thread):
{json.dumps(data, ensure_ascii=False, indent=1)}

Style guide (the current template's sentences; the [brackets] are filled from "f"; you may word things your own way):
{guide}

Hard rules:
- Use only names, numbers, dates and domains that are in the facts. Never invent or round differently.
- The post names each of the first 5 items (its domain or inbox, or its client and count). Show at most 5 numbered
  items; say how many more are in the thread.
- At most 12 non-blank lines. Plain words, no markdown headings, no bold, no emoji, no code blocks.
- Start with one line that gives the date ({facts.get('day')}) and how many things there are to do ({facts.get('n')}).
- Write only the post itself (not the thread), nothing before or after it.
"""


def claude_cmd(where: str = "windows") -> tuple[list[str], dict | None]:
    """(command, env) for ``claude -p``: the Windows Claude Code login (default), or WSL Ubuntu's ("wsl"). An unattended
    run needs a long-lived login: CLAUDE_CODE_OAUTH_TOKEN (``claude setup-token``) in the env file, passed into WSL
    through WSLENV."""
    args = ["claude", "-p", "--model", MODEL, "--output-format", "text"]
    if where != "wsl":
        return args, None
    env = dict(os.environ)
    if env.get("CLAUDE_CODE_OAUTH_TOKEN"):
        env["WSLENV"] = ":".join(x for x in [env.get("WSLENV", ""), "CLAUDE_CODE_OAUTH_TOKEN/u"] if x)
    return ["wsl.exe", "-d", "Ubuntu", "--", "bash", "-lc", " ".join(args)], env


def ai_text(facts: dict, run=subprocess.run, where: str = "windows") -> tuple[str, str]:
    """(text, error) from ``claude -p`` on this PC (Manveen's Claude Code login). error is "" when it answered."""
    cmd, env = claude_cmd(where)
    try:
        r = run(cmd, input=prompt(facts), capture_output=True, text=True, encoding="utf-8", timeout=CLAUDE_TIMEOUT_S,
                **({"env": env} if env else {}))
    except FileNotFoundError:
        return "", "claude CLI not found on PATH"
    except subprocess.TimeoutExpired:
        return "", f"claude -p did not answer in {CLAUDE_TIMEOUT_S} s"
    if r.returncode != 0 or not (r.stdout or "").strip():
        tail = " ".join((r.stderr or r.stdout or "").strip().split())[-200:]
        return "", f"claude -p exit {r.returncode}" + (f": {tail}" if tail else "")
    return r.stdout.strip(), ""


def template_post(facts: dict) -> str:
    return str(facts.get("text") or "").split("\n\n--- thread ---")[0].strip()


def dm_text(day: str, k: int, v1: str, v2: str, problems: list[str], fail_version: int | None) -> str:
    note = ""
    if problems and fail_version:
        shown = "; ".join(problems[:4]) + (f"; and {len(problems) - 4} more" if len(problems) > 4 else "")
        note = f"\n\nNote: Version {fail_version} failed the fact check ({shown})."
    return (f"Inbox digest blind test, day {k} of {DAYS} ({day}). Two ways to write today's digest.\n\n"
            f"Version 1:\n\n{v1}\n\n----------\n\nVersion 2:\n\n{v2}\n\n----------{note}\n\n{ASK}")


def build_dm(facts: dict, ai: str, problems: list[str], k: int, rng=random) -> tuple[str, int]:
    """(DM text, which version is the AI)."""
    tpl = template_post(facts)
    ai_is = rng.choice([1, 2])
    v1, v2 = (ai, tpl) if ai_is == 1 else (tpl, ai)
    return dm_text(str(facts.get("day")), k, v1, v2, problems, ai_is if problems else None), ai_is


def answer_of(messages: list[dict], owner: str, after_ts: str) -> str | None:
    """His first "1" or "2" after the DM (DM history or the DM's thread), else None."""
    for m in sorted(messages, key=lambda m: float(m.get("ts") or 0)):
        if m.get("user") != owner or float(m.get("ts") or 0) <= float(after_ts or 0):
            continue
        t = str(m.get("text") or "").strip().strip(".!").lower().replace("version ", "")
        if t in ("1", "2"):
            return t
    return None


def tally(docs: list[dict]) -> dict:
    """{answered, ai, template, decided}: the count so far and whether the 4-of-7 rule is met."""
    done = [d for d in docs if d.get("winner") in ("ai", "template")]
    ai = sum(1 for d in done if d["winner"] == "ai")
    return {"answered": len(done), "ai": ai, "template": len(done) - ai, "ai_wins": ai >= WIN}


def summary_text(t: dict) -> str:
    who = "AI wins" if t["ai_wins"] else "Template wins"
    rule = (f"The AI version won {t['ai']} of {t['answered']}, so the parent session will propose switching the digest "
            f"to the AI design (nothing changes until you say yes)." if t["ai_wins"] else
            f"The AI version won {t['ai']} of {t['answered']} (it needed {WIN}), so the approved sentences stay.")
    return f"Inbox digest blind test: done. {who}: template {t['template']}, AI {t['ai']}.\n\n{rule}"


# --- Slack and Mongo ---------------------------------------------------------------------------------------------------

def slack(method: str, token: str, **params) -> dict:
    import requests
    r = requests.post(f"https://slack.com/api/{method}", headers={"Authorization": f"Bearer {token}"}, data=params,
                      timeout=30)
    if r.status_code != 200:
        return {"ok": False, "error": f"HTTP {r.status_code}"}
    return r.json()


def dm(token: str, owner: str, text: str) -> tuple[str, str] | None:
    """(channel, ts) of a DM from the bot to ``owner`` (chat:write to a user ID opens the app DM), or None."""
    r = slack("chat.postMessage", token, channel=owner, text=text, unfurl_links="false")
    return (r["channel"], r["ts"]) if r.get("ok") else None


def read_answers(db, token: str, owner: str, log) -> None:
    for d in db[BLIND].find({"answer": None, "channel": {"$ne": None}}):
        msgs = slack("conversations.history", token, channel=d["channel"], oldest=d["ts"], limit=50).get("messages") or []
        msgs += slack("conversations.replies", token, channel=d["channel"], ts=d["ts"], limit=50).get("messages") or []
        a = answer_of(msgs, owner, d["ts"])
        if a:
            winner = "ai" if int(a) == d["ai_is"] else "template"
            db[BLIND].update_one({"_id": d["_id"]}, {"$set": {"answer": a, "winner": winner,
                                                              "read_at": datetime.now(timezone.utc).isoformat()}})
            log(f"{d['_id']}: answered {a} -> {winner}")


def wait_for_facts(db, day: str, minutes: int, sleep=time.sleep) -> dict | None:
    for _ in range(max(1, minutes // 2)):
        f = db[FACTS].find_one({"_id": day})
        if f:
            return f
        sleep(120)
    return None


def local_facts(now: datetime) -> dict:
    """Today's facts built here, read-only (for a dry run before Render saves them)."""
    import digest_post as dp
    from smartlead import digest as dg
    db = dg._db()
    sources = list(db[dg.ASKS].find({})) + dp.removed_source(db, now)
    done = [d for d in db[dg.DONE].find({"posted_on": None})]
    p = dp.plan(sources, done, [], now, dg.load_holds(db), [], dp.load_ctx(db, now))
    head, parts = dp.compose(p, now)
    return dp.day_facts(p, now, head, parts)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--env-file", required=True)
    ap.add_argument("--log-dir", default="")
    ap.add_argument("--dry-run", action="store_true", help="render today's DM; send nothing, write nothing")
    ap.add_argument("--facts-local", action="store_true", help="build today's facts here (read-only)")
    ap.add_argument("--end", action="store_true", help="send the summary now, whatever the count")
    ap.add_argument("--claude", choices=("windows", "wsl"), default="windows",
                    help="whose Claude Code login runs claude -p: Windows (default) or WSL Ubuntu")
    args = ap.parse_args(argv)
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    from dotenv import load_dotenv
    load_dotenv(args.env_file)
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from smartlead import digest as dg
    from smartlead import digest_sentences as ds
    now = datetime.now(timezone.utc)
    day = f"{now.astimezone(IST):%Y-%m-%d}"
    logf = Path(args.log_dir) / "blind-test.log" if args.log_dir else None

    def log(x: str) -> None:
        line = f"{datetime.now(IST):%Y-%m-%d %H:%M} IST  {x}"
        print(line)
        if logf and not args.dry_run:
            logf.parent.mkdir(parents=True, exist_ok=True)
            with logf.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")

    db = dg._db()
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if not token and not args.dry_run:
        log("SLACK_BOT_TOKEN missing: nothing sent")
        return 1
    if not args.dry_run:
        read_answers(db, token, dg.OWNER, log)
    docs = list(db[BLIND].find({"_id": {"$ne": SUMMARY_ID}}))
    t = tally(docs)
    if (t["answered"] >= DAYS or args.end) and not db[BLIND].find_one({"_id": SUMMARY_ID}):
        text = summary_text(t)
        log(f"summary: {text.splitlines()[0]}")
        if not args.dry_run and dm(token, dg.OWNER, text):
            db[BLIND].replace_one({"_id": SUMMARY_ID}, {"_id": SUMMARY_ID, **t, "at": now.isoformat()}, upsert=True)
        return 0
    if db[BLIND].find_one({"_id": SUMMARY_ID}):
        log("the test is over (summary sent): nothing to do")
        return 0
    if db[BLIND].find_one({"_id": day}) and not args.dry_run:
        log(f"{day}: already sent")
        return 0
    facts = local_facts(now) if args.facts_local else (
        db[FACTS].find_one({"_id": day}) if args.dry_run else wait_for_facts(db, day, WAIT_MIN))
    if not facts:
        log(f"{day}: no digest facts from Render today (not deployed yet, or the digest did not post): no DM")
        return 0
    if not facts.get("items") and not facts.get("thread"):
        log(f"{day}: nothing to do today: no DM (nothing to compare)")
        return 0
    ai, err = ai_text(facts, where=args.claude)
    if err:
        log(f"{day}: no AI version ({err}): no DM")
        return 1
    problems = ds.check(ai, facts)
    k = len([d for d in docs if d.get("channel")]) + 1
    text, ai_is = build_dm(facts, ai, problems, k)
    log(f"{day}: AI check {'passed' if not problems else 'FAILED: ' + '; '.join(problems)}")
    if args.dry_run:
        print("\n--- DM (dry run, not sent) ---\n" + text)
        print(f"\n(the AI is Version {ai_is}; kept in Mongo {BLIND}, never shown)")
        return 0
    sent = dm(token, dg.OWNER, text)
    db[BLIND].replace_one({"_id": day}, {"_id": day, "ai_is": ai_is, "channel": sent[0] if sent else None,
                                         "ts": sent[1] if sent else None, "problems": problems, "ai_text": ai,
                                         "template_text": template_post(facts), "answer": None, "winner": None,
                                         "at": now.isoformat()}, upsert=True)
    log(f"{day}: DM {'sent' if sent else 'NOT sent (Slack refused)'}")
    return 0 if sent else 1


if __name__ == "__main__":
    raise SystemExit(main())
