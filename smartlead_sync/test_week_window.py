"""The trailing-7-day window used for `week_sent`.

Both entrypoints computed it as `today.replace(day=max(1, today.day - 7))`,
which only works mid-month. On the 3rd it clamps to the 1st (a 2-day window);
on the 1st it returns the 1st (a zero-day window). It can never reach back
into the previous month at all.

That matters more since the metrics tab began dropping PAUSED/COMPLETED
campaigns that sent nothing in the last week: during the first week of any
month the window would collapse and quietly drop campaigns that were in fact
still sending.
"""
from datetime import datetime, timezone
from smartlead.campaign_metrics import week_start_str

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m


def d(y, m, day):
    return datetime(y, m, day, 12, 0, tzinfo=timezone.utc)


# Mid-month: the straightforward case the old code got right.
ok(week_start_str(d(2026, 9, 16)) == "2026-09-09", "16 Sep -> 9 Sep")
ok(week_start_str(d(2026, 9, 30)) == "2026-09-23", "30 Sep -> 23 Sep")

# Month boundaries: the old code clamped these to the 1st.
ok(week_start_str(d(2026, 9, 1)) == "2026-08-25",
   f"1 Sep reaches into August, got {week_start_str(d(2026, 9, 1))}")
ok(week_start_str(d(2026, 9, 3)) == "2026-08-27",
   f"3 Sep reaches into August, got {week_start_str(d(2026, 9, 3))}")
ok(week_start_str(d(2026, 9, 7)) == "2026-08-31", "7 Sep -> 31 Aug")

# Year boundary.
ok(week_start_str(d(2026, 1, 3)) == "2025-12-27",
   f"3 Jan reaches into last December, got {week_start_str(d(2026, 1, 3))}")

# Leap day, and the month after a short month.
ok(week_start_str(d(2026, 3, 2)) == "2026-02-23", "2 Mar 2026 -> 23 Feb (leap year)")
ok(week_start_str(d(2025, 3, 2)) == "2025-02-23", "2 Mar 2025 -> 23 Feb")

# The window is always exactly 7 days back, every day of a full month.
for day in range(1, 29):
    t = d(2026, 9, day)
    got = week_start_str(t)
    want = (t.date().toordinal() - 7)
    ok(datetime.fromisoformat(got).date().toordinal() == want,
       f"{day:02d} Sep is exactly 7 days back")

print("\nALL PASSED")
