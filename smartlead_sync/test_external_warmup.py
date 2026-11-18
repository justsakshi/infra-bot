"""Inboxes whose warm-up runs outside Smartlead (t248, 18 Nov 2026): the marker itself. No network; synthetic inboxes."""
from smartlead import external_warmup as xw


class Col:
    def __init__(self, docs=None, boom=False):
        self.docs, self.boom = list(docs or []), boom

    def find(self, q=None, proj=None):
        if self.boom:
            raise RuntimeError("mongo down")
        return [dict(d) for d in self.docs]

    def update_one(self, q, upd, upsert=False):
        for d in self.docs:
            if d["_id"] == q["_id"]:
                d.update(upd.get("$set", {}))
                return
        self.docs.append({**q, **upd.get("$set", {}), **upd.get("$setOnInsert", {})})


def test_an_inbox_is_marked_by_its_address_or_its_whole_domain():
    marks = xw.from_docs([{"_id": "a@one.test", "vendor": "InboxKit"}, {"_id": "Two.Test", "vendor": "inboxkit"}])
    assert xw.vendor("A@one.test", marks) == "InboxKit"
    assert xw.vendor("b@two.test", marks) == "InboxKit"          # a domain entry covers every inbox on it
    assert xw.vendor("b@one.test", marks) is None                # an address entry covers that inbox only
    assert xw.vendor("", marks) is None and xw.vendor("x@y.test", None) is None


def test_a_doc_switched_off_or_without_a_vendor_marks_nothing():
    marks = xw.from_docs([{"_id": "a@one.test", "vendor": "InboxKit", "off": True}, {"_id": "b@one.test"}])
    assert marks == {}


def test_load_never_raises_and_says_when_it_could_not_read():
    assert xw.load(Col([{"_id": "a@one.test", "vendor": "InboxKit"}])) == {"a@one.test": "InboxKit"}
    assert xw.load(Col(boom=True)) == {}
    assert xw.load(None) == {}


def test_mark_adds_one_doc_per_inbox_and_keeps_the_first_date():
    col = Col()
    n = xw.mark(col, ["A@one.test", "a@one.test", "b@one.test"], vendor="InboxKit", since="2026-11-18",
                who="t248", why="pre-warmed by the vendor")
    assert n == 2 and {d["_id"] for d in col.docs} == {"a@one.test", "b@one.test"}
    xw.mark(col, ["a@one.test"], vendor="InboxKit", since="2026-12-01", who="later", why="again")
    assert [d["since"] for d in col.docs if d["_id"] == "a@one.test"] == ["2026-11-18"]


def test_the_note_names_the_vendor_and_says_smartlead_warm_up_is_not_used():
    n = xw.note("InboxKit")
    assert "InboxKit" in n and "Smartlead" in n


def test_the_cli_mark_is_a_dry_run_unless_applied_and_never_prints_an_address(monkeypatch, tmp_path, capsys):
    import external_warmup_cli as cli
    f = tmp_path / "keys.txt"
    f.write_text("A@one.test\nb@one.test\n# a comment\ntwo.test\n", encoding="utf-8")
    col = Col()
    monkeypatch.setattr(cli, "_col", lambda: col)
    assert cli.main(["mark", "--file", str(f), "--vendor", "inboxkit", "--why", "t"]) == 0
    assert col.docs == []
    assert cli.main(["mark", "--file", str(f), "--vendor", "inboxkit", "--why", "t", "--apply"]) == 0
    assert {d["_id"]: d["vendor"] for d in col.docs} == {"a@one.test": "InboxKit", "b@one.test": "InboxKit",
                                                         "two.test": "InboxKit"}
    out = capsys.readouterr().out
    assert "one.test" not in out and "2 inboxes, 1 domains" in out
