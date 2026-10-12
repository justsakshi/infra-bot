# Guided Buy Flow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** From the Slack domain suggestions, one "Buy these" modal walks the team through provider → per-domain inbox mix (with live volume and cost) → senders → stage, for ScaledMail and Zapmail.

**Architecture:** One offer file (`smartlead_sync/inbox_offer.json`) holds per-type inboxes/domain, price and daily volume; Python ScaledMail staging and the new `buy_flow.js` both read it. `buy_flow.js` keeps all state in the modal's `private_metadata` and redraws with `views.update`; on submit it calls the existing stagers (`scaledmail_cli.py stage`, `inbox_jobs.py --create --kind new`) and posts the existing cards (`stagedBlocks`, `jobBlocks`). Nothing new can spend.

**Tech Stack:** Node 18+ with `@slack/bolt` 3 (Socket Mode), Python 3.11 (`smartlead_sync/.venv`), plain `assert` node tests run by `npm test`, pytest.

**Spec:** `docs/superpowers/specs/2026-10-12-guided-buy-flow-design.md`

## Global Constraints
- Volume per inbox per day: Google 20, Outlook 8, SMTP 8 (Zapmail Outlook 8 too).
- A month = 22 sending days (weekdays). Show per day and per month.
- Always 3 mailboxes per domain for ScaledMail Google, Zapmail Google, Zapmail Outlook. ScaledMail Outlook 25/domain ($50/domain), SMTP 4/domain ($3.75/domain), ScaledMail Google $3.50/mailbox, Zapmail $3.25/mailbox, Zapmail has no SMTP.
- Warmup 21-30 days after setup; setup ScaledMail 3 business days, Zapmail 1.
- Staging only. Placing stays: approver click + `SCALEDMAIL_ALLOW_SPEND`; Zapmail job approval + `ZAPMAIL_ALLOW_SPEND`. No inbox passwords anywhere.
- Slack limits: modal ≤ 100 blocks, `private_metadata` ≤ 3000 chars, modal title ≤ 24 chars, checkbox option `value` ≤ 150 chars.
- Old paths keep working: `/domains sm` order form, `domains_stage` / `domains_sm_order` handlers stay registered.
- Commits: no co-author line. Push only when the user says "push it".
- Python from WSL: `smartlead_sync/.venv/Scripts/python.exe`. Node: `node` (WSL node 24 is fine for tests).

## Review Focus
- A ticked name is not sold by the chosen provider (e.g. no ScaledMail price): it is listed as "left out" and never staged; a provider with 0 usable names has no button.
- Two quick clicks redraw from a stale view: `views.update` rejects with `hash_conflict`; the handler swallows it (the next click works) instead of crashing.
- Bettrdata has no default senders: the senders box starts empty and Stage refuses until a "First Last" name is given.
- 20 ticked names: metadata stays under 3000 chars and every view under 100 blocks.
- One row fails to stage (name just taken / Python error): the other rows still stage; the failed row posts its own error line.

---

## File map
- Create `smartlead_sync/inbox_offer.json` — the offer table (Task 1).
- Modify `smartlead_sync/smartlead/scaledmail_orders.py:36-37` — `PRICE` / `PRICE_BASIS` from the offer file (Task 1).
- Create `buy_flow.js` — pure math (Task 2), views (Task 3), Slack handlers (Task 5).
- Create `test_buy_flow.js` — node tests for Tasks 2, 3, 5.
- Modify `domain_suggest_command.js` — option values carry prices, `selectedPicks`, one "Buy these" button (Task 4).
- Modify `test_domain_suggest_command.js:47` — new button list (Task 4).
- Modify `domains_command.js:642` — register the flow (Task 5).
- Modify `smartlead_sync/domain_clients.json` — `redirect` for Precise Leads (Task 5).
- Modify `docs/SCALEDMAIL_INTEGRATION.md` — one paragraph (Task 6).

---

### Task 1: Offer file, read by Python

**Files:**
- Create: `smartlead_sync/inbox_offer.json`
- Modify: `smartlead_sync/smartlead/scaledmail_orders.py:36-37`
- Test: `smartlead_sync/test_scaledmail.py`

**Interfaces:**
- Produces: `inbox_offer.json` shape below (used by Tasks 2-5); `scaledmail_orders.load_offer() -> dict`; `PRICE`, `PRICE_BASIS` unchanged names and values.

- [ ] **Step 1: Write the failing test** (append to `smartlead_sync/test_scaledmail.py`)

```python
def test_prices_come_from_the_offer_file():
    offer = so.load_offer()
    assert offer["sending_days_per_month"] == 22
    assert so.PRICE == {"google": 3.5, "outlook": 50.0, "smtp": 3.75}
    assert so.PRICE_BASIS == {"google": "mailbox", "outlook": "domain", "smtp": "domain"}
    zm = {t["key"]: t for t in offer["providers"]["zapmail"]["types"]}
    assert set(zm) == {"GOOGLE", "MICROSOFT"} and all(t["price"] == 3.25 and t["per_domain"] == 3 for t in zm.values())
```

- [ ] **Step 2: Run it, expect FAIL** (`AttributeError: ... load_offer`)

Run: `cd smartlead_sync && ./.venv/Scripts/python.exe -m pytest -q test_scaledmail.py -k offer_file`

- [ ] **Step 3: Create `smartlead_sync/inbox_offer.json`**

```json
{
  "sending_days_per_month": 22,
  "warmup_days": [21, 30],
  "providers": {
    "scaledmail": {
      "label": "ScaledMail",
      "blurb": "Google, Outlook or SMTP. ScaledMail sets up the inboxes; their support uploads them to Smartlead.",
      "setup_business_days": 3,
      "types": [
        {"key": "google", "label": "Google Workspace", "per_domain": 3, "per_inbox_day": 20, "price": 3.5, "basis": "mailbox"},
        {"key": "outlook", "label": "Microsoft Outlook", "per_domain": 25, "per_inbox_day": 8, "price": 50.0, "basis": "domain"},
        {"key": "smtp", "label": "SMTP", "per_domain": 4, "per_inbox_day": 8, "price": 3.75, "basis": "domain"}
      ]
    },
    "zapmail": {
      "label": "Zapmail",
      "blurb": "Google or Outlook. The bot buys the domains, creates the inboxes and connects them to Smartlead.",
      "setup_business_days": 1,
      "types": [
        {"key": "GOOGLE", "label": "Google Workspace", "per_domain": 3, "per_inbox_day": 20, "price": 3.25, "basis": "mailbox"},
        {"key": "MICROSOFT", "label": "Microsoft Outlook", "per_domain": 3, "per_inbox_day": 8, "price": 3.25, "basis": "mailbox"}
      ]
    }
  }
}
```

- [ ] **Step 4: Replace lines 36-37 of `scaledmail_orders.py`**

Old:
```python
PRICE = {"google": 3.50, "outlook": 50.0, "smtp": 3.75}   # live 2026-10-08
PRICE_BASIS = {"google": "mailbox", "outlook": "domain", "smtp": "domain"}
```
New:
```python
OFFER_FILE = os.getenv("INBOX_OFFER_FILE") or os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "inbox_offer.json")


def load_offer() -> dict:
    """The per-type offer (inboxes/domain, price, daily volume) shared with Slack's buy flow."""
    import json
    with open(OFFER_FILE, encoding="utf-8") as f:
        return json.load(f)


_SM_TYPES = load_offer()["providers"]["scaledmail"]["types"]
PRICE = {t["key"]: float(t["price"]) for t in _SM_TYPES}          # live 2026-10-08
PRICE_BASIS = {t["key"]: t["basis"] for t in _SM_TYPES}
```

- [ ] **Step 5: Run the ScaledMail tests, expect all PASS**

Run: `cd smartlead_sync && ./.venv/Scripts/python.exe -m pytest -q test_scaledmail.py`
Expected: `61 passed`

- [ ] **Step 6: Commit**

```bash
git add smartlead_sync/inbox_offer.json smartlead_sync/smartlead/scaledmail_orders.py smartlead_sync/test_scaledmail.py
git commit -m "Inbox offer file: per-type inboxes, price, daily volume; ScaledMail prices read it"
```

---

### Task 2: Buy-flow math (pure)

**Files:**
- Create: `buy_flow.js`
- Test: `test_buy_flow.js`

**Interfaces:**
- Consumes: `smartlead_sync/inbox_offer.json` (Task 1).
- Produces (exported from `buy_flow.js`):
  - `OFFER` (parsed JSON)
  - `typesFor(provider: 'scaledmail'|'zapmail') -> Type[]`
  - `domainMonthly(t: Type) -> number`
  - `rowMath(t: Type, n: number) -> {domains, inboxes, perDay, perMonth, monthlyUsd}`
  - `usable(picks: Pick[], provider) -> Pick[]` where `Pick = {d: string, zp: number|null, sp: number|null}`
  - `initialCounts(provider, n: number) -> number[]`
  - `rebalance(counts: number[], i: number, delta: 1|-1) -> number[]`
  - `split(names: string[], counts: number[]) -> string[][]`
  - `totals(provider, picks: Pick[], counts: number[]) -> {domains, inboxes, perDay, perMonth, monthlyUsd, domainsUsd, firstChargeUsd}`
  - `addBusinessDays(iso: string, n: number) -> string`, `readyRange(todayIso: string, provider) -> [string, string]`

- [ ] **Step 1: Write the failing test** — create `test_buy_flow.js`

```js
// Guided buy flow: math, views, handlers. Run: node test_buy_flow.js (no Slack, no Python, no network)
const assert = require('assert');
const bf = require('./buy_flow');

let passed = 0;
async function test(name, fn) {
  try { await fn(); passed += 1; } catch (err) { console.error('FAIL', name, '\n ', err.message); process.exitCode = 1; }
}
const T = (prov, key) => bf.typesFor(prov).find(t => t.key === key);
const picks = n => Array.from({ length: n }, (_, i) => ({ d: 'name' + i + '.com', zp: 12.99, sp: 15.5 }));

(async () => {
  await test('row math per domain matches the agreed table', () => {
    assert.deepStrictEqual(bf.rowMath(T('scaledmail', 'google'), 7), { domains: 7, inboxes: 21, perDay: 420, perMonth: 9240, monthlyUsd: 73.5 });
    assert.deepStrictEqual(bf.rowMath(T('scaledmail', 'outlook'), 2), { domains: 2, inboxes: 50, perDay: 400, perMonth: 8800, monthlyUsd: 100 });
    assert.deepStrictEqual(bf.rowMath(T('scaledmail', 'smtp'), 5), { domains: 5, inboxes: 20, perDay: 160, perMonth: 3520, monthlyUsd: 18.75 });
    assert.deepStrictEqual(bf.rowMath(T('zapmail', 'GOOGLE'), 1), { domains: 1, inboxes: 3, perDay: 60, perMonth: 1320, monthlyUsd: 9.75 });
    assert.deepStrictEqual(bf.rowMath(T('zapmail', 'MICROSOFT'), 1), { domains: 1, inboxes: 3, perDay: 24, perMonth: 528, monthlyUsd: 9.75 });
  });

  await test('usable keeps only names the provider sells', () => {
    const p = [{ d: 'a.com', zp: 12.99, sp: null }, { d: 'b.com', zp: null, sp: 15.5 }];
    assert.deepStrictEqual(bf.usable(p, 'scaledmail').map(x => x.d), ['b.com']);
    assert.deepStrictEqual(bf.usable(p, 'zapmail').map(x => x.d), ['a.com']);
  });

  await test('rebalance keeps the sum and never goes negative', () => {
    let c = bf.initialCounts('scaledmail', 5);
    assert.deepStrictEqual(c, [5, 0, 0]);
    c = bf.rebalance(c, 1, 1); assert.deepStrictEqual(c, [4, 1, 0]);
    c = bf.rebalance(c, 2, 1); assert.deepStrictEqual(c, [4, 0, 1]);   // takes from the last non-empty other row
    c = bf.rebalance(c, 2, -1); assert.deepStrictEqual(c, [5, 0, 0]);  // gives back to the first row
    c = bf.rebalance(c, 0, -1); assert.deepStrictEqual(c, [4, 1, 0]);  // first row gives to the second
    assert.deepStrictEqual(bf.rebalance([0, 0, 0], 1, 1), [0, 0, 0]);
    assert.deepStrictEqual(bf.rebalance([3, 0, 0], 1, -1), [3, 0, 0]);
  });

  await test('split hands out names in tick order', () => {
    assert.deepStrictEqual(bf.split(['a', 'b', 'c', 'd'], [2, 0, 2]), [['a', 'b'], [], ['c', 'd']]);
  });

  await test('totals add rows and domain registrations', () => {
    const t = bf.totals('scaledmail', picks(4), [3, 1, 0]);
    assert.strictEqual(t.domains, 4);
    assert.strictEqual(t.inboxes, 9 + 25);
    assert.strictEqual(t.perDay, 180 + 200);
    assert.strictEqual(t.perMonth, 380 * 22);
    assert.strictEqual(t.monthlyUsd, 31.5 + 50);
    assert.strictEqual(t.domainsUsd, 62);
    assert.strictEqual(t.firstChargeUsd, 143.5);
  });

  await test('ready dates skip weekends, then 21-30 days of warmup', () => {
    assert.strictEqual(bf.addBusinessDays('2026-10-16', 3), '2026-10-21');          // Fri + 3 = Wed
    assert.deepStrictEqual(bf.readyRange('2026-10-12', 'scaledmail'), ['2026-11-05', '2026-11-14']);
    assert.deepStrictEqual(bf.readyRange('2026-10-12', 'zapmail'), ['2026-11-03', '2026-11-12']);
  });

  console.log(passed + ' passed');
})();
```

- [ ] **Step 2: Run it, expect FAIL** (`Cannot find module './buy_flow'`)

Run: `node test_buy_flow.js`

- [ ] **Step 3: Create `buy_flow.js` with the math**

```js
/**
 * Guided buy flow: ticked suggestions → provider → per-domain inbox mix
 * (live volume + cost) → senders → stage. Staging only; placing stays with
 * approvers (ScaledMail plan card / Zapmail job card).
 * Offer numbers: smartlead_sync/inbox_offer.json. Spec:
 * docs/superpowers/specs/2026-10-12-guided-buy-flow-design.md
 */
const fs = require('fs');
const path = require('path');

const OFFER = JSON.parse(fs.readFileSync(
  process.env.INBOX_OFFER_FILE || path.join(__dirname, 'smartlead_sync', 'inbox_offer.json'), 'utf8'));

const round2 = v => Math.round(v * 100) / 100;
const typesFor = provider => (OFFER.providers[provider] || { types: [] }).types;
const domainMonthly = t => (t.basis === 'mailbox' ? t.price * t.per_domain : t.price);

function rowMath(t, n) {
  const inboxes = n * t.per_domain;
  const perDay = inboxes * t.per_inbox_day;
  return { domains: n, inboxes, perDay, perMonth: perDay * OFFER.sending_days_per_month,
    monthlyUsd: round2(n * domainMonthly(t)) };
}

const priceOf = (p, provider) => (provider === 'scaledmail' ? p.sp : p.zp);
const usable = (picks, provider) => picks.filter(p => priceOf(p, provider) !== null && priceOf(p, provider) !== undefined);

function initialCounts(provider, n) {
  return typesFor(provider).map((_, i) => (i === 0 ? n : 0));
}

function rebalance(counts, i, delta) {
  const c = counts.slice();
  if (delta > 0) {
    let j = -1;
    for (let k = c.length - 1; k >= 0; k--) if (k !== i && c[k] > 0) { j = k; break; }
    if (j < 0) return c;
    c[j] -= 1; c[i] += 1;
  } else {
    if (c[i] <= 0 || c.length < 2) return c;
    const target = i === 0 ? 1 : 0;
    c[i] -= 1; c[target] += 1;
  }
  return c;
}

function split(names, counts) {
  const out = [];
  let at = 0;
  for (const n of counts) { out.push(names.slice(at, at + n)); at += n; }
  return out;
}

function totals(provider, picks, counts) {
  const types = typesFor(provider);
  const groups = split(picks, counts);
  const t = { domains: 0, inboxes: 0, perDay: 0, perMonth: 0, monthlyUsd: 0, domainsUsd: 0 };
  types.forEach((ty, i) => {
    const r = rowMath(ty, counts[i] || 0);
    for (const k of ['domains', 'inboxes', 'perDay', 'perMonth', 'monthlyUsd']) t[k] += r[k];
    t.domainsUsd += (groups[i] || []).reduce((s, p) => s + Number(priceOf(p, provider) || 0), 0);
  });
  t.monthlyUsd = round2(t.monthlyUsd);
  t.domainsUsd = round2(t.domainsUsd);
  t.firstChargeUsd = round2(t.monthlyUsd + t.domainsUsd);
  return t;
}

function addDays(iso, n) {
  const d = new Date(iso + 'T00:00:00Z');
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}
function addBusinessDays(iso, n) {
  let d = iso;
  let left = n;
  while (left > 0) {
    d = addDays(d, 1);
    const wd = new Date(d + 'T00:00:00Z').getUTCDay();
    if (wd !== 0 && wd !== 6) left -= 1;
  }
  return d;
}
function readyRange(todayIso, provider) {
  const live = addBusinessDays(todayIso, OFFER.providers[provider].setup_business_days);
  return [addDays(live, OFFER.warmup_days[0]), addDays(live, OFFER.warmup_days[1])];
}

module.exports = { OFFER, typesFor, domainMonthly, rowMath, usable, initialCounts, rebalance, split, totals,
  addBusinessDays, readyRange };
```

- [ ] **Step 4: Run, expect PASS** — `node test_buy_flow.js` → `6 passed`

- [ ] **Step 5: Commit**

```bash
git add buy_flow.js test_buy_flow.js
git commit -m "Buy flow: per-domain volume and cost math from the offer file"
```

---

### Task 3: Buy-flow views

**Files:**
- Modify: `buy_flow.js` (add views + exports)
- Test: `test_buy_flow.js`

**Interfaces:**
- Consumes: Task 2 functions.
- Produces:
  - `Meta = {c: client, ch: channel, n: Pick[], p?: provider, k?: number[]}` (JSON in `private_metadata`)
  - `providerView(meta) -> modal view`, `mixView(meta) -> modal view`, `confirmView(meta, defaults: {senders: string, redirect: string}) -> modal view`
  - All views: `type 'modal'`, `callback_id 'buy_flow'`, title `'Buy domains + inboxes'`. Only `confirmView` has `submit` (`'Stage (no charge)'`).
  - Action ids used in views: `buy_provider` (value provider), `buy_plus` / `buy_minus` (value row index), `buy_next`, `buy_back` (value `'provider'` or `'mix'`). Inputs: block `senders`, block `redirect` (ScaledMail only), each with `action_id 'value'`.

- [ ] **Step 1: Write the failing tests** (insert before `console.log(passed …)` in `test_buy_flow.js`)

```js
  const meta = (n, extra) => ({ c: 'Precise Leads', ch: 'C1', n: picks(n), ...extra });
  const txt = v => JSON.stringify(v.blocks);

  await test('provider view: a button per provider that sells something, left-out names listed', () => {
    const n = [{ d: 'a.com', zp: 12.99, sp: null }, { d: 'b.com', zp: 12.99, sp: 15.5 }];
    const v = bf.providerView({ c: 'Melior', ch: 'C1', n });
    const btns = v.blocks.filter(b => b.accessory && b.accessory.action_id === 'buy_provider').map(b => b.accessory.value);
    assert.deepStrictEqual(btns, ['scaledmail', 'zapmail']);
    assert.match(txt(v), /left out: a\.com/);
    const none = bf.providerView({ c: 'Melior', ch: 'C1', n: [{ d: 'a.com', zp: 12.99, sp: null }] });
    assert.deepStrictEqual(none.blocks.filter(b => b.accessory && b.accessory.action_id === 'buy_provider').map(b => b.accessory.value), ['zapmail']);
    assert.ok(!v.submit && v.callback_id === 'buy_flow' && v.title.text.length <= 24);
  });

  await test('mix view: a row per type with − / + and the running total', () => {
    const v = bf.mixView(meta(7, { p: 'scaledmail', k: [7, 0, 0] }));
    const s = txt(v);
    assert.match(s, /Google Workspace/);
    assert.match(s, /3 inboxes\/domain · 60\/day · 1,320\/mo · \$10\.50\/mo per domain/);
    assert.match(s, /21 inboxes · ~420\/day · ~9,240\/mo/);
    assert.match(s, /\$73\.50\/mo/);
    assert.match(s, /ready to send around/i);
    const plus = v.blocks.flatMap(b => b.elements || []).filter(e => e.action_id === 'buy_plus');
    assert.strictEqual(plus.length, 3);
  });

  await test('confirm view: senders prefilled, redirect only for ScaledMail, submit Stage', () => {
    const v = bf.confirmView(meta(2, { p: 'scaledmail', k: [2, 0, 0] }), { senders: 'Avinash Haridas, Aravind Haridas', redirect: 'https://preciseleads.in' });
    assert.strictEqual(v.submit.text, 'Stage (no charge)');
    const ids = v.blocks.filter(b => b.type === 'input').map(b => b.block_id);
    assert.deepStrictEqual(ids, ['senders', 'redirect']);
    assert.strictEqual(v.blocks.find(b => b.block_id === 'senders').element.initial_value, 'Avinash Haridas, Aravind Haridas');
    const z = bf.confirmView(meta(2, { p: 'zapmail', k: [2, 0] }), { senders: '', redirect: '' });
    assert.deepStrictEqual(z.blocks.filter(b => b.type === 'input').map(b => b.block_id), ['senders']);
    assert.ok(!('initial_value' in z.blocks.find(b => b.block_id === 'senders').element));
  });

  await test('20 names fit Slack limits', () => {
    const m = meta(20, { p: 'scaledmail', k: [10, 5, 5] });
    for (const v of [bf.providerView(m), bf.mixView(m), bf.confirmView(m, { senders: 'A B', redirect: '' })]) {
      assert.ok(v.blocks.length <= 100, 'blocks ' + v.blocks.length);
      assert.ok(v.private_metadata.length <= 3000, 'metadata ' + v.private_metadata.length);
    }
  });
```

- [ ] **Step 2: Run, expect FAIL** (`bf.providerView is not a function`)

Run: `node test_buy_flow.js`

- [ ] **Step 3: Add the views to `buy_flow.js`** (above `module.exports`, then extend the exports)

```js
const NL = String.fromCharCode(10);
const fmt = n => Number(n).toLocaleString('en-US');
const usd = v => '$' + Number(v).toFixed(2);
const section = text => ({ type: 'section', text: { type: 'mrkdwn', text: String(text).slice(0, 2900) } });
const context = text => ({ type: 'context', elements: [{ type: 'mrkdwn', text: String(text).slice(0, 2900) }] });
const button = (text, action_id, value, style) => {
  const b = { type: 'button', action_id, text: { type: 'plain_text', text }, value: String(value) };
  if (style) b.style = style;
  return b;
};

function shell(meta, blocks, submit) {
  const v = { type: 'modal', callback_id: 'buy_flow', private_metadata: JSON.stringify(meta),
    title: { type: 'plain_text', text: 'Buy domains + inboxes' }, close: { type: 'plain_text', text: 'Cancel' }, blocks };
  if (submit) v.submit = { type: 'plain_text', text: submit };
  return v;
}

function providerView(meta) {
  const blocks = [section('*' + meta.n.length + ' domain(s) ticked for ' + meta.c + '.* Pick who sets up the inboxes.')];
  for (const prov of ['scaledmail', 'zapmail']) {
    const o = OFFER.providers[prov];
    const ok = usable(meta.n, prov);
    const out = meta.n.filter(p => !ok.includes(p)).map(p => p.d);
    const line = '*' + o.label + '* · ' + o.blurb + NL + ok.length + ' of ' + meta.n.length + ' names available'
      + (out.length ? ' · left out: ' + out.join(', ') : '');
    const b = section(line);
    if (ok.length) b.accessory = button('Use ' + o.label, 'buy_provider', prov, 'primary');
    blocks.push(b);
  }
  return shell({ c: meta.c, ch: meta.ch, n: meta.n }, blocks);
}

function totalsText(meta) {
  const ok = usable(meta.n, meta.p);
  const t = totals(meta.p, ok, meta.k);
  const [from, to] = readyRange(new Date().toISOString().slice(0, 10), meta.p);
  return '*Total:* ' + t.domains + ' domains · ' + t.inboxes + ' inboxes · ~' + fmt(t.perDay) + '/day · ~' + fmt(t.perMonth) + '/mo'
    + NL + 'First charge ' + usd(t.firstChargeUsd) + ' (domains ' + usd(t.domainsUsd) + ' + first month) · then ' + usd(t.monthlyUsd) + '/mo'
    + NL + 'Ready to send around ' + from + ' to ' + to + ' (setup, then 21-30 days of warmup)';
}

function mixView(meta) {
  const types = typesFor(meta.p);
  const ok = usable(meta.n, meta.p);
  const groups = split(ok.map(p => p.d), meta.k);
  const blocks = [section('*' + OFFER.providers[meta.p].label + '* · ' + meta.c + ' · set how many domains get each inbox type'
    + ' (' + OFFER.sending_days_per_month + ' sending days a month).')];
  types.forEach((t, i) => {
    const one = rowMath(t, 1);
    blocks.push(section('*' + t.label + '* · ' + meta.k[i] + ' domain(s)' + NL
      + t.per_domain + ' inboxes/domain · ' + fmt(one.perDay) + '/day · ' + fmt(one.perMonth) + '/mo · ' + usd(one.monthlyUsd) + '/mo per domain'
      + (groups[i].length ? NL + groups[i].join(', ') : '')));
    blocks.push({ type: 'actions', block_id: 'row_' + i, elements: [button('−', 'buy_minus', i), button('+', 'buy_plus', i)] });
  });
  blocks.push(section(totalsText(meta)));
  blocks.push({ type: 'actions', block_id: 'nav', elements: [button('Back', 'buy_back', 'provider'), button('Next', 'buy_next', 'x', 'primary')] });
  return shell(meta, blocks);
}

function confirmView(meta, defaults) {
  const senders = { type: 'plain_text_input', action_id: 'value', placeholder: { type: 'plain_text', text: 'Jane Doe, John Roe' } };
  if (defaults.senders) senders.initial_value = defaults.senders;
  const blocks = [section(totalsText(meta)),
    { type: 'input', block_id: 'senders', label: { type: 'plain_text', text: 'Sender names' }, element: senders,
      hint: { type: 'plain_text', text: 'First and last name each, comma-separated. Spread across the domains.' } }];
  if (meta.p === 'scaledmail') {
    const redirect = { type: 'plain_text_input', action_id: 'value', placeholder: { type: 'plain_text', text: 'https://client.com' } };
    if (defaults.redirect) redirect.initial_value = defaults.redirect;
    blocks.push({ type: 'input', block_id: 'redirect', optional: true, label: { type: 'plain_text', text: 'Redirect to' }, element: redirect });
  }
  blocks.push({ type: 'actions', block_id: 'nav', elements: [button('Back', 'buy_back', 'mix')] });
  blocks.push(context('Stage only prices and records the order. Nothing is bought until an approver presses *Place order* (ScaledMail) or *Approve & start* (Zapmail).'));
  return shell(meta, blocks, 'Stage (no charge)');
}
```

Exports line becomes:
```js
module.exports = { OFFER, typesFor, domainMonthly, rowMath, usable, initialCounts, rebalance, split, totals,
  addBusinessDays, readyRange, providerView, mixView, confirmView };
```

- [ ] **Step 4: Run, expect PASS** — `node test_buy_flow.js` → `10 passed`

- [ ] **Step 5: Commit**

```bash
git add buy_flow.js test_buy_flow.js
git commit -m "Buy flow: provider, inbox-mix and confirm views"
```

---

### Task 4: Suggestions carry prices; one "Buy these" button

**Files:**
- Modify: `domain_suggest_command.js` (`suggestionBlocks` option value + buttons, `selectedDomains`, new `selectedPicks`, exports)
- Modify: `test_domain_suggest_command.js:47` and add tests

**Interfaces:**
- Produces: option `value` = `domain|zapmailPrice|scaledmailPrice` (empty string when not sold), `selectedPicks(stateValues) -> Pick[]`, button `buy_open` (value = client key). `selectedDomains` returns plain names as before.

- [ ] **Step 1: Write the failing tests** — in `test_domain_suggest_command.js` change line 47 to:

```js
  assert.deepStrictEqual(next, ['buy_open', 'domains_suggest_again']);
```

and add:

```js
test('option value carries both prices; selectedPicks reads them back', () => {
  const r = { ...RESULT, suggestions: [
    { domain: 'a.com', price: 12.99, source: 'ai', scaledmail: { available: true, price: 15.5 } },
    { domain: 'b.com', price: null, source: 'scaledmail', scaledmail: { available: true, price: 15.5 } },
    { domain: 'c.com', price: 12.99, source: 'ai', scaledmail: { available: false, price: null } }] };
  const opts = m.suggestionBlocks(r).find(b => b.block_id === 'domains_pick_0').elements[0].options;
  assert.deepStrictEqual(opts.map(o => o.value), ['a.com|12.99|15.5', 'b.com||15.5', 'c.com|12.99|']);
  const state = { domains_pick_0: { domains_pick: { selected_options: opts.map(o => ({ value: o.value })).concat([{ value: 'evil|1|1' }]) } } };
  assert.deepStrictEqual(m.selectedPicks(state), [
    { d: 'a.com', zp: 12.99, sp: 15.5 }, { d: 'b.com', zp: null, sp: 15.5 }, { d: 'c.com', zp: 12.99, sp: null }]);
  assert.deepStrictEqual(m.selectedDomains(state), ['a.com', 'b.com', 'c.com']);
});
```

- [ ] **Step 2: Run, expect FAIL** — `node test_domain_suggest_command.js`

- [ ] **Step 3: Implement in `domain_suggest_command.js`**

In `suggestionBlocks`, the option `value: s.domain` becomes:
```js
          value: optionValue(s)
```
Add above `suggestionBlocks`:
```js
/** "domain|zapmail price|scaledmail price" — prices travel with the tick so the buy flow can cost it. */
function optionValue(s) {
  const p = v => (v === null || v === undefined || !Number.isFinite(Number(v)) ? '' : String(Number(v)));
  const sm = s.scaledmail && s.scaledmail.available ? p(s.scaledmail.price) : '';
  return [s.domain, p(s.price), sm].join('|');
}
```
Replace the two buy buttons in `suggestionBlocks`:
```js
  if (rows.length) {
    buttons.push({ type: 'button', action_id: 'buy_open', style: 'primary',
      text: { type: 'plain_text', text: 'Buy these' }, value: r.client });
  }
```
Replace `selectedDomains` with:
```js
/** Ticked suggestions with their prices: [{d, zp, sp}] (null = not sold there). */
function selectedPicks(stateValues) {
  const out = [];
  const num = v => (v === '' || v === undefined ? null : Number(v));
  for (const block of Object.values(stateValues || {})) {
    const el = block && block.domains_pick;
    if (!el || !Array.isArray(el.selected_options)) continue;
    for (const o of el.selected_options) {
      const [d0, zp, sp] = String(o.value || '').split('|');
      const d = String(d0 || '').toLowerCase();
      if (SAFE_DOMAIN_RE.test(d) && !out.some(x => x.d === d)) out.push({ d, zp: num(zp), sp: num(sp) });
    }
  }
  return out;
}

/** Ticked domain names (older stage / ScaledMail-order buttons). */
function selectedDomains(stateValues) {
  return selectedPicks(stateValues).map(p => p.d);
}
```
Add `selectedPicks` to `module.exports`. Also change the head text `'Tick the ones you want, then *Stage purchase*.'` to `'Tick the ones you want, then *Buy these*.'`.

- [ ] **Step 4: Run, expect PASS** — `node test_domain_suggest_command.js` and `node test_scaledmail_command.js`

- [ ] **Step 5: Commit**

```bash
git add domain_suggest_command.js test_domain_suggest_command.js
git commit -m "Suggestions: ticks carry both prices; one 'Buy these' button"
```

---

### Task 5: Slack handlers + staging + registration

**Files:**
- Modify: `buy_flow.js` (add `registerBuyFlow`, `defaultsFor`, `stageArgs`)
- Modify: `domains_command.js:642` (register)
- Modify: `smartlead_sync/domain_clients.json` (Precise Leads `redirect`)
- Test: `test_buy_flow.js`

**Interfaces:**
- Consumes: Tasks 2-4; `require('./domain_suggest_command').{runPy, selectedPicks, loadProfiles}`; `require('./scaledmail_command').stagedBlocks`; `require('./zapmail_actions').jobBlocks`.
- Produces: `registerBuyFlow(app, baseDir, deps?)` where `deps = {runPy, say, profiles, senders}` override the defaults (tests only); `stageArgs(meta, rowIndex, names, senders, redirect, user) -> string[]`; `defaultsFor(clientKey, profiles, sendersByClient) -> {senders, redirect}`.

- [ ] **Step 1: Write the failing tests** (insert before `console.log(passed …)`)

```js
  await test('stage args: ScaledMail per row, Zapmail inbox job with ; names', () => {
    const m = meta(3, { p: 'scaledmail', k: [2, 1, 0] });
    assert.deepStrictEqual(bf.stageArgs(m, 0, ['a.com', 'b.com'], 'A B, C D', 'https://x.in', 'U1'),
      ['scaledmail_cli.py', '--json', 'stage', '--client', 'Precise Leads', '--provider', 'google', '--domains', 'a.com,b.com',
        '--senders', 'A B, C D', '--user', 'U1', '--per-domain', '3', '--redirect', 'https://x.in']);
    assert.deepStrictEqual(bf.stageArgs(m, 1, ['c.com'], 'A B', '', 'U1').slice(4, 8), ['Precise Leads', '--provider', 'outlook', '--domains']);
    const z = meta(2, { p: 'zapmail', k: [1, 1] });
    assert.deepStrictEqual(bf.stageArgs(z, 1, ['d.com'], 'A B, C D', '', 'U1'),
      ['inbox_jobs.py', '--create', '--client', 'Precise Leads', '--kind', 'new', '--provider', 'MICROSOFT',
        '--domain', 'd.com', '--per-domain', '3', '--names', 'A B;C D', '--by', 'U1', '--json']);
  });

  await test('defaults: senders from inbox profiles, redirect from profile or main domain', () => {
    const profiles = { 'Precise Leads': { main_domain: 'preciseleads.in', redirect: 'https://preciseleads.in' }, Bettrdata: { main_domain: 'bettrdata.io' } };
    const senders = { 'Precise Leads': ['Aravind Haridas', 'Avinash Haridas'], Bettrdata: [] };
    assert.deepStrictEqual(bf.defaultsFor('Precise Leads', profiles, senders), { senders: 'Aravind Haridas, Avinash Haridas', redirect: 'https://preciseleads.in' });
    assert.deepStrictEqual(bf.defaultsFor('Bettrdata', profiles, senders), { senders: '', redirect: 'https://bettrdata.io' });
  });

  // ── handlers with a fake Slack app ──
  function harness(runPy) {
    const h = { actions: {}, views: {}, opened: [], updated: [], said: [] };
    const app = { action: (id, fn) => { h.actions[id] = fn; }, view: (id, fn) => { h.views[id] = fn; } };
    h.client = { views: { open: async a => { h.opened.push(a.view); }, update: async a => { h.updated.push(a.view); } } };
    bf.registerBuyFlow(app, __dirname, {
      runPy: runPy || (async args => ({ staged: true, plan_id: 'abc123', args })),
      say: async (client, channel, user, payload) => { h.said.push(payload); },
      profiles: () => ({ 'Precise Leads': { main_domain: 'preciseleads.in' }, Bettrdata: { main_domain: 'bettrdata.io' } }),
      senders: () => ({ 'Precise Leads': ['Aravind Haridas', 'Avinash Haridas'], Bettrdata: [] })
    });
    return h;
  }
  const noop = async () => {};
  const tickState = { domains_pick_0: { domains_pick: { selected_options: [{ value: 'a.com|12.99|15.5' }, { value: 'b.com|12.99|15.5' }] } } };

  await test('open → provider → plus → next: views redraw with the right state', async () => {
    const h = harness();
    await h.actions.buy_open({ ack: noop, action: { value: 'Precise Leads' }, body: { state: { values: tickState }, trigger_id: 't', channel: { id: 'C1' } }, client: h.client, respond: noop });
    let m = JSON.parse(h.opened[0].private_metadata);
    assert.deepStrictEqual(m.n.map(p => p.d), ['a.com', 'b.com']);
    const view = v => ({ id: 'V1', hash: 'h', private_metadata: v.private_metadata });
    await h.actions.buy_provider({ ack: noop, action: { value: 'scaledmail' }, body: { view: view(h.opened[0]) }, client: h.client });
    m = JSON.parse(h.updated[0].private_metadata);
    assert.deepStrictEqual(m.k, [2, 0, 0]);
    await h.actions.buy_plus({ ack: noop, action: { value: '1' }, body: { view: view(h.updated[0]) }, client: h.client });
    assert.deepStrictEqual(JSON.parse(h.updated[1].private_metadata).k, [1, 1, 0]);
    await h.actions.buy_next({ ack: noop, action: { value: 'x' }, body: { view: view(h.updated[1]) }, client: h.client });
    assert.strictEqual(h.updated[2].submit.text, 'Stage (no charge)');
    assert.strictEqual(h.updated[2].blocks.find(b => b.block_id === 'senders').element.initial_value, 'Aravind Haridas, Avinash Haridas');
  });

  await test('open with nothing ticked says so and opens nothing', async () => {
    const h = harness();
    const said = [];
    await h.actions.buy_open({ ack: noop, action: { value: 'Precise Leads' }, body: { state: { values: {} }, trigger_id: 't' }, client: h.client, respond: async m => said.push(m) });
    assert.strictEqual(h.opened.length, 0);
    assert.match(said[0].text, /Tick at least one/);
  });

  await test('a stale view (hash_conflict) is swallowed', async () => {
    const h = harness();
    h.client.views.update = async () => { const e = new Error('hash_conflict'); e.data = { error: 'hash_conflict' }; throw e; };
    const v = bf.mixView(meta(2, { p: 'scaledmail', k: [2, 0, 0] }));
    await h.actions.buy_plus({ ack: noop, action: { value: '1' }, body: { view: { id: 'V1', hash: 'h', private_metadata: v.private_metadata } }, client: h.client });
  });

  const submitBody = (m, senders, redirect) => ({ user: { id: 'U1' }, view: { private_metadata: JSON.stringify(m), state: { values: {
    senders: { value: { value: senders } }, ...(redirect !== undefined ? { redirect: { value: { value: redirect } } } : {}) } } } });

  await test('submit refuses senders without a last name', async () => {
    const h = harness();
    let acked;
    await h.views.buy_flow({ ack: async a => { acked = a; }, body: submitBody(meta(1, { p: 'zapmail', k: [1, 0] }), 'Avinash'), view: submitBody(meta(1, { p: 'zapmail', k: [1, 0] }), 'Avinash').view, client: h.client });
    assert.strictEqual(acked.response_action, 'errors');
    assert.ok(acked.errors.senders);
  });

  await test('submit stages each non-empty row; a failed row does not stop the others', async () => {
    const calls = [];
    const h = harness(async args => { calls.push(args); if (args.includes('outlook')) throw new Error('boom'); return { staged: true, plan_id: 'abc123', client: 'Precise Leads', provider: 'google', domains: ['a.com'], mailboxes_per_domain: 3, mailboxes: 3, monthly_usd: 10.5, domains_usd: 15.5, senders: ['A B'] }; });
    const m = meta(3, { p: 'scaledmail', k: [1, 1, 1] });
    const b = submitBody(m, 'Avinash Haridas', 'https://preciseleads.in');
    await h.views.buy_flow({ ack: noop, body: b, view: b.view, client: h.client });
    assert.strictEqual(calls.length, 3);
    assert.deepStrictEqual(calls.map(a => a[a.indexOf('--provider') + 1]), ['google', 'outlook', 'smtp']);
    const all = JSON.stringify(h.said);
    assert.match(all, /Microsoft Outlook.*boom/);
    assert.match(all, /abc123/);
  });
```

- [ ] **Step 2: Run, expect FAIL** (`bf.stageArgs is not a function`)

Run: `node test_buy_flow.js`

- [ ] **Step 3: Add to `buy_flow.js`** (above `module.exports`; extend exports with `registerBuyFlow, stageArgs, defaultsFor`)

```js
const NAMES_RE = /^[A-Za-z][A-Za-z .'-]{0,40}(,\s*[A-Za-z][A-Za-z .'-]{0,40}){0,24}$/;
const URL_RE = /^(https?:\/\/)?[a-z0-9.-]+\.[a-z]{2,}(\/[^\s]*)?$/i;

function stageArgs(meta, i, names, senders, redirect, user) {
  const t = typesFor(meta.p)[i];
  if (meta.p === 'scaledmail') {
    const a = ['scaledmail_cli.py', '--json', 'stage', '--client', meta.c, '--provider', t.key, '--domains', names.join(','),
      '--senders', senders, '--user', user];
    if (t.key === 'google') a.push('--per-domain', String(t.per_domain));
    if (redirect) a.push('--redirect', redirect);
    return a;
  }
  return ['inbox_jobs.py', '--create', '--client', meta.c, '--kind', 'new', '--provider', t.key,
    ...names.flatMap(d => ['--domain', d]), '--per-domain', String(t.per_domain),
    '--names', senders.split(',').map(s => s.trim()).filter(Boolean).join(';'), '--by', user, '--json'];
}

function defaultsFor(clientKey, profiles, sendersByClient) {
  const p = profiles[clientKey] || {};
  return { senders: (sendersByClient[clientKey] || []).join(', '),
    redirect: p.redirect || (p.main_domain ? 'https://' + p.main_domain : '') };
}

function loadDefaultSenders(baseDir) {
  try {
    const all = JSON.parse(fs.readFileSync(path.join(baseDir, 'smartlead_sync', 'inbox_profiles.json'), 'utf8')).clients || {};
    return Object.fromEntries(Object.entries(all).map(([k, v]) => [k, v.default_senders || []]));
  } catch (e) { return {}; }
}

function registerBuyFlow(app, baseDir, deps = {}) {
  const suggest = require('./domain_suggest_command');
  const runPy = deps.runPy || (args => suggest.runPy(args, baseDir, 3 * 60 * 1000));
  const say = deps.say || require('./scaledmail_command').say;
  const profiles = deps.profiles || (() => suggest.loadProfiles(baseDir));
  const senders = deps.senders || (() => loadDefaultSenders(baseDir));
  const metaOf = body => JSON.parse(body.view.private_metadata || '{}');
  const redraw = async (client, body, view) => {
    try {
      await client.views.update({ view_id: body.view.id, hash: body.view.hash, view });
    } catch (err) {
      // Two quick clicks: Slack refuses the older redraw. The next click works.
      const code = (err.data && err.data.error) || err.message;
      if (code !== 'hash_conflict') console.error('[buy-flow] redraw failed:', code);
    }
  };

  app.action('buy_open', async ({ ack, action, body, client, respond }) => {
    await ack();
    const picks = suggest.selectedPicks((body.state || {}).values).slice(0, 30);
    if (!picks.length) {
      return respond({ response_type: 'ephemeral', replace_original: false, text: 'Tick at least one domain first, then press *Buy these*.' });
    }
    const meta = { c: String(action.value || ''), ch: (body.channel && body.channel.id) || '', n: picks };
    await client.views.open({ trigger_id: body.trigger_id, view: providerView(meta) });
  });

  app.action('buy_provider', async ({ ack, action, body, client }) => {
    await ack();
    const meta = metaOf(body);
    if (!OFFER.providers[action.value]) return;
    meta.p = action.value;
    meta.k = initialCounts(meta.p, usable(meta.n, meta.p).length);
    await redraw(client, body, mixView(meta));
  });

  for (const [id, delta] of [['buy_plus', 1], ['buy_minus', -1]]) {
    app.action(id, async ({ ack, action, body, client }) => {
      await ack();
      const meta = metaOf(body);
      const i = Number(action.value);
      if (!Number.isInteger(i) || i < 0 || i >= (meta.k || []).length) return;
      meta.k = rebalance(meta.k, i, delta);
      await redraw(client, body, mixView(meta));
    });
  }

  app.action('buy_next', async ({ ack, body, client }) => {
    await ack();
    const meta = metaOf(body);
    await redraw(client, body, confirmView(meta, defaultsFor(meta.c, profiles(), senders())));
  });

  app.action('buy_back', async ({ ack, action, body, client }) => {
    await ack();
    const meta = metaOf(body);
    if (action.value === 'mix') return redraw(client, body, mixView(meta));
    return redraw(client, body, providerView(meta));
  });

  app.view('buy_flow', async ({ ack, body, view, client }) => {
    const v = view.state.values;
    const sendersText = String(((v.senders || {}).value || {}).value || '').trim();
    const redirect = String(((v.redirect || {}).value || {}).value || '').trim();
    const errors = {};
    if (!NAMES_RE.test(sendersText) || !sendersText.split(',').every(n => n.trim().split(/\s+/).length >= 2)) {
      errors.senders = 'First and last name each: "Jane Doe, John Roe"';
    }
    if (redirect && !URL_RE.test(redirect)) errors.redirect = 'A web address like https://client.com';
    if (Object.keys(errors).length) return ack({ response_action: 'errors', errors });
    await ack();
    const meta = JSON.parse(view.private_metadata || '{}');
    const user = body.user.id;
    const groups = split(usable(meta.n, meta.p).map(p => p.d), meta.k);
    const types = typesFor(meta.p);
    await say(client, meta.ch, user, { text: ':hourglass: Staging ' + groups.filter(g => g.length).length + ' order(s) — nothing is bought yet…' });
    for (let i = 0; i < types.length; i++) {
      if (!groups[i].length) continue;
      try {
        const r = await runPy(stageArgs(meta, i, groups[i], sendersText, redirect, user));
        const blocks = meta.p === 'scaledmail' ? require('./scaledmail_command').stagedBlocks(r) : require('./zapmail_actions').jobBlocks(r);
        await say(client, meta.ch, user, { text: types[i].label + ' plan', blocks });
      } catch (err) {
        await say(client, meta.ch, user, { text: ':x: ' + types[i].label + ' (' + groups[i].join(', ') + '): ' + err.message });
      }
    }
  });
}
```

Also export `say` from `scaledmail_command.js` (add `say,` to its `module.exports`).

- [ ] **Step 4: Register in `domains_command.js`** — after line 642 (`suggestFlow.registerDomainSuggestFlow(domainsApp, baseDir);`) add:

```js
  require('./buy_flow').registerBuyFlow(domainsApp, baseDir);
```

- [ ] **Step 5: Add the Precise Leads redirect** — in `smartlead_sync/domain_clients.json`, inside `"Precise Leads": { ... }` add `"redirect": "https://preciseleads.in",` next to `"main_domain"`.

- [ ] **Step 6: Run, expect PASS** — `node test_buy_flow.js` → `17 passed`; then `npm test` → every test file passes.

- [ ] **Step 7: Commit**

```bash
git add buy_flow.js test_buy_flow.js domains_command.js scaledmail_command.js smartlead_sync/domain_clients.json
git commit -m "Buy flow: Slack handlers; Stage makes ScaledMail plans or Zapmail inbox jobs per inbox type"
```

---

### Task 6: Docs + live dry run (no buying)

**Files:**
- Modify: `docs/SCALEDMAIL_INTEGRATION.md` (one paragraph under "Buying domains + mailboxes through the bot")

- [ ] **Step 1: Add the paragraph** after the runbook's step 1 ("Names."):

```markdown
   **Slack, guided:** `/domains suggest <client>` → tick names → **Buy these** → pick ScaledMail or Zapmail → set domains per inbox type with − / + (inboxes, ~emails/day, ~emails/month at 22 sending days, $/mo, first charge and a ready-to-send date update as you click) → **Next** → senders (pre-filled) → **Stage (no charge)**. ScaledMail gets one plan per inbox type with *Place order* for approvers; Zapmail gets one inbox job per type with *Approve & start*. Numbers: `smartlead_sync/inbox_offer.json`.
```

- [ ] **Step 2: Full test run** — `npm test` and `cd smartlead_sync && ./.venv/Scripts/python.exe -m pytest -q --ignore=test_smart_delivery.py`. Expected: all pass.

- [ ] **Step 3: Commit**

```bash
git add docs/SCALEDMAIL_INTEGRATION.md
git commit -m "docs: guided Slack buy flow"
```

- [ ] **Step 4: Ask the user to say "push it"** (Render deploys on push). Do not push without it.

- [ ] **Step 5: Live dry run in Slack after the deploy (the user clicks; nothing is placed)**
  1. `/domains suggest Precise Leads`, tick 3 names, **Buy these**.
  2. ScaledMail: put 2 on Google, 1 on SMTP; check totals (2 Google = 6 inboxes, 120/day, 2,640/mo, $21/mo; 1 SMTP = 4 inboxes, 32/day, 704/mo, $3.75/mo). **Next**, **Stage**. Expect 2 plan cards. Do NOT press Place.
  3. Drop both plans: `scaledmail_cli.py drop <plan_id>` (each).
  4. Repeat with Zapmail, 1 Google + 1 Outlook. Expect 2 job cards. Do NOT approve. Cancel both: `inbox_jobs.py --cancel <job_id> --by manveen`.
  5. Report what matched and what didn't.
