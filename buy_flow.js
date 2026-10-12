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

module.exports = { OFFER, typesFor, domainMonthly, rowMath, usable, initialCounts, rebalance, split, totals,
  addBusinessDays, readyRange, providerView, mixView, confirmView };
