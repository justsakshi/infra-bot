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
