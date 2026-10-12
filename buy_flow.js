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

/** Set row i to n domains (clamped), moving the difference to/from other rows. */
function setCount(counts, i, n) {
  let c = counts.slice();
  const want = Math.max(0, Math.min(c.reduce((a, b) => a + b, 0), Math.floor(Number(n) || 0)));
  for (let guard = 0; c[i] !== want && guard < 1000; guard++) {
    const next = rebalance(c, i, c[i] < want ? 1 : -1);
    if (next[i] === c[i]) break;
    c = next;
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
  return shell({ c: meta.c, ch: meta.ch, n: meta.n, ...(meta.ru ? { ru: meta.ru } : {}) }, blocks);
}

const plural = (n, word) => n + ' ' + word + (n === 1 ? '' : 's');
const divider = () => ({ type: 'divider' });
const field = (label, value) => ({ type: 'mrkdwn', text: '*' + label + '*' + NL + value });

/** The running total as a two-column grid, then the ready-to-send dates. */
function totalsBlocks(meta) {
  const t = totals(meta.p, usable(meta.n, meta.p), meta.k);
  const [from, to] = readyRange(new Date().toISOString().slice(0, 10), meta.p);
  return [
    { type: 'section', fields: [field('Domains', t.domains), field('Inboxes', t.inboxes),
      field('Per day', '~' + fmt(t.perDay)), field('Per month', '~' + fmt(t.perMonth)),
      field('First charge', usd(t.firstChargeUsd)), field('Then', usd(t.monthlyUsd) + '/mo')] },
    context('First charge = domains ' + usd(t.domainsUsd) + ' + first month. Ready to send around ' + from + ' to ' + to
      + ' (setup, then 21-30 days of warmup).')];
}

/** Domain-count picker for one row: "0 domains" … "N domains". */
function countPicker(i, max, current) {
  const options = Array.from({ length: Math.min(max, 99) + 1 },
    (_, n) => ({ text: { type: 'plain_text', text: plural(n, 'domain') }, value: i + '|' + n }));
  return { type: 'static_select', action_id: 'buy_count', options,
    initial_option: options[Math.min(current, options.length - 1)] };
}

function mixView(meta) {
  const types = typesFor(meta.p);
  const ok = usable(meta.n, meta.p);
  const groups = split(ok.map(p => p.d), meta.k);
  const blocks = [section('*' + OFFER.providers[meta.p].label + '* · ' + meta.c + NL
    + 'Pick how many domains get each inbox type (' + OFFER.sending_days_per_month + ' sending days a month).'), divider()];
  types.forEach((t, i) => {
    const one = rowMath(t, 1);
    const row = section('*' + t.label + '*' + NL + t.per_domain + ' inboxes/domain · ' + fmt(one.perDay) + '/day · '
      + fmt(one.perMonth) + '/mo · ' + usd(one.monthlyUsd) + '/mo per domain');
    row.accessory = countPicker(i, ok.length, meta.k[i]);
    blocks.push(row);
    if (groups[i].length) blocks.push(context(groups[i].map(d => '`' + d + '`').join('  ')));
    blocks.push(divider());
  });
  blocks.push(...totalsBlocks(meta));
  blocks.push({ type: 'actions', block_id: 'nav', elements: [button('Back', 'buy_back', 'provider'), button('Next', 'buy_next', 'x', 'primary')] });
  return shell(meta, blocks);
}

function confirmView(meta, defaults) {
  const senders = { type: 'plain_text_input', action_id: 'value', placeholder: { type: 'plain_text', text: 'Jane Doe, John Roe' } };
  if (defaults.senders) senders.initial_value = defaults.senders;
  const blocks = [...totalsBlocks(meta), divider(),
    { type: 'input', block_id: 'senders', label: { type: 'plain_text', text: 'Sender names' }, element: senders,
      hint: { type: 'plain_text', text: 'First and last name each, comma-separated. Spread across the domains.' } }];
  if (meta.p === 'scaledmail') {
    const redirect = { type: 'plain_text_input', action_id: 'value', placeholder: { type: 'plain_text', text: 'https://client.com' } };
    if (defaults.redirect) redirect.initial_value = defaults.redirect;
    blocks.push({ type: 'input', block_id: 'redirect', optional: true, label: { type: 'plain_text', text: 'Redirect to' }, element: redirect });
  }
  const sigOpt = { text: { type: 'plain_text', text: 'Add ' + meta.c + '’s signature in Smartlead' }, value: 'yes' };
  blocks.push({ type: 'input', block_id: 'sig', optional: true, label: { type: 'plain_text', text: 'Signature' },
    element: { type: 'checkboxes', action_id: 'value', options: [sigOpt], initial_options: [sigOpt] },
    hint: { type: 'plain_text', text: meta.p === 'zapmail'
      ? 'Added by the job once the inboxes reach Smartlead.'
      : 'ScaledMail inboxes reach Smartlead later; the 9:40 message says when to add it.' } });
  blocks.push({ type: 'actions', block_id: 'nav', elements: [button('Back', 'buy_back', 'mix')] });
  blocks.push(context('Stage only prices and records the order. Nothing is bought until an approver presses *Place order* (ScaledMail) or *Approve & start* (Zapmail).'));
  return shell(meta, blocks, 'Stage (no charge)');
}

const NAMES_RE = /^[A-Za-z][A-Za-z .'-]{0,40}(,\s*[A-Za-z][A-Za-z .'-]{0,40}){0,24}$/;
const URL_RE = /^(https?:\/\/)?[a-z0-9.-]+\.[a-z]{2,}(\/[^\s]*)?$/i;

function stageArgs(meta, i, names, senders, redirect, user, { signature = false } = {}) {
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
    '--names', senders.split(',').map(s => s.trim()).filter(Boolean).join(';'),
    ...(signature ? ['--signature'] : []), '--by', user, '--json'];
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
  const post = deps.post || ((url, payload) => require('axios').post(url, payload));
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
      return require('./slack_reply').add(respond, { text: 'Tick at least one domain first, then press *Buy these*.' });
    }
    // ru: the suggestions message's response_url, so Stage can replace it.
    const meta = { c: String(action.value || ''), ch: (body.channel && body.channel.id) || '', n: picks,
      ru: String(body.response_url || '') };
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

  app.action('buy_count', async ({ ack, action, body, client }) => {
    await ack();
    const meta = metaOf(body);
    const [i, n] = String((action.selected_option || {}).value || '').split('|').map(Number);
    if (!Number.isInteger(i) || i < 0 || i >= (meta.k || []).length || !Number.isInteger(n)) return;
    meta.k = setCount(meta.k, i, n);
    await redraw(client, body, mixView(meta));
  });

  // − / + from forms opened before the count picker (kept so those still work).
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
    const signature = (((v.sig || {}).value || {}).selected_options || []).length > 0;
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
    // One message per conversation: through the suggestions message's
    // response_url (30 min, 5 uses: Staging + up to 3 cards + summary) the
    // suggestions become "Staging…" then a summary, and each card is its own
    // message so Place / Approve can replace just that card. If the link has
    // expired, everything is posted the old way.
    let viaUrl = /^https:\/\/hooks\.slack\.com\//.test(meta.ru || '');
    const send = async (payload, replaceOriginal) => {
      if (viaUrl) {
        try {
          await post(meta.ru, { response_type: 'ephemeral', replace_original: replaceOriginal, ...payload });
          return;
        } catch (err) {
          viaUrl = false;
          console.error('[buy-flow] response_url failed, posting instead:', err.message);
        }
      }
      await say(client, meta.ch, user, payload);
    };
    const rows = groups.filter(g => g.length).length;
    let staged = 0;
    await send({ text: ':hourglass: Staging ' + rows + ' order(s) for ' + meta.c + ' — nothing is bought yet…' }, true);
    for (let i = 0; i < types.length; i++) {
      if (!groups[i].length) continue;
      try {
        const r = await runPy(stageArgs(meta, i, groups[i], sendersText, redirect, user, { signature }));
        const blocks = meta.p === 'scaledmail' ? require('./scaledmail_command').stagedBlocks(r) : require('./zapmail_actions').jobBlocks(r);
        if (!r.error && (r.staged || r.job_id)) staged += 1;
        // Row label first: an answered error renders as a bare ':x:' block otherwise.
        await send({ text: types[i].label + ' plan',
          blocks: [context('*' + types[i].label + '* · ' + groups[i].map(d => '`' + d + '`').join('  ')), ...blocks] }, false);
      } catch (err) {
        await send({ text: ':x: ' + types[i].label + ' (' + groups[i].join(', ') + '): ' + err.message }, false);
      }
    }
    if (viaUrl) {
      const { screen } = require('./slack_reply');
      await send(screen({ text: ':memo: Staged ' + staged + ' order(s) of ' + rows + ' for *' + meta.c + '* on '
        + OFFER.providers[meta.p].label + ' — the cards are below. Nothing is bought until an approver presses '
        + (meta.p === 'scaledmail' ? '*Place order*.' : '*Approve & start*.') }), true);
    }
  });
}

module.exports = { OFFER, typesFor, domainMonthly, rowMath, usable, initialCounts, rebalance, setCount, split, totals,
  addBusinessDays, readyRange, providerView, mixView, confirmView, registerBuyFlow, stageArgs, defaultsFor };
