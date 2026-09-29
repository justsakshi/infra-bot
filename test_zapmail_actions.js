// Tests for Zapmail Slack actions + webhooks. No Slack, no network, no Python.
// Run: node test_zapmail_actions.js
const assert = require('assert');
const crypto = require('crypto');

let passed = 0;
const queue = [];
// Tests share env + the call log, so they run one after another.
function test(name, fn) { queue.push([name, fn]); }

// Swap runPy before the actions module grabs it.
const suggest = require('./domain_suggest_command');
const calls = [];
suggest.runPy = async (args) => { calls.push(args); return { ok: true, applied: 3, export_id: 7 }; };
const actions = require('./zapmail_actions');
const hooks = require('./zapmail_webhooks');

/* ---------------------------------------------------------------- webhooks */

function sign(body, secret, t) {
  const v1 = crypto.createHmac('sha256', secret).update(t + '.' + body).digest('hex');
  return 't=' + t + ',v1=' + v1;
}

test('valid signature passes; tampered, stale, or wrong secret fail', () => {
  const body = JSON.stringify({ id: 'evt_1', type: 'mailbox.status_changed' });
  const now = 1790600000;
  const h = sign(body, 's3cret', now);
  assert.strictEqual(hooks.verifySignature(Buffer.from(body), h, 's3cret', now), true);
  assert.strictEqual(hooks.verifySignature(Buffer.from(body + ' '), h, 's3cret', now), false);
  assert.strictEqual(hooks.verifySignature(Buffer.from(body), h, 'other', now), false);
  assert.strictEqual(hooks.verifySignature(Buffer.from(body), h, 's3cret', now + 301), false);
  assert.strictEqual(hooks.verifySignature(Buffer.from(body), '', 's3cret', now), false);
  assert.strictEqual(hooks.verifySignature(Buffer.from(body), 't=1,v1=zz', 's3cret', 1), false);
});

test('secretFor maps account names to env and rejects junk', () => {
  process.env.ZAPMAIL_WEBHOOK_SECRET_PRECISE_LEADS = 'pl';
  process.env.ZAPMAIL_WEBHOOK_SECRET = 'bw';
  assert.strictEqual(hooks.secretFor('precise_leads'), 'pl');
  assert.strictEqual(hooks.secretFor('primary'), 'bw');
  assert.strictEqual(hooks.secretFor('../etc'), null);
  assert.strictEqual(hooks.secretFor('nobody'), null);
});

test('event summaries alert on the things that need people', () => {
  const failed = hooks.summarizeEvent({ type: 'mailbox.status_changed', data: {
    mailboxDetails: { username: 'ann', domain: 'x.com', status: 'FAILED' }, previousState: { status: 'IN_PROGRESS' } } });
  assert.ok(failed.alert && failed.domain === 'x.com' && /ann@x\.com/.test(failed.text));
  const active = hooks.summarizeEvent({ type: 'mailbox.status_changed', data: {
    mailboxDetails: { username: 'ann', domain: 'x.com', status: 'ACTIVE' } } });
  assert.ok(!active.alert && active.domain === 'x.com');       // quiet, but refreshes tracker
  const crit = hooks.summarizeEvent({ type: 'domain.status_changed', data: {
    domainDetails: { domain: 'x.com', status: 'ACTIVE', healthScore: 12 } } });
  assert.ok(crit.alert && /CRITICAL/.test(crit.text));
  assert.ok(hooks.summarizeEvent({ type: 'export.failed', data: { app_name: 'SMARTLEAD', error: 'expired' } }).alert);
  assert.ok(hooks.summarizeEvent({ type: 'placement_test.status_changed', data: { id: 'p', status: 'COMPLETED' } }).alert);
  assert.ok(!hooks.summarizeEvent({ type: 'placement_test.status_changed', data: { id: 'p', status: 'RUNNING' } }).alert);
});

test('webhook route: 503 unconfigured, 401 bad sig, 200 + handled once', async () => {
  const routes = {};
  const app = { post: (p, mw, h) => { routes[p] = h; } };
  const express = { raw: () => null };
  hooks.registerZapmailWebhook(app, express, { baseDir: '.' });
  const h = routes['/webhooks/zapmail/:account'];
  const mkRes = () => { const r = { code: 0, status(c) { r.code = c; return r; }, send() { return r; } }; return r; };
  const body = JSON.stringify({ id: 'evt_9', type: 'placement_test.status_changed', data: { id: 'p', status: 'RUNNING' } });
  const t = Math.floor(Date.now() / 1000);
  let res = mkRes();
  h({ params: { account: 'nobody' }, body: Buffer.from(body), get: () => '' }, res);
  assert.strictEqual(res.code, 503);
  res = mkRes();
  h({ params: { account: 'precise_leads' }, body: Buffer.from(body), get: () => sign(body, 'wrong', t) }, res);
  assert.strictEqual(res.code, 401);
  res = mkRes();
  h({ params: { account: 'precise_leads' }, body: Buffer.from(body), get: () => sign(body, 'pl', t) }, res);
  assert.strictEqual(res.code, 200);
});

/* ------------------------------------------------------------------ blocks */

const HIT = { domain: 'askbettrdata.com', hits: [{ account: 'PRECISE_LEADS', provider: 'MICROSOFT', domain: 'askbettrdata.com',
  domain_id: '0cadd9c6-0e11-44cd-bfab-ed351117e9c4', status: 'ACTIVE', expire_on: '2027-07-29', auto_renew: false,
  health: { score: 90, label: 'healthy' }, client: 'Bettrdata',
  mailboxes: [{ email: 'aaron@askbettrdata.com', status: 'ACTIVE' }] }] };

test('domain view: current client gets actions, past client gets none', () => {
  const blocks = actions.domainBlocks(HIT);
  const ids = blocks.find(b => b.type === 'actions').elements.map(e => e.action_id);
  assert.deepStrictEqual(ids, ['zm_mbx_open', 'zm_export', 'zm_autorenew', 'zm_sync_domain']);
  assert.match(blocks[0].text.text, /Outlook.*health 90/);
  const past = actions.domainBlocks({ domain: 'k.com', hits: [{ ...HIT.hits[0], domain: 'k.com', client: null }] });
  assert.ok(!past.some(b => b.type === 'actions'));
  assert.match(JSON.stringify(past), /not a current client/);
});

test('renewals: buttons only for current clients, past clients listed', () => {
  const blocks = actions.renewalBlocks({ renewals: [
    { domain: 'gomelior.com', domain_id: 'd1', client: 'Melior', expire_on: '2026-10-29', assigned_mailboxes: 0 },
    { domain: 'kombinatorfunds.com', domain_id: 'd2', client: null, expire_on: '2026-10-20' }] });
  const menus = blocks.filter(b => b.accessory && b.accessory.action_id === 'zm_renewal_menu');
  assert.strictEqual(menus.length, 1);
  assert.deepStrictEqual(menus[0].accessory.options.map(o => o.value.split('|')[0]), ['renew', 'autorenew', 'lookup']);
  assert.match(JSON.stringify(blocks), /Past clients \(no action\): kombinatorfunds\.com/);
});

test('checkPayload rejects forged button values', () => {
  assert.strictEqual(actions.checkPayload({ domain: 'a.com', client: 'Melior' }, ['domain', 'client']), null);
  assert.ok(actions.checkPayload({ domain: 'a.com; rm -rf', client: 'Melior' }, ['domain']));
  assert.ok(actions.checkPayload({ domain: 'a.com', client: 'Darlean' }, ['client']));
  assert.ok(actions.checkPayload({ domain_id: '--approve' }, ['domain_id']));
});

/* ---------------------------------------------------------------- handlers */

function fakeApp() {
  const handlers = { action: [], view: {} };
  return {
    handlers,
    action: (id, fn) => handlers.action.push([id, fn]),
    view: (id, fn) => { handlers.view[id] = fn; },
    find(actionId) {
      const hit = handlers.action.find(([id]) => (id instanceof RegExp ? id.test(actionId) : id === actionId));
      return hit && hit[1];
    }
  };
}

test('non-approver pressing Export is refused and nothing runs', async () => {
  delete process.env.ZAPMAIL_APPROVERS;
  const app = fakeApp();
  actions.registerZapmailActions(app, '.', {});
  const said = [];
  calls.length = 0;
  await app.find('zm_export')({ ack: async () => {}, body: { user: { id: 'U1' } },
    action: { value: JSON.stringify({ domain: 'askbettrdata.com', client: 'Bettrdata' }) },
    respond: async (m) => said.push(m.text) });
  assert.strictEqual(calls.length, 0);
  assert.match(said[0], /Only Zapmail approvers/);
});

test('approver Export runs the export CLI for that client', async () => {
  process.env.ZAPMAIL_APPROVERS = 'U1';
  const app = fakeApp();
  actions.registerZapmailActions(app, '.', {});
  calls.length = 0;
  await app.find('zm_export')({ ack: async () => {}, body: { user: { id: 'U1' } },
    action: { value: JSON.stringify({ domain: 'askbettrdata.com', client: 'Bettrdata' }) },
    respond: async () => {} });
  assert.deepStrictEqual(calls[0], ['zapmail_export.py', '--export', 'askbettrdata.com', '--client', 'Bettrdata', '--approve', '--json']);
});

test('approver renew via the renewals menu runs the renew CLI (spend still gated in Python)', async () => {
  process.env.ZAPMAIL_APPROVERS = 'U1';
  const app = fakeApp();
  actions.registerZapmailActions(app, '.', {});
  calls.length = 0;
  await app.find('zm_renewal_menu')({ ack: async () => {}, body: { user: { id: 'U1' } },
    action: { selected_option: { value: 'renew|0cadd9c6-0e11-44cd-bfab-ed351117e9c4|Melior' } },
    respond: async () => {}, client: {} });
  assert.deepStrictEqual(calls[0], ['zapmail_maintenance.py', '--renew', '0cadd9c6-0e11-44cd-bfab-ed351117e9c4', '--approve', '--client', 'Melior', '--json']);
});

test('tracker apply triggers the sheet refresh', async () => {
  process.env.ZAPMAIL_APPROVERS = 'U1';
  const app = fakeApp();
  let refreshed = 0;
  actions.registerZapmailActions(app, '.', { onTrackerChanged: () => { refreshed += 1; } });
  await app.find('zm_sync_apply')({ ack: async () => {}, body: { user: { id: 'U1' } }, respond: async () => {} });
  assert.strictEqual(refreshed, 1);
});

test('mailbox form rejects bad sender names and caps count at 5', async () => {
  process.env.ZAPMAIL_APPROVERS = 'U1';
  const app = fakeApp();
  actions.registerZapmailActions(app, '.', {});
  let acked;
  await app.handlers.view.zm_mbx_submit({
    ack: async (x) => { acked = x; }, body: { user: { id: 'U1' } },
    view: { private_metadata: JSON.stringify({ domain: 'gomelior.com', client: 'Melior' }),
      state: { values: { names: { value: { value: 'Jane; --approve' } }, count: { value: { selected_option: { value: '9' } } } } } },
    client: { chat: { postMessage: async () => {} } } });
  assert.ok(acked && acked.response_action === 'errors');
  calls.length = 0;
  await app.handlers.view.zm_mbx_submit({
    ack: async () => {}, body: { user: { id: 'U1' } },
    view: { private_metadata: JSON.stringify({ domain: 'gomelior.com', client: 'Melior', channel: 'C1' }),
      state: { values: { names: { value: { value: 'Jane Doe, John Roe' } }, count: { value: { selected_option: { value: '9' } } } } } },
    client: { chat: { postMessage: async () => {} } } });
  assert.deepStrictEqual(calls[0].slice(0, 5), ['zapmail_lifecycle.py', '--mailboxes', 'gomelior.com', '--per-domain', '5']);
  assert.deepStrictEqual(calls[0].slice(-2), ['--names', 'Jane Doe, John Roe']);
});

test('home menu has the navigation and action entry points', () => {
  const ids = actions.homeBlocks().filter(b => b.type === 'actions').flatMap(b => b.elements.map(e => e.action_id));
  for (const id of ['zm_nav_status', 'zm_nav_renewals', 'zm_nav_batches', 'zm_nav_digest', 'zm_home_suggest',
    'zm_lookup_open', 'zm_nav_prewarmed', 'zm_nav_sync', 'zm_nav_cross-check']) {
    assert.ok(ids.includes(id), 'missing ' + id);
  }
});

(async () => {
  for (const [name, fn] of queue) {
    try { await fn(); passed += 1; } catch (err) { console.error('FAIL', name, '\n ', err.stack || err.message); process.exitCode = 1; }
  }
  console.log(passed + ' passed' + (process.exitCode ? ' (with failures)' : ''));
})();
