// One /domains for Zapmail + ScaledMail: routing, merged views, billing.
// Run: node test_domains_unified.js   (Python is stubbed; no network)
const assert = require('assert');
let n = 0;
const ok = (c, m) => { assert.ok(c, m); n++; };

// Stub the Python runner before anything loads it.
const calls = [];
const flow = require('./domain_suggest_command');
flow.runPy = async (args) => {
  calls.push(args.join(' '));
  const a = args.join(' ');
  if (a.includes('zapmail_status.py --domain')) return { domain: 'x.com', hits: [] };
  if (a.includes('scaledmail_cli.py --json domain meliorbuild.com')) return { domain: 'meliorbuild.com', found: true, order: null,
    row: { domain: 'meliorbuild.com', provider: 'google', status: 'Active', mailboxes: 3, client: 'Melior', order: '30 × Google',
      order_status: 'Active', order_id: 'rec0FpQhXI7AdldYc', billing_day: '2026-11-06', renewal_at: '2027-07-06', renewal_price: 15.12,
      redirect: 'getmelior.com', masking: false, mailbox_rows: [] } };
  if (a.includes('scaledmail_cli.py --json domain')) return { domain: 'x.com', found: false, inventory: null };
  if (a.includes('--billing')) return { days: 14, billing_errors: [], billing: [
    { bills_on: new Date().toISOString().slice(0, 10), price: 48.75, mailboxes: 15, provider: 'MICROSOFT', kind: 'inboxes',
      account: 'PRECISE_LEADS', clients: ['Bettrdata'], payment_failure: 'No payment method on file', domains: ['askbettrdata.com'] },
    { bills_on: '2099-01-01', price: 9, mailboxes: 3, provider: 'GOOGLE', kind: 'inboxes', account: 'X', clients: [], domains: [] }] };
  if (a.includes('--renewals')) return { renewals: [], renewal_errors: [] };
  if (a.includes('scaledmail_cli.py --json renewals')) return { billing: [], domains: [], billing_days: 14, domain_days: 60 };
  return { error: 'unexpected ' + a };
};
const u = require('./domains_unified');

(async () => {
  ok(u.routeText('').home && u.routeText('  ').home, 'empty → menu');
  ok(u.routeText('renewals').sub === 'renewals' && u.routeText('domain a.com').args[0] === 'a.com', 'views route');
  ok(u.routeText('bettrdata.io data,ingest') === null && u.routeText('suggest melior') === null, 'name generation still goes to the generator');
  ok(u.planFor('domain', ['--approve']) === null, 'flag cannot pass as a domain');
  ok(u.planFor('status').map(p => p.provider).join() === 'Zapmail,ScaledMail', 'status asks both providers');
  ok(u.planFor('renewals').length === 3, 'renewals = Zapmail inbox billing + Zapmail domains + ScaledMail');

  const r = await u.runView('renewals', [], __dirname);
  const txt = JSON.stringify(r.blocks);
  ok(/No payment method on file/.test(txt) && /askbettrdata/.test(txt), 'failed payment shown on the billing line');
  ok(!/2099-01-01/.test(txt), 'only the next 14 days');
  ok(/Zapmail inbox billing/.test(txt) && /ScaledMail/.test(txt), 'one message, both providers');
  ok(r.blocks.length <= 49, 'within Slack block limit');

  calls.length = 0;
  const d = await u.runView('domain', ['meliorbuild.com'], __dirname);
  ok(calls.length === 2, 'domain look-up asks both providers');
  ok(!/\*Zapmail\*/.test(JSON.stringify(d.blocks)) && /meliorbuild/.test(JSON.stringify(d.blocks)), 'shows only where it lives');
  const none = await u.runView('domain', ['nothere.com'], __dirname);
  ok(/not on Zapmail or ScaledMail/.test(none.text), 'neither → one clear line');

  const home = u.homeBlocks(__dirname);
  const ids = home.filter(b => b.type === 'actions').flatMap(b => b.elements.map(e => e.action_id));
  ok(ids.some(i => /^domains_suggest_/.test(i)), 'menu has the per-client suggest buttons');
  ok(!ids.includes('dh_zapmail') && !ids.includes('sm_home'), 'no separate Zapmail / ScaledMail menus');
  console.log(n + ' passed');
})().catch(e => { console.error(e); process.exit(1); });

// ── renewal review view ─────────────────────────────────────────────────
(async () => {
  const blocks = u.reviewBlocks({ days: 14, errors: [], reviews: [
    { provider: 'Zapmail', label: '15 Outlook inboxes', bills_on: '2026-10-10', price: 48.75, counts: { KEEP: 1, RETIRE: 2, CHECK: 0 }, retire_saves_monthly: 6.5,
      rows: [{ email: 'a@x.com', verdict: 'RETIRE', why: '47%', client: 'Bettrdata', campaigns: ['C'] },
             { email: 'b@y.com', verdict: 'RETIRE', why: 'spam domain', client: 'Melior', campaigns: [] },
             { email: 'c@z.com', verdict: 'KEEP', why: '100%', client: 'Bettrdata' }] },
    { provider: 'ScaledMail', label: '39 × Google', bills_on: '2026-10-08', price: 136.5, counts: { KEEP: 0, RETIRE: 1, CHECK: 0 }, retire_saves_monthly: 3.5,
      rows: [{ email: 'd@w.com', verdict: 'RETIRE', why: '60%', client: 'Precise Leads' }] }] });
  const txt = JSON.stringify(blocks);
  const retire = blocks.filter(b => b.type === 'actions').flatMap(b => b.elements);
  assert.ok(/\$10\/month/.test(txt), 'total savings');
  assert.deepStrictEqual(retire.map(e => e.action_id).sort(), ['u_retire_bettrdata', 'u_retire_melior'], 'one retire button per client, Zapmail only');
  assert.ok(retire.every(e => e.style === 'danger' && e.confirm), 'retire asks first');
  assert.ok(/Replace domain/.test(txt), 'ScaledMail gets the per-order explanation');
  assert.ok(u.routeText('review').sub === 'review' && u.planFor('review')[0].cli.includes('renewal_review.py'), 'typed /domains review');
  console.log('review view: 5 passed');
})().catch(e => { console.error(e); process.exit(1); });

// Kill warnings view
{
  const u2 = require('./domains_unified');
  assert.ok(u2.routeText('kill').sub === 'kill' && u2.planFor('kill')[0].cli.includes('kill_report.py'), 'typed /domains kill');
  const ids2 = u2.homeBlocks(__dirname).filter(b => b.type === 'actions').flatMap(b => b.elements.map(e => e.action_id));
  assert.ok(ids2.includes('u_nav_kill'), 'menu has Kill warnings');
  console.log('kill view: 2 passed');
}
