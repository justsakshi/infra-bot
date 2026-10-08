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
