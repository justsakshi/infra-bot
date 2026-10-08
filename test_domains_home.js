// Domain Suggester as the one front door: DMs / agent tab, the home menu,
// /domains infra …, and the shared tracker features.
// Run: node test_domains_home.js   (no Slack, no Mongo, no network)
const assert = require('assert');
const dayjs = require('dayjs');

let n = 0;
const ok = (c, m) => { assert.ok(c, m); n++; };

// ── fake Mongo model for the tracker ─────────────────────────────────────
const rows = [
  { name: 'a@x.com', type: 'INBOX', status: 'Active', client: 'Melior', provider: 'Zapmail', expiryDate: dayjs().startOf('day').toDate() },
  { name: 'x.com', type: 'DOMAIN', status: 'Active', client: 'Melior', provider: 'Zapmail', expiryDate: dayjs().add(5, 'day').toDate() },
  { name: 'old.com', type: 'DOMAIN', status: 'Active', client: 'Bettrdata', provider: 'Zapmail', expiryDate: dayjs().add(40, 'day').toDate() }
];
const match = (r, q) => {
  if (q.status && r.status !== q.status) return false;
  if (q.name && q.name.$regex) return q.name.$regex.test(r.name);
  if (typeof q.name === 'string') return r.name === q.name;
  return true;
};
const Asset = {
  find: (q = {}) => { const out = rows.filter(r => match(r, q)); out.sort = () => out; return out; },
  findOne: async q => rows.find(r => match(r, q)) || null,
  findOneAndUpdate: async (q, u) => { const r = rows.find(x => match(x, q)); Object.assign(r, u); return r; },
  deleteOne: async q => { const i = rows.findIndex(r => match(r, q)); if (i >= 0) rows.splice(i, 1); return { deletedCount: i >= 0 ? 1 : 0 }; }
};
let synced = 0;
const deps = {
  Asset, dayjs, axios: null, syncAllAssetsToSheet: async () => { synced++; },
  prepareAssetsForSheet: list => list.map(a => ({ ...a, daysLeft: a.expiryDate ? dayjs(a.expiryDate).startOf('day').diff(dayjs().startOf('day'), 'day') : null })),
  parseDate: () => null, parseCost: () => null, resolveOwner: () => null, computeDaysLeft: () => 0,
  parseCSVBuffer: async () => [], buildNameQuery: name => ({ name: { $regex: new RegExp('^' + name.replace(/\./g, '\\.') + '$', 'i') } })
};

const infraSlack = require('./infra_slack');
const handlers = { actions: {}, views: {}, events: {}, messages: [] };
const fakeApp = {
  command: (name, fn) => { handlers.actions['cmd:' + name] = fn; },
  action: (id, fn) => { handlers.actions[String(id)] = fn; },
  view: (id, fn) => { handlers.views[id] = fn; },
  event: (id, fn) => { handlers.events[id] = fn; },
  message: fn => { handlers.messages.push(fn); }
};
const infra = infraSlack.registerInfraHandlers(fakeApp, deps, { botToken: 'xoxb-test' });
require('./domains_command').registerDomainsCommand(fakeApp, __dirname, { infra });
require('./domains_home').registerDomainsHome(fakeApp, __dirname, { infra, botToken: 'xoxb-test' });

const posted = [];
const client = { chat: { postMessage: async m => { posted.push(m); return { ok: true }; } }, views: { open: async v => { posted.push({ modal: v.view.callback_id }); } } };
const dm = text => handlers.messages[0]({ message: { channel_type: 'im', channel: 'D0BR7HVV0QJ', user: 'U1', text, ts: '1.1', thread_ts: '1.0' }, client });

(async () => {
  // ── tracker features ───────────────────────────────────────────────────
  const exp = await infra.expiring(7);
  ok(exp.map(r => r.name).join(',') === 'a@x.com,x.com', 'expiring 7 days: today + 5d, not 40d');
  const blocks = infraSlack.expiringBlocks(exp, 7);
  ok(blocks.length === 2 && blocks[1].accessory.action_id === 'infra_mark_renewed' && /\*today\*/.test(blocks[1].text.text), 'expiring view, one Mark renewed per client');
  const r = await infra.markRenewed(['A@X.com', 'nope.com']);
  ok(r.renewed === 1 && r.notFound === 1 && synced === 1, 'mark renewed is case-insensitive and resyncs the sheet');
  ok(dayjs(rows[0].expiryDate).diff(dayjs().startOf('day'), 'month') === 1, 'inbox renewed for 1 month');

  // ── DMs / agent tab ────────────────────────────────────────────────────
  posted.length = 0;
  await dm('hi');
  ok(posted[0] && posted[0].text === 'Domains & inboxes' && posted[0].thread_ts === '1.0', '"hi" → home menu, in the agent thread');
  const ids = posted[0].blocks.filter(b => b.type === 'actions').flatMap(b => b.elements.map(e => e.action_id));
  ['u_nav_status', 'u_nav_renewals', 'u_lookup_open', 'u_nav_purchases', 'sm_order_open', 'dh_audit', 'infra_expiring_0', 'infra_expiring_7', 'infra_add_open', 'infra_renew_open', 'infra_list']
    .forEach(id => ok(ids.includes(id), 'home has ' + id));
  ['dh_suggest', 'dh_audit', 'infra_add_open', 'infra_renew_open', 'infra_list', 'infra_mark_renewed']
    .forEach(id => ok(typeof handlers.actions[id] === 'function', 'button handled: ' + id));
  ok(Object.keys(handlers.actions).some(k => k.includes('infra_expiring')), 'expiring buttons handled');
  ok(typeof handlers.events.assistant_thread_started === 'function', 'agent tab greeting handled');

  posted.length = 0;
  await dm('sm');
  ok(posted[0] && posted[0].text === 'ScaledMail' && !('response_type' in posted[0]), '"sm" → ScaledMail menu in the DM');
  posted.length = 0;
  await dm('zapmail');
  ok(posted[0] && posted[0].text === 'Zapmail', '"zapmail" → Zapmail menu');
  posted.length = 0;
  await dm('infra expiring 7');
  ok(posted[0] && posted[0].text === 'Expiring assets', '"infra expiring 7" → tracker view');
  posted.length = 0;
  await dm('infra add');
  ok(posted[0] && posted[0].blocks[0].elements[0].action_id === 'infra_add_open', '"infra add" in a DM → a button (no trigger to open a form)');
  posted.length = 0;
  await dm('/domains help');
  ok(posted[0] && /Quickest/.test(posted[0].text), 'typing "/domains help" as a message works');
  posted.length = 0;
  await dm('renew\nx.com');
  ok(posted[0] && /Renewed 1 asset/.test(posted[0].text), 'renew + names list, like Infra Bot');
  posted.length = 0;
  await handlers.messages[0]({ message: { channel_type: 'channel', channel: 'C1', user: 'U1', text: 'hi', ts: '1' }, client });
  await handlers.messages[0]({ message: { channel_type: 'im', channel: 'D1', bot_id: 'B1', text: 'hi', ts: '1' }, client });
  ok(posted.length === 0, 'channel messages and bot messages are ignored');

  // ── access: events now carry the user ──────────────────────────────────
  const { userOf } = require('./domains_access');
  ok(userOf({ type: 'event_callback', event: { user: 'U9' } }) === 'U9', 'DM event user is read (was dropped before)');
  ok(userOf({ event: { assistant_thread: { user_id: 'U8' } } }) === 'U8', 'agent-tab event user is read');

  console.log(n + ' passed');
})().catch(e => { console.error(e); process.exit(1); });
