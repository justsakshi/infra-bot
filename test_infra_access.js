// /infra and tracker-changing messages: team allow-list only.
// Run: node test_infra_access.js   (no Slack, no Mongo)
const assert = require('assert');
const { registerInfraHandlers, isInfraMessage, LOCKED } = require('./infra_slack');

let n = 0;
const ok = (c, m) => { assert.ok(c, m); n++; };

const rows = [{ name: 'x.com', type: 'DOMAIN', status: 'Active' }];
let deletes = 0;
const deps = {
  Asset: {
    find: () => { const r = [...rows]; r.sort = () => r; return r; },
    findOne: async () => rows[0], findOneAndUpdate: async () => rows[0],
    deleteOne: async () => { deletes++; return { deletedCount: 1 }; }
  },
  dayjs: require('dayjs'), axios: null, syncAllAssetsToSheet: async () => {}, prepareAssetsForSheet: l => l,
  parseDate: () => null, parseCost: () => null, resolveOwner: () => null, computeDaysLeft: () => 0,
  parseCSVBuffer: async () => [], buildNameQuery: name => ({ name })
};
const h = { cmd: null, msg: null, views: {}, actions: {} };
registerInfraHandlers({
  command: (_, fn) => { h.cmd = fn; }, message: fn => { h.msg = fn; },
  view: (id, fn) => { h.views[id] = fn; }, action: (id, fn) => { h.actions[String(id)] = fn; }
}, deps, { command: '/infra', messages: true, botToken: 'x', allowed: u => u === 'UOK' });

const posted = [];
const client = { chat: { postMessage: async m => { posted.push(m); } }, views: { open: async () => {} } };

(async () => {
  ok(isInfraMessage({ text: 'delete\nx.com' }) && isInfraMessage({ files: [{ name: 'delete_domains.csv' }] }), 'recognises tracker-changing messages');
  ok(!isInfraMessage({ text: 'delete this later' }) && !isInfraMessage({ text: 'hello' }), 'ordinary chat is not one');

  posted.length = 0;
  await h.msg({ message: { channel: 'C1', user: 'USTRANGER', text: 'delete\nx.com' }, client });
  ok(deletes === 0 && posted[0] && posted[0].text === LOCKED, 'a stranger cannot delete tracker rows');

  posted.length = 0;
  await h.msg({ message: { channel: 'C1', user: 'USTRANGER', text: 'good morning' }, client });
  ok(posted.length === 0, 'ordinary messages are ignored, no lock spam');

  posted.length = 0;
  await h.msg({ message: { channel: 'C1', user: 'UOK', text: 'delete\nx.com' }, client });
  ok(deletes === 1 && /Deleted 1 asset/.test(posted[0].text), 'a team member can');

  const out = [];
  await h.cmd({ ack: async () => {}, body: { user_id: 'USTRANGER', trigger_id: 't' }, client, command: { text: 'list' }, respond: async m => out.push(m) });
  ok(out[0] && out[0].text === LOCKED, '/infra refused for a stranger');

  let acked = false;
  posted.length = 0;
  await h.views.ADD_ASSET_MODAL({ ack: async () => { acked = true; }, body: { user: { id: 'USTRANGER' } }, view: { state: { values: {} } }, client });
  ok(acked && posted[0].text === LOCKED, 'add-asset form refused for a stranger');

  const r2 = [];
  await h.actions.infra_mark_renewed({ ack: async () => {}, body: { user: { id: 'USTRANGER' } }, action: { value: '{"names":["x.com"]}' }, respond: async m => r2.push(m), client });
  ok(r2[0] && r2[0].text === LOCKED, 'mark renewed refused for a stranger');
  console.log(n + ' passed');
})().catch(e => { console.error(e); process.exit(1); });
