// "Name & signature" in Slack: button, form, result text. Run: node test_inbox_slack.js
const assert = require('assert');
const { domainBlocks, inboxModal, inboxResultText } = require('./zapmail_actions');

let n = 0;
const hit = { domain: 'askbettrdata.com', client: 'Bettrdata', account: 'PRECISE_LEADS', status: 'ACTIVE',
  health: { score: 90 }, domain_id: 'd1',
  mailboxes: [{ email: 'aaron@askbettrdata.com', status: 'ACTIVE' }, { email: 'aarond@askbettrdata.com', status: 'ACTIVE' }] };

const btns = domainBlocks({ hits: [hit] }).filter(b => b.type === 'actions')[0].elements;
const b = btns.find(e => e.action_id === 'zm_inbox_open');
assert.ok(b, 'button shown when the domain has inboxes'); n++;
assert.deepStrictEqual(JSON.parse(b.value).inboxes, ['aaron@askbettrdata.com', 'aarond@askbettrdata.com']); n++;

const none = domainBlocks({ hits: [{ ...hit, mailboxes: [] }] }).filter(x => x.type === 'actions')[0].elements;
assert.ok(!none.find(e => e.action_id === 'zm_inbox_open'), 'no button without inboxes'); n++;

const m = inboxModal({ domain: 'askbettrdata.com', client: 'Bettrdata', inboxes: ['aaron@askbettrdata.com', 'EVIL<script>'] }, 'C1');
assert.strictEqual(m.callback_id, 'zm_inbox_submit');
assert.strictEqual(m.blocks[0].element.options.length, 1, 'only real addresses offered'); n++;
assert.ok(/never changes/.test(JSON.stringify(m.blocks)), 'says the address is kept'); n++;

const ok = inboxResultText({ client: 'Bettrdata', changes: [{ email: 'aaron@askbettrdata.com',
  fields: { from_name: 'Aaron Dixon' }, before: { from_name: 'Aaron Dix' } }], results: [{ ok: true, done: ['from_name', 'zapmail'] }] });
assert.ok(/Aaron Dix\* → \*Aaron Dixon/.test(ok) && /white_check_mark/.test(ok), ok); n++;

const noTmpl = inboxResultText({ client: 'Bettrdata', changes: [{ email: 'a@x.com',
  error: 'no signature template for this client yet (inbox_profiles.json)' }] });
assert.ok(/:x:/.test(noTmpl) && /no signature template/.test(noTmpl)); n++;

const partial = inboxResultText({ client: 'Melior', changes: [{ email: 'a@x.com', fields: { from_name: 'A B' }, before: {} }],
  results: [{ ok: true, done: ['from_name'], zapmail_error: 'not found on this client’s Zapmail account' }] });
assert.ok(/Zapmail’s name did not change/.test(partial)); n++;

assert.ok(/nothing to change/.test(inboxResultText({ changes: [{ email: 'a@x.com', fields: {}, before: {} }], results: [{ ok: true, done: [] }] }))); n++;
console.log(n + ' passed');
