// Only allowed people get past the domains app's front door.
// Run: node test_domains_access.js
const assert = require('assert');
const { accessMiddleware, isAllowed } = require('./domains_access');

function run(body) {
  const seen = { acked: false, next: false, replies: [] };
  return accessMiddleware({
    body,
    ack: async () => { seen.acked = true; },
    respond: async (m) => { seen.replies.push(m); },
    next: async () => { seen.next = true; }
  }).then(() => seen);
}

(async () => {
  let n = 0;
  process.env.DOMAINS_ALLOWED_USERS = 'UTEAM1, UTEAM2';
  process.env.ZAPMAIL_APPROVERS = 'UBOSS';
  delete process.env.ZAPMAIL_BUY_APPROVERS;

  assert.ok(isAllowed('UTEAM2')); n++;
  assert.ok(isAllowed('UBOSS'), 'approvers are always allowed'); n++;
  assert.ok(!isAllowed('USTRANGER')); n++;
  assert.ok(!isAllowed(undefined)); n++;

  let s = await run({ command: '/domains', user_id: 'UTEAM1' });
  assert.ok(s.next && !s.replies.length); n++;

  s = await run({ command: '/domains', user_id: 'USTRANGER' });
  assert.ok(!s.next && s.acked, 'stranger stopped, command acked');
  assert.strictEqual(s.replies[0].response_type, 'ephemeral');
  assert.ok(/limited to the domains team/.test(s.replies[0].text)); n++;

  s = await run({ type: 'block_actions', user: { id: 'USTRANGER' } });
  assert.ok(!s.next && s.replies.length === 1, 'buttons blocked too'); n++;

  s = await run({ type: 'view_submission', user: { id: 'USTRANGER' } });
  assert.ok(!s.next && s.acked && !s.replies.length, 'forms close quietly'); n++;

  delete process.env.DOMAINS_ALLOWED_USERS;
  delete process.env.ZAPMAIL_APPROVERS;
  assert.ok(!isAllowed('UTEAM1'), 'nobody configured = nobody in'); n++;

  console.log(n + ' passed');
})().catch(e => { console.error(e); process.exit(1); });
