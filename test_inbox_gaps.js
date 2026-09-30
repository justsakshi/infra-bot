// Retry / retire buttons, and jobs moving on Zapmail events. Run: node test_inbox_gaps.js
const assert = require('assert');
const { domainBlocks, retireModal } = require('./zapmail_actions');
const jobs = require('./inbox_jobs_notify');

(async () => {
  let n = 0;
  const hit = { domain: 'gomelior.com', client: 'Melior', account: 'PRECISE_LEADS', status: 'ACTIVE', domain_id: 'd1',
    mailboxes: [{ email: 'ryan@gomelior.com', status: 'ACTIVE' }, { email: 'ryanm@gomelior.com', status: 'FAILED' }] };
  const ids = domainBlocks({ hits: [hit] }).filter(b => b.type === 'actions')[0].elements.map(e => e.action_id);
  assert.ok(ids.includes('zm_retire_open') && ids.includes('zm_retry_failed')); n++;
  const noFail = domainBlocks({ hits: [{ ...hit, mailboxes: [hit.mailboxes[0]] }] }).filter(b => b.type === 'actions')[0].elements.map(e => e.action_id);
  assert.ok(!noFail.includes('zm_retry_failed'), 'retry only when something failed'); n++;

  const m = retireModal({ domain: 'gomelior.com', client: 'Melior', inboxes: ['ryan@gomelior.com', '<bad>'] }, 'C1');
  assert.strictEqual(m.blocks[1].element.options.length, 1); n++;
  assert.ok(/next renewal/.test(m.blocks[0].text.text) && /undo/i.test(JSON.stringify(m.blocks[2]))); n++;

  assert.ok(jobs.eventMovesJobs('mailbox.updated') && jobs.eventMovesJobs('domain.connection_status_changed')
    && jobs.eventMovesJobs('export.completed') && !jobs.eventMovesJobs('subscription.billing_changed')); n++;

  // A burst of events -> one run; an event during a run -> exactly one more.
  let runs = 0;
  const slow = async () => { runs++; await new Promise(r => setTimeout(r, 60)); };
  for (let i = 0; i < 5; i++) jobs.nudge('.', 10, slow);
  await new Promise(r => setTimeout(r, 30));
  assert.strictEqual(runs, 1, 'burst grouped into one run'); n++;
  jobs.nudge('.', 10, slow); jobs.nudge('.', 10, slow);         // during the run
  await new Promise(r => setTimeout(r, 200));
  assert.strictEqual(runs, 2, 'exactly one follow-up run'); n++;
  console.log(n + ' passed');
})().catch(e => { console.error(e); process.exit(1); });
