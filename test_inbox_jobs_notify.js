// Job update messages. Run: node test_inbox_jobs_notify.js
const assert = require('assert');
const { jobUpdateText } = require('./inbox_jobs_notify');

let n = 0;
const base = { job_id: 'abc123', client: 'Melior', domains: ['gomelior.com'], progress: 'Inbox slots ✓ · Create inboxes …' };
assert.ok(/white_check_mark.*Melior.*gomelior\.com.*is done/.test(jobUpdateText({ ...base, status: 'done' }))); n++;
const w = jobUpdateText({ ...base, status: 'waiting', detail: 'bought 2 slot(s); pay: https://invoice' });
assert.ok(/hourglass/.test(w) && /pay: https:\/\/invoice/.test(w) && /`abc123`/.test(w)); n++;
const chk = jobUpdateText({ ...base, status: 'needs_check', detail: 'purchase b1 is unknown' });
assert.ok(/needs a person/.test(chk) && /Nothing will be retried/.test(chk)); n++;
assert.ok(/stopped/.test(jobUpdateText({ ...base, status: 'failed', detail: 'x.com is taken' }))); n++;
console.log(n + ' passed');
