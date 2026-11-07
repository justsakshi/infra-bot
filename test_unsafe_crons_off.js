// The executors judged unsafe or retired must never be on the schedule again (audit 7 Nov 2026).
// Run: node test_unsafe_crons_off.js   (no network)
const assert = require('assert');
const fs = require('fs');
const src = fs.readFileSync(require('path').join(__dirname, 'index.js'), 'utf8');
// Scheduled jobs start through runJob('name', ['script.py', ...]) since bot health (7 Nov 2026); a raw spawn still counts.
const scheduled = [...src.matchAll(/(?:spawn\('python', |runJob\('[a-z0-9-]+', )\[\s*'([a-z_]+\.py)'/g)].map(m => m[1]);
for (const f of ['warmup_executor.py', 'rotation_executor.py', 'eg_test_executor.py', 'nc_test_executor.py']) {
  assert.ok(!scheduled.includes(f), `${f} is scheduled in index.js`);
}
assert.ok(scheduled.includes('blacklist_monitor.py'), 'sanity: the matcher still finds scheduled scripts');
console.log('unsafe crons off: 5 passed');
