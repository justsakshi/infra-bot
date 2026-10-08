// Run every Slack-side test (test_*.js). Exits non-zero if any fails.
// `npm test` — also run by GitHub Actions before Render deploys.
const { spawnSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const root = path.join(__dirname, '..');
const files = fs.readdirSync(root).filter(f => /^test_.*\.js$/.test(f)).sort();
let failed = 0;
for (const f of files) {
  const r = spawnSync(process.execPath, [f], { cwd: root, encoding: 'utf8', env: { ...process.env, NODE_ENV: 'test' } });
  const out = (r.stdout || '') + (r.stderr || '');
  const last = out.trim().split('\n').filter(l => /passed|Error|assert/i.test(l)).pop() || '';
  if (r.status === 0) {
    console.log('ok   ' + f + '  ' + last);
  } else {
    failed++;
    console.log('FAIL ' + f + '\n' + out.split('\n').slice(-15).join('\n'));
  }
}
console.log('\n' + (files.length - failed) + '/' + files.length + ' test files passed');
process.exit(failed ? 1 : 0);
