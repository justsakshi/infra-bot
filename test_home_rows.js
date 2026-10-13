// Home: no button row longer than 6. Slack folded the 7-button Check row into "+ 2 more"
// (screenshot 2026-10-13); the 6-button Tracker row showed in full. Run: node test_home_rows.js
const assert = require('assert');
const { homeBlocks } = require('./domains_unified');
const rows = homeBlocks(__dirname).filter(b => b.type === 'actions');
for (const r of rows) assert.ok(r.elements.length <= 6, r.block_id + ' has ' + r.elements.length + ' buttons');
const ids = rows.flatMap(r => r.elements.map(e => e.action_id));
for (const id of ['u_nav_kill', 'dh_audit', 'u_nav_digest', 'u_nav_status']) assert.ok(ids.includes(id), id + ' on the home screen');
console.log('2 passed');
