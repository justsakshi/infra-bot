// /domains audit + kill screens: headline, then one block per thread section. Run: node test_report_blocks.js
const assert = require('assert');
const { reportBlocks } = require('./domains_home');

let passed = 0;
function test(name, fn) { try { fn(); passed += 1; } catch (err) { console.error('FAIL', name, '\n ', err.message); process.exitCode = 1; } }

test('headline first, then each thread part, no marker text', () => {
  const b = reportBlocks({ headline: ['🚨 Kill warnings, 13 Oct: 19 inboxes', 'Bettrdata 13'], thread: ['*Bettrdata*\n• a', 'rule'] });
  assert.strictEqual(b[0].text.text, '🚨 Kill warnings, 13 Oct: 19 inboxes\nBettrdata 13');
  assert.deepStrictEqual(b.slice(1).filter(x => x.type === 'section').map(x => x.text.text), ['*Bettrdata*\n• a', 'rule']);
  assert.ok(!JSON.stringify(b).includes('— thread —'));
});

test('a long part is cut at line breaks, never mid-line', () => {
  const long = Array.from({ length: 300 }, (_, i) => '• inbox' + i + '@example.com — two strikes, 50% then 40%').join('\n');
  const b = reportBlocks({ headline: ['h'], thread: [long] });
  for (const x of b.filter(y => y.type === 'section')) {
    assert.ok(x.text.text.length <= 2900);
    assert.ok(!x.text.text.startsWith('two strikes'), 'no cut mid-line');
  }
});

test('stays under the screen cap and says where the rest is', () => {
  const parts = Array.from({ length: 40 }, (_, i) => 'part ' + i);
  const b = reportBlocks({ headline: ['h'], thread: parts }, 21);
  assert.ok(b.length <= 21);
  assert.match(b[b.length - 1].elements[0].text, /more section.*channel thread/);
});

test('old shape (text only) still renders', () => {
  const b = reportBlocks({ text: 'just text' });
  assert.strictEqual(b[0].text.text, 'just text');
});

test('an error reads as one line', () => {
  const b = reportBlocks({ error: 'boom' });
  assert.match(b[0].text.text, /boom/);
});

console.log(passed + ' passed');
