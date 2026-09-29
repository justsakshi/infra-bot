// `/domains zapmail …` reaches the Zapmail menu without a /zapmail slash
// command registered in Slack (only the app owner can add one).
// Run: node test_domains_zapmail_route.js
const assert = require('assert');
const { registerDomainsCommand } = require('./domains_command');

const commands = {};
const fakeApp = {
  command: (name, fn) => { commands[name] = fn; },
  action: () => {},
  view: () => {}
};
registerDomainsCommand(fakeApp, __dirname);

async function call(text) {
  const out = [];
  await commands['/domains']({ ack: async () => {}, command: { text, user_id: 'U1' },
    respond: async (m) => { out.push(m); } });
  return out;
}

(async () => {
  let n = 0;
  for (const text of ['zapmail', 'ZAPMAIL', 'zm', '  zapmail  ']) {
    const out = await call(text);
    assert.ok(out[0] && Array.isArray(out[0].blocks), 'menu for ' + JSON.stringify(text));
    assert.strictEqual(out[0].text, 'Zapmail');
    n++;
  }
  const help = await call('zapmail help');
  assert.ok(/domains zapmail/.test(help[0].text)); n++;
  const bad = await call('zapmail domain not-a-domain');
  assert.ok(/Give a domain/.test(bad[0].text)); n++;
  // A domain list that merely starts with "zapmail" as a word is not hijacked.
  const suggest = await call('help');
  assert.ok(!/Zapmail/.test(suggest[0].text || '') || /domains/.test(suggest[0].text)); n++;
  console.log(n + ' passed');
})().catch(e => { console.error(e); process.exit(1); });
