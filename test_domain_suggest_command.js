// Tests for the Slack domain-suggestion flow (block building + guards).
// Run: node test_domain_suggest_command.js   (no Slack, no network)
const assert = require('assert');
const m = require('./domain_suggest_command');
const { parseDomainsArgs, formatBuyResult } = require('./domains_command');

let passed = 0;
function test(name, fn) {
  try { fn(); passed += 1; } catch (err) { console.error('FAIL', name, '\n ', err.message); process.exitCode = 1; }
}

const PROFILES = {
  Bettrdata: { label: 'BettrData', main_domain: 'bettrdata.io', keywords: ['data', 'ingest', 'pipeline'] },
  Melior: { label: 'Melior', main_domain: '', keywords: [] }
};

test('picker shows ready clients as buttons, lists unready ones', () => {
  const blocks = m.clientPickerBlocks(PROFILES);
  const buttons = blocks.find(b => b.type === 'actions').elements;
  assert.deepStrictEqual(buttons.map(b => b.value), ['Bettrdata']);
  assert.match(buttons[0].text.text, /Suggest for BettrData/);
  assert.match(JSON.stringify(blocks), /Not set up yet: Melior/);
});

test('resolveClientKey matches key or label, case-insensitive', () => {
  assert.strictEqual(m.resolveClientKey(PROFILES, 'bettrdata'), 'Bettrdata');
  assert.strictEqual(m.resolveClientKey(PROFILES, 'BettrData'), 'Bettrdata');
  assert.strictEqual(m.resolveClientKey(PROFILES, 'acme'), null);
});

const RESULT = {
  client: 'Bettrdata', label: 'BettrData', main_domain: 'bettrdata.io', keywords: ['data'],
  ai_usable: 9, generator_usable: 3, price_ceiling: 25, estate_ok: true, errors: [],
  suggestions: Array.from({ length: 12 }, (_, i) => ({
    domain: 'name' + i + '.com', price: 12.99, renew_price: 20.99, source: i < 8 ? 'ai' : 'generator'
  }))
};

test('suggestions become <=10-option checkbox groups + stage button', () => {
  const blocks = m.suggestionBlocks(RESULT);
  const groups = blocks.filter(b => b.block_id && b.block_id.startsWith('domains_pick_'));
  assert.strictEqual(groups.length, 2);
  assert.strictEqual(groups[0].elements[0].options.length, 10);
  assert.strictEqual(groups[1].elements[0].options.length, 2);
  assert.match(groups[0].elements[0].options[0].text.text, /name0\.com.*\$12\.99.*renews \$20\.99.*Zapmail AI/);
  const next = blocks.find(b => b.block_id === 'domains_next').elements.map(e => e.action_id);
  assert.deepStrictEqual(next, ['domains_stage', 'domains_suggest_again']);
});

test('error result renders an error, no buttons to stage', () => {
  const blocks = m.suggestionBlocks({ error: 'Melior needs a main_domain' });
  assert.strictEqual(blocks.length, 1);
  assert.match(blocks[0].text.text, /Melior needs/);
});

test('selectedDomains reads every group and rejects junk values', () => {
  const state = {
    domains_pick_0: { domains_pick: { type: 'checkboxes', selected_options: [{ value: 'a.com' }, { value: '--approve' }] } },
    domains_pick_1: { domains_pick: { type: 'checkboxes', selected_options: [{ value: 'b.com' }, { value: 'a.com' }] } },
    other: { x: {} }
  };
  assert.deepStrictEqual(m.selectedDomains(state), ['a.com', 'b.com']);
});

const STAGED = {
  client: 'Bettrdata', spend_account: 'PRECISE_LEADS', ledger_ok: true, price_ceiling: 25, total_usd: 25.98,
  unavailable: [], unknown: [], over_ceiling: [], already_planned: [],
  batches: [
    { batch_id: 'aaaaaaaaaaaa', earliest_date: '2026-09-28', domains: ['a.com', 'b.com'], estimated_usd: 25.98, status: 'planned' },
    { batch_id: 'bbbbbbbbbbbb', earliest_date: '2026-09-30', domains: ['c.com'], estimated_usd: 12.99, status: 'planned' }
  ]
};

test('no approvers configured → no Buy buttons, explains CLI', () => {
  delete process.env.ZAPMAIL_BUY_APPROVERS;
  const blocks = m.stagedBlocks(STAGED, formatBuyResult, '2026-09-28');
  assert.ok(!JSON.stringify(blocks).includes('domains_buy'));
  assert.match(JSON.stringify(blocks), /One-click buying is off/);
});

test('approvers configured → Buy button only for the batch due today, with confirm', () => {
  process.env.ZAPMAIL_BUY_APPROVERS = 'U123';
  const blocks = m.stagedBlocks(STAGED, formatBuyResult, '2026-09-28');
  const buys = blocks.filter(b => b.accessory && b.accessory.action_id === 'domains_buy');
  assert.deepStrictEqual(buys.map(b => b.accessory.value), ['aaaaaaaaaaaa']);
  assert.ok(buys[0].accessory.confirm);
  assert.match(buys[0].accessory.confirm.text.text, /PRECISE_LEADS/);
  assert.match(JSON.stringify(blocks), /1 later batch/);
  delete process.env.ZAPMAIL_BUY_APPROVERS;
});

test('no billing account → no Buy buttons even with approvers', () => {
  process.env.ZAPMAIL_BUY_APPROVERS = 'U123';
  const blocks = m.stagedBlocks({ ...STAGED, spend_account: null }, formatBuyResult, '2026-09-28');
  assert.ok(!JSON.stringify(blocks).includes('domains_buy'));
  delete process.env.ZAPMAIL_BUY_APPROVERS;
});

test('buyButtonBlocks skips purchased / future batches', () => {
  const blocks = m.buyButtonBlocks([
    { batch_id: 'aaaaaaaaaaaa', status: 'purchased', earliest_date: '2026-09-01', domains: ['x.com'] },
    { batch_id: 'bbbbbbbbbbbb', status: 'planned', earliest_date: '2026-12-01', domains: ['y.com'] },
    { batch_id: 'cccccccccccc', status: 'failed', earliest_date: '2026-09-01', domains: ['z.com'] }
  ], '2026-09-28', null);
  assert.deepStrictEqual(blocks.map(b => b.accessory.value), ['cccccccccccc']);
});

test('/domains with no args opens the picker; suggest X and help parse', () => {
  assert.deepStrictEqual(parseDomainsArgs(''), { mode: 'picker' });
  assert.deepStrictEqual(parseDomainsArgs('suggest belardi wong'), { mode: 'suggest', client: 'belardi wong' });
  assert.deepStrictEqual(parseDomainsArgs('suggest'), { mode: 'picker' });
  assert.deepStrictEqual(parseDomainsArgs('help'), { error: 'help' });
});

console.log(passed + ' passed' + (process.exitCode ? ' (with failures)' : ''));
