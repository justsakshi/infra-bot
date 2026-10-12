/**
 * One message per conversation in the domains app.
 *
 *   replace(respond, payload, back?)  the clicked message becomes the next
 *                                      screen, with a nav row (⌂ Home, ← Back)
 *   add(respond, payload)              a new message, for when replacing would
 *                                      wipe something still needed (ticked
 *                                      boxes, a list of other Buy buttons)
 *
 * Slack lets the bot change a message only through the click that made it
 * (response_url: 30 minutes, 5 uses), so replies go through `respond`.
 */

const MAX_BLOCKS = 50;   // Slack's cap per message

function button(text, action_id, value) {
  return { type: 'button', action_id, text: { type: 'plain_text', text }, value: String(value || 'x') };
}

/** ⌂ Home, plus ← Back to a section menu ({label, action_id, value?}). */
function navRow(back) {
  const elements = [button('⌂ Home', 'nav_home', 'home')];
  if (back) elements.push(button('← ' + back.label, back.action_id, back.value));
  return { type: 'actions', block_id: 'nav_row', elements };
}

function screen(payload, back) {
  const body = payload.blocks && payload.blocks.length
    ? payload.blocks
    : [{ type: 'section', text: { type: 'mrkdwn', text: String(payload.text || '').slice(0, 2900) } }];
  return { ...payload, blocks: [...body.slice(0, MAX_BLOCKS - 1), navRow(back)] };
}

function replace(respond, payload, back) {
  return respond({ response_type: 'ephemeral', replace_original: true, ...screen(payload, back) });
}

function add(respond, payload) {
  return respond({ response_type: 'ephemeral', replace_original: false, ...payload });
}

/** ⌂ Home: the one domains menu, in place of the clicked message. */
function registerNav(app, baseDir) {
  app.action('nav_home', async ({ ack, respond }) => {
    await ack();
    await respond({ response_type: 'ephemeral', replace_original: true, text: 'Domains & inboxes',
      blocks: require('./domains_unified').homeBlocks(baseDir) });
  });
}

module.exports = { replace, add, navRow, screen, registerNav, MAX_BLOCKS };
