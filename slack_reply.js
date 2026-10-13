/**
 * One message per conversation in the domains app.
 *
 *   replace(respond, payload, back?)  the clicked message becomes the next
 *                                      screen, with a nav row (⌂ Home, ← Back, ✕ Close)
 *   add(respond, payload)              a new message, for when replacing would
 *                                      wipe something still needed (ticked
 *                                      boxes, a list of other Buy buttons)
 *   swap(respond, payload)             a typed command's "Checking…" message
 *                                      becomes its result (one message, not two)
 *
 * Slack lets the bot change a message only through the click that made it
 * (response_url: 30 minutes, 5 uses), so replies go through `respond`.
 *
 * Every "Only visible to you" message gets a ✕ Close button (withClose):
 * Slack keeps no list of them, so the bot cannot clear them later, but a
 * click on one may delete it. withCloseButtons(app) adds it to every
 * listener's `respond`; posts that skip `respond` call withClose themselves.
 */

const MAX_BLOCKS = 50;   // Slack's cap per message
const SECTION_MAX = 2900;

function button(text, action_id, value) {
  return { type: 'button', action_id, text: { type: 'plain_text', text }, value: String(value || 'x') };
}
const closeButton = () => button('✕ Close', 'nav_close', 'close');

/** ⌂ Home, plus ← Back to a section menu ({label, action_id, value?}), then ✕ Close. */
function navRow(back) {
  const elements = [button('⌂ Home', 'nav_home', 'home')];
  if (back) elements.push(button('← ' + back.label, back.action_id, back.value));
  elements.push(closeButton());
  return { type: 'actions', block_id: 'nav_row', elements };
}

/** Text as section blocks of at most SECTION_MAX characters, cut at line breaks. */
function textSections(text) {
  const out = [];
  let cur = '';
  for (const line of String(text || '').split('\n')) {
    for (let i = 0; i < Math.max(line.length, 1); i += SECTION_MAX) {
      const piece = line.slice(i, i + SECTION_MAX);
      if (cur && cur.length + 1 + piece.length > SECTION_MAX) { out.push(cur); cur = ''; }
      cur = cur ? cur + '\n' + piece : piece;
    }
  }
  if (cur.trim()) out.push(cur);
  return out.map(t => ({ type: 'section', text: { type: 'mrkdwn', text: t } }));
}

const hasClose = blocks => blocks.some(b => (b.elements || []).some(e => e.action_id === 'nav_close'));

/** The reply with a ✕ Close button, once. Messages everyone sees and
 *  delete-only replies pass through untouched. */
function withClose(payload) {
  const p = typeof payload === 'string' ? { text: payload } : { ...payload };
  if (p.delete_original || p.response_type === 'in_channel') return p;
  const blocks = p.blocks && p.blocks.length ? p.blocks : textSections(p.text);
  if (!blocks.length || hasClose(blocks)) return p;
  return { ...p, blocks: [...blocks.slice(0, MAX_BLOCKS - 1), { type: 'actions', block_id: 'close_row', elements: [closeButton()] }] };
}

function screen(payload, back) {
  const body = payload.blocks && payload.blocks.length ? payload.blocks : textSections(payload.text);
  return { ...payload, blocks: [...body.slice(0, MAX_BLOCKS - 1), navRow(back)] };
}

function replace(respond, payload, back) {
  return respond({ response_type: 'ephemeral', replace_original: true, ...screen(payload, back) });
}

function add(respond, payload) {
  return respond({ response_type: 'ephemeral', replace_original: false, ...withClose(payload) });
}

/** The typed command's last message becomes this one; if Slack refuses, a new message. */
async function swap(respond, payload) {
  try {
    return await respond({ response_type: 'ephemeral', replace_original: true, ...withClose(payload) });
  } catch (err) {
    console.error('[slack-reply] replace refused, posting instead:', err.message);
    return add(respond, payload);
  }
}

/** The app as handlers see it, with every listener's `respond` adding ✕ Close.
 *  Register the domains app's handlers on this. */
function withCloseButtons(app) {
  const wrap = fn => (typeof fn !== 'function' ? fn
    : args => fn(args && args.respond ? { ...args, respond: p => args.respond(withClose(p)) } : args));
  const on = method => (...a) => app[method](...a.map(wrap));
  return { action: on('action'), command: on('command'), view: on('view'), shortcut: on('shortcut'),
    event: on('event'), message: on('message'), use: on('use'), client: app.client };
}

/** ⌂ Home: the one domains menu, in place of the clicked message. ✕ Close: delete it. */
function registerNav(app, baseDir) {
  app.action('nav_home', async ({ ack, respond }) => {
    await ack();
    await respond(withClose({ response_type: 'ephemeral', replace_original: true, text: 'Domains & inboxes',
      blocks: require('./domains_unified').homeBlocks(baseDir) }));
  });
  app.action('nav_close', async ({ ack, respond }) => {
    await ack();
    try { await respond({ delete_original: true }); } catch (err) { console.error('[slack-reply] close failed:', err.message); }
  });
}

module.exports = { replace, add, swap, withClose, withCloseButtons, textSections, navRow, screen, registerNav, MAX_BLOCKS };
