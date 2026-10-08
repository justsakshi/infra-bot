/**
 * Who may use the domains app (/domains, /zapmail, every button and form).
 *
 *   DOMAINS_ALLOWED_USERS  comma-separated Slack member ids (U…) who may use it
 *   ZAPMAIL_APPROVERS      approvers are always allowed too (they can also act)
 *
 * Nobody configured → nobody gets in. The app handles purchases, mailboxes
 * and client domain lists, so "open to the whole workspace" is never the
 * default. Everything the app says is ephemeral ("Only visible to you"), so
 * an allowed user in a shared channel does not broadcast results either.
 */

function _ids(value) {
  return String(value || '').split(',').map(s => s.trim()).filter(Boolean);
}

function allowedUsers() {
  const { approvers } = require('./domain_suggest_command');
  return new Set([..._ids(process.env.DOMAINS_ALLOWED_USERS), ...approvers(),
    ..._ids(process.env.SCALEDMAIL_APPROVERS)]);
}

function isAllowed(userId) {
  return Boolean(userId) && allowedUsers().has(userId);
}

const DENIED = ':lock: This bot is limited to the domains team. Ask an admin to add your Slack member id to DOMAINS_ALLOWED_USERS.';

/** Slack user id from a command, action, view or shortcut payload. */
function userOf(body) {
  if (!body) return null;
  // Events (DMs, the agent tab) carry the person in body.event — without this
  // every DM was dropped as "unknown user".
  const ev = body.event || {};
  return body.user_id || (body.user && body.user.id) || ev.user
    || (ev.assistant_thread && ev.assistant_thread.user_id) || null;
}

/**
 * Bolt global middleware: stops anyone not allowed before any handler runs.
 * Commands and buttons get a private "not allowed" note; forms just close.
 */
async function accessMiddleware({ body, ack, respond, next, logger }) {
  const user = userOf(body);
  if (isAllowed(user)) return next();
  const kind = body && body.type;
  if (kind === 'event_callback') return;   // no handler listens to events; drop quietly
  if (kind === 'view_submission' || kind === 'view_closed') {
    if (ack) await ack();
    return;
  }
  if (ack) await ack();
  if (respond) {
    try { await respond({ response_type: 'ephemeral', replace_original: false, text: DENIED }); }
    catch (err) { (logger || console).warn('[domains-access] could not reply:', err.message); }
  }
  console.log('[domains-access] blocked ' + (user || 'unknown user') + ' (' + (kind || body && body.command || 'event') + ')');
}

module.exports = { accessMiddleware, isAllowed, allowedUsers, userOf, DENIED };
