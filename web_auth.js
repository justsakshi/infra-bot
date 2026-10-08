/**
 * Login for the Infra Bot website (dashboard, REST API, CSV upload, triggers).
 *
 * Until 2026-10-08 every route was open: anyone with the Render URL could
 * list, rename or delete tracker rows (which drops them out of renewal
 * reminders), upload CSVs, and start full Smartlead syncs. Now every route
 * asks for a username + password (HTTP Basic auth: the browser shows a login
 * box once, then sends it with every request, including the dashboard's own
 * fetch() calls).
 *
 *   DASHBOARD_USER       default "infra"
 *   DASHBOARD_PASSWORD   required. Unset = the site refuses everything (503):
 *                        failing closed beats an open tracker.
 *
 * Not behind the login: the Zapmail webhook (mounted earlier, has its own
 * HMAC signature check) and /healthz (no data).
 */

const crypto = require('crypto');

function safeEqual(a, b) {
  const x = Buffer.from(String(a));
  const y = Buffer.from(String(b));
  // timingSafeEqual needs equal lengths; compare hashes so length leaks nothing.
  const hx = crypto.createHash('sha256').update(x).digest();
  const hy = crypto.createHash('sha256').update(y).digest();
  return crypto.timingSafeEqual(hx, hy) && x.length === y.length;
}

function parseBasic(header) {
  const m = /^Basic\s+([A-Za-z0-9+/=]+)$/.exec(String(header || ''));
  if (!m) return null;
  const decoded = Buffer.from(m[1], 'base64').toString('utf8');
  const i = decoded.indexOf(':');
  if (i < 0) return null;
  return { user: decoded.slice(0, i), pass: decoded.slice(i + 1) };
}

/** Express middleware. `env` is injectable for tests. */
function requireLogin(env = process.env) {
  return (req, res, next) => {
    if (req.path === '/healthz') return next();
    const password = env.DASHBOARD_PASSWORD;
    if (!password) {
      return res.status(503).send('Infra Bot website is locked: set DASHBOARD_PASSWORD on the server.');
    }
    const creds = parseBasic(req.headers.authorization);
    const user = env.DASHBOARD_USER || 'infra';
    if (creds && safeEqual(creds.user, user) && safeEqual(creds.pass, password)) return next();
    res.set('WWW-Authenticate', 'Basic realm="Infra Bot", charset="UTF-8"');
    return res.status(401).send('Login required.');
  };
}

/** One run at a time for a manual trigger (e.g. a full Smartlead sync). */
function singleFlight() {
  const running = new Set();
  return {
    tryStart(name) { if (running.has(name)) return false; running.add(name); return true; },
    done(name) { running.delete(name); }
  };
}

module.exports = { requireLogin, parseBasic, safeEqual, singleFlight };
