/**
 * Turn Python/CLI error strings into sentences a teammate can act on.
 * Unknown errors pass through unchanged (minus a leading "ERROR:"), so
 * nothing is hidden - only the known ones get rewritten.
 */
const RULES = [
  [/SCALEDMAIL_ALLOW_SPEND/,
    'Ordering on ScaledMail is switched off on this server (SCALEDMAIL_ALLOW_SPEND), so nothing was charged. Ask an admin if this order should go ahead.'],
  [/SCALEDMAIL_ALLOW_CANCEL/,
    'Cancelling ScaledMail orders is switched off on this server (SCALEDMAIL_ALLOW_CANCEL), so nothing was cancelled. Ask an admin.'],
  [/SCALEDMAIL_API_KEY is not set/,
    'ScaledMail is not connected on this server (SCALEDMAIL_API_KEY missing). Ask an admin.'],
  [/ZAPMAIL_ALLOW_SPEND/,
    'Spending is switched off on this server (ZAPMAIL_ALLOW_SPEND), so nothing was charged. Ask an admin if this purchase should go ahead.'],
  [/has no (configured )?Zapmail account|no Zapmail account configured/i,
    'This client has no Zapmail account set up in the bot. Ask an admin to map it.'],
  [/no export target/i,
    'This client has no Smartlead account connected for exports yet. Connect it in the Zapmail app (Exports), then ask an admin to add it.'],
  [/rate.?limit|429/i,
    'Zapmail is rate-limiting us right now. Try again in about 30 minutes.'],
  [/timed out/i,
    'Zapmail took too long to answer. Nothing was changed on our side; try again in a few minutes.']
];

const PROVIDER_HOSTS = [[/smartlead\.ai/i, 'Smartlead'], [/zapmail\.ai/i, 'Zapmail'], [/scaledmail\.com/i, 'ScaledMail'],
  [/godaddy\.com/i, 'GoDaddy'], [/slack\.com/i, 'Slack']];

/** Keys and tokens out of any text: api_key=…, Bearer …, JWTs, Slack tokens. */
function redact(text) {
  return String(text || '')
    .replace(/((?:api_?key|token|key|secret|password)=)[^&\s'"]+/gi, '$1<hidden>')
    .replace(/Bearer\s+[A-Za-z0-9._~+/=-]+/g, 'Bearer <hidden>')
    .replace(/eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)?/g, '<hidden>')
    .replace(/xox[abposr]-[A-Za-z0-9-]+/g, '<hidden>');
}

/** An HTTP failure (httpx message or an error page) as one sentence, or null. */
function httpSentence(msg) {
  const status = (/\b([45]\d\d)\b(?:\s+[A-Z][A-Za-z ]+)?['"]?\s+for url/.exec(msg) || /\b([45]\d\d)\b/.exec(msg) || [])[1];
  const isHttp = /for url '|HTTPStatusError|<html|<!doctype/i.test(msg);
  if (!status || !isHttp) return null;
  const who = (PROVIDER_HOSTS.find(([re]) => re.test(msg)) || [])[1] || 'The provider';
  if (status === '401' || status === '403') {
    return who + ' rejected an API key (' + status + '), so this did not run.'
      + (who === 'Smartlead' ? ' If it is a past client\'s account, an admin should add it to SMARTLEAD_IGNORE_ACCOUNTS.' : ' Ask an admin to check the key.');
  }
  if (status[0] === '5') return who + ' had a server error (' + status + '). Nothing was changed; try again in a few minutes.';
  return who + ' refused the request (' + status + '). Nothing was changed.';
}

function plainError(raw) {
  const msg = redact(String(raw || '').replace(/^\s*ERROR:\s*/i, '').trim());
  for (const [re, text] of RULES) if (re.test(msg)) return text;
  return httpSentence(msg) || msg || 'Something went wrong (no details).';
}

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

/**
 * A provider date ("2026-11-08", "2026-11-08T08:26:00Z") as people write it:
 * "8 Nov", with the year only when it is not this year ("6 Jul 2027").
 * Anything that is not an ISO date passes through unchanged; empty → ''.
 */
function day(iso, now = new Date()) {
  const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(iso || '').trim());
  if (!m) return String(iso || '').trim();
  const [y, mo, d] = [Number(m[1]), Number(m[2]), Number(m[3])];
  if (mo < 1 || mo > 12) return String(iso).trim();
  return d + ' ' + MONTHS[mo - 1] + (y !== now.getFullYear() ? ' ' + y : '');
}

/**
 * '⚠️ Kill warnings, 13 Oct' -> '⚠️ *Kill warnings, 13 Oct*': the title in bold
 * after its emoji (same rule as notify.title in Python). A line that already
 * has bold, or starts a list, quote, code or link, is left as it is.
 */
function boldTitle(line) {
  const s = String(line || '');
  if (!s.trim() || /^\s*[>•`<]/.test(s)) return s;
  const m = /^((?:(?::[\w+-]+:)|[^\x00-\x7F\p{L}\p{N}\s])+\s+)?(.*\S)\s*$/su.exec(s);
  if (!m || m[2].includes('*')) return s;
  return (m[1] || '') + '*' + m[2] + '*';
}

module.exports = { plainError, redact, day, boldTitle };
