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

function plainError(raw) {
  const msg = String(raw || '').replace(/^\s*ERROR:\s*/i, '').trim();
  for (const [re, text] of RULES) if (re.test(msg)) return text;
  return msg || 'Something went wrong (no details).';
}

module.exports = { plainError };
