/**
 * Plain-English labels for renewal alerts, so a teammate can tell a real
 * expiry from a guess without opening anything.
 *
 * Why this exists (29 Sep 2026): the Daily Renewal Check said "5 domains
 * expire today" while the Zapmail digest said 0. Both were right about their
 * own data: the 5 were Inboxkit domains with NO expiry date saved, so the bot
 * guessed "bought + 1 year"; Zapmail cannot see Inboxkit at all. Nothing in
 * the message said so, and the team could not tell which to believe.
 */
const dayjs = require('dayjs');

/** How sure are we of this asset's expiry? 'saved' | 'estimated' | null. */
function expirySource(asset) {
  if (asset.expiryDate) return 'saved';
  if (asset.purchaseDate) return 'estimated';
  return null;
}

/** One line per asset in a renewal list. */
function assetLine(asset) {
  const icon = asset.type === 'DOMAIN' ? '🌐' : '📧';
  const provider = asset.provider ? ` · ${asset.provider}` : '';
  let line = `  ${icon} ${asset.name}${provider}`;
  // From the renewal review (placement tests + Smartlead): don't pay for a dead inbox again.
  if (asset.renewalFlag === 'DROP') {
    line += ` — ⛔ *don't renew*: ${asset.renewalFlagReason || 'not delivering'}`;
  } else if (asset.renewalFlag === 'CHECK') {
    line += ` — ❔ check before renewing: ${asset.renewalFlagReason || ''}`;
  }
  if (expirySource(asset) === 'estimated') {
    const bought = dayjs(asset.purchaseDate).format('DD MMM YYYY');
    line += asset.type === 'DOMAIN'
      ? ` — _estimated: no expiry saved, bought ${bought} + 1 year. Check ${asset.provider || 'the provider'}, then save the real date._`
      : ` — _estimated: no expiry saved, monthly from ${bought}. Check ${asset.provider || 'the provider'}._`;
  }
  return line;
}

/** Summary suffix: how many of these dates are guesses. '' when none. */
function estimatedNote(assets) {
  const n = assets.filter(a => expirySource(a) === 'estimated').length;
  if (!n) return '';
  return n === assets.length
    ? ' _(all estimated — no expiry date saved; check before renewing)_'
    : ` _(${n} estimated — no expiry date saved)_`;
}

module.exports = { expirySource, assetLine, estimatedNote };
