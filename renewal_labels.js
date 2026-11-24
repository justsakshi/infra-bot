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

/** A DROP flag saved without a reason (older rows): say only that, never "not delivering". */
const NO_REASON = 'the renewal review said drop it (no reason saved)';

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
  let line = `${icon} ${asset.name}${provider}`;
  // From the renewal review (placement tests + Smartlead): don't pay for a dead inbox again.
  if (asset.renewalFlag === 'DROP') {
    line += ` · ⛔ *don't renew*: ${asset.renewalFlagReason || NO_REASON}`;
  } else if (asset.renewalFlag === 'CHECK') {
    line += ' · ❔ check before renewing' + (asset.renewalFlagReason ? `: ${asset.renewalFlagReason}` : '');
  }
  if (expirySource(asset) === 'estimated') {
    const bought = dayjs(asset.purchaseDate).format('D MMM YYYY');
    line += asset.type === 'DOMAIN'
      ? ` · _estimated: no expiry saved, bought ${bought} + 1 year. Check ${asset.provider || 'the provider'}, then save the real date._`
      : ` · _estimated: no expiry saved, monthly from ${bought}. Check ${asset.provider || 'the provider'}._`;
  }
  return line;
}

/** Summary suffix: how many of these dates are guesses. '' when none. */
function estimatedNote(assets) {
  const n = assets.filter(a => expirySource(a) === 'estimated').length;
  if (!n) return '';
  return n === assets.length
    ? ' _(all estimated: no expiry date saved, check before renewing)_'
    : ` _(${n} estimated: no expiry date saved)_`;
}

const plural = (n, one, many) => n + ' ' + (n === 1 ? one : many);

/** "2 domains · 1 inbox" for a list of tracker rows. */
function countLine(assets) {
  const domains = assets.filter(a => a.type === 'DOMAIN').length;
  const inboxes = assets.filter(a => a.type === 'INBOX').length;
  return [domains ? plural(domains, 'domain', 'domains') : '', inboxes ? plural(inboxes, 'inbox', 'inboxes') : '']
    .filter(Boolean).join(' · ') || 'nothing';
}

/** The *Read more* thread reply: a count line, then one group per client. */
function expiringDetail(label, assets) {
  const byClient = {};
  for (const a of assets) (byClient[a.client || 'No client'] = byClient[a.client || 'No client'] || []).push(a);
  const groups = Object.entries(byClient).map(([c, items]) => `*${c}*\n` + items.map(assetLine).join('\n'));
  const when = String(label || '');
  return [`*Expiring ${when.charAt(0).toLowerCase() + when.slice(1)}*\n${countLine(assets)}`, ...groups].join('\n\n');
}

/** What one renewal costs, from the tracker: '$7/mo', '$12/yr', '' when not saved. */
function costText(a) {
  const cur = !a.currency || /^usd$/i.test(a.currency) ? '$' : `${a.currency} `;
  if (a.monthlyCost !== null && a.monthlyCost !== undefined && a.monthlyCost !== '') return `${cur}${a.monthlyCost}/mo`;
  if (a.yearlyCost !== null && a.yearlyCost !== undefined && a.yearlyCost !== '') return `${cur}${a.yearlyCost}/yr`;
  return '';
}

/** One asset on the card itself (17 Nov 2026, Manveen: a post gives the complete context, not a count behind a
 *  button): name, client, vendor, cost, and the renewal review's flag. */
function cardLine(a) {
  const bits = [a.client || 'no client', a.provider || 'vendor not saved', costText(a)].filter(Boolean);
  let line = `• ${a.name} (${bits.join(', ')})`;
  if (a.renewalFlag === 'DROP') line += ` ⛔ don't renew: ${a.renewalFlagReason || NO_REASON}`;
  else if (a.renewalFlag === 'CHECK') line += ' ❔ check first' + (a.renewalFlagReason ? `: ${a.renewalFlagReason}` : '');
  if (expirySource(a) === 'estimated') line += ' (date estimated, check it)';
  return line;
}

const CARD_LINES = 8;      // assets named per group on the card; *Read more* lists them all

/** The decision the card asks for, said once under the groups. 24 Nov 2026 (t396): it said ⛔ meant "found it not
 *  delivering" while the flag held only a DO NOT USE tag; the owner had to ask whether last week's test was checked.
 *  The legend now claims only what a drop is made of; each flag says what the review saw. */
const CARD_STEP = 'Renew each one in its vendor before its date, or let it go on purpose. ⛔ means the renewal '
  + 'review says don\'t renew it: a person tagged it DO NOT USE in Smartlead, or it failed two weekly tests in a row. '
  + 'After each one: the newest test, warm-up and All Inboxes answer it saw, or that there was none.';

/**
 * The Daily Renewal Check (index.js runNoonSummary, 10:00 IST weekdays).
 * groups: [{ assets, label, icon, key, daysFilter }]. The button value keeps
 * the label exactly as before: *Read more* re-reads it from there.
 */
function renewalCheckBlocks(groups, { channel, baseUrl, now = dayjs() }) {
  const title = `Daily Renewal Check, ${now.format('D MMM')}`;
  const blocks = [{ type: 'header', text: { type: 'plain_text', text: `⏰ ${title}` } }];
  const shown = groups.filter(g => g.assets.length);
  if (!shown.length) {
    blocks.push({ type: 'section', text: { type: 'mrkdwn',
      text: '✅ Nothing expires today, tomorrow or in the next 3 working days.' } });
  }
  for (const { label, icon, key, daysFilter, assets } of shown) {
    const domains = assets.filter(a => a.type === 'DOMAIN').length;
    const inboxes = assets.filter(a => a.type === 'INBOX').length;
    const parts = [];
    if (domains) parts.push(`*${plural(domains, 'domain', 'domains')}*`);
    if (inboxes) parts.push(`*${plural(inboxes, 'inbox', 'inboxes')}*`);
    const named = assets.slice(0, CARD_LINES).map(cardLine);
    if (assets.length > CARD_LINES) named.push(`• and ${assets.length - CARD_LINES} more (Read more)`);
    blocks.push({
      type: 'section',
      text: { type: 'mrkdwn', text: `${icon} ${parts.join(' and ')} expire${assets.length === 1 ? 's' : ''} *${label.charAt(0).toLowerCase() + label.slice(1)}*`
        + estimatedNote(assets) + '\n' + named.join('\n') },
      accessory: {
        type: 'button',
        text: { type: 'plain_text', text: 'Read more' },
        action_id: `view_details_${key}`,
        // Only a compact lookup key: the detail is re-read from the DB on click
        // (Slack caps a button value at 2,000 characters).
        value: JSON.stringify({ key, channel, daysFilter, label })
      }
    });
  }
  if (shown.length) blocks.push({ type: 'section', text: { type: 'mrkdwn', text: CARD_STEP } });
  blocks.push({ type: 'context', elements: [{ type: 'mrkdwn', text: `🔗 <${baseUrl}/|Open dashboard> to renew or manage assets` }] });
  return { text: title, blocks };
}

/** The 4 PM weekday nudge; ``count`` = what the 10:00 check found expiring (null = not known); ``names`` = which
 *  (17 Nov 2026: a reminder that names nothing makes people open the dashboard to find out). */
function reminderText(baseUrl, count = null, names = []) {
  const what = count ? `${plural(count, 'domain or inbox', 'domains or inboxes')} expire${count === 1 ? 's' : ''} in the next 3 working days`
    : 'some domains or inboxes may expire soon';
  const list = (names || []).slice(0, 10);
  const listed = list.length ? `: ${list.join(', ')}${names.length > list.length ? ` and ${names.length - list.length} more` : ''}` : '';
  return `🔔 Reminder: ${what}${listed}.\n\nRenew ${count === 1 ? 'it' : 'them'} or let ${count === 1 ? 'it' : 'them'} go on purpose (this morning's Daily Renewal Check has each one's vendor and cost). <${baseUrl}/|Open dashboard>`;
}

module.exports = { expirySource, assetLine, estimatedNote, countLine, expiringDetail, renewalCheckBlocks, reminderText,
  cardLine, costText };
