/**
 * `/zapmail` — read-only Zapmail fleet view in Slack.
 *
 * Every subcommand here is a pure READ against every configured Zapmail
 * account (or the purchase ledger). Nothing spends money or mutates anything,
 * so there is no approval gate to get wrong. Buying, connecting, and mailbox
 * creation stay on the CLI (see docs/ZAPMAIL_INTEGRATION_HANDOFF.md).
 *
 * Slack usage:
 *   /zapmail status          - wallet, plan, mailboxes, placement credits per account
 *   /zapmail renewals        - domains expiring <=2mo, per account
 *   /zapmail cross-check     - Zapmail renewals vs /infra asset tracker drift
 *   /zapmail domain x.com    - which account has x.com, its state and mailboxes
 *   /zapmail batches         - the domain purchase plan (ledger)
 *   /zapmail digest          - today's action list (same as the daily post)
 *
 * Registered on the same Slack app as `/domains` (which has its own token and
 * is editable, unlike the deactivated Infra Bot app).
 */

const path = require('path');
const { spawn } = require('child_process');

const NL = String.fromCharCode(10);
const SAFE_DOMAIN_RE = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$/;

const ZAPMAIL_HELP = [
  '*Usage:* `/domains zapmail` (menu) or `/domains zapmail <status|renewals|batches|digest|domain x.com|prewarmed|sync|cross-check>` — same as `/zapmail …` where that command is set up.',
  '',
  'Views are read-only. Changes (mailboxes, export, auto-renew, renew, tracker sync, buy) are buttons for approvers (`ZAPMAIL_APPROVERS`), each asking first.',
  '',
  '`status`        — wallet, auto-recharge, domains + mailboxes (Google/Outlook), placement credits.',
  '`renewals`      — current-client domains expiring in 2 months, with renew / auto-renew.',
  '`batches`       — the staggered domain purchase plan; Buy now on due batches.',
  '`digest`        — today’s Zapmail action list.',
  '`domain x.com`  — account, provider, health, expiry, mailboxes — plus Set up mailboxes / Export / Auto-renew.',
  '`prewarmed`     — our pre-warmed slots (used/free) and Zapmail’s stock.',
  '`sync`          — preview Zapmail → /infra tracker changes; Apply button.',
  '`cross-check`   — Zapmail vs /infra tracker: missing or mismatched.'
].join('\n');

/** Map a subcommand to the Python script + args it runs. */
function commandFor(sub, arg) {
  switch (sub) {
    case 'status': return ['zapmail_status.py', '--status', '--json'];
    case 'renewals': return ['zapmail_status.py', '--renewals', '--json'];
    case 'cross-check': return ['zapmail_status.py', '--cross-check', '--json'];
    case 'domain': return ['zapmail_status.py', '--domain', arg, '--json'];
    case 'batches': return ['zapmail_buy.py', '--list', '--json'];
    case 'digest': return ['zapmail_digest.py', '--json', '--no-post'];
    case 'prewarmed': return ['zapmail_status.py', '--prewarmed', '--json'];
    case 'sync': return ['zapmail_asset_sync.py', '--json'];
    default: return null;
  }
}

function runZapmailScript(args, baseDir) {
  return new Promise((resolve, reject) => {
    const syncDir = path.join(baseDir, 'smartlead_sync');
    const python = process.env.INFRABOT_PYTHON || 'python';
    const proc = spawn(python, args, {
      cwd: syncDir,
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
    });

    let stdout = '';
    let stderr = '';
    proc.stdout.on('data', d => { stdout += d; });
    proc.stderr.on('data', d => { stderr += d; process.stderr.write('[zapmail] ' + d); });

    const timer = setTimeout(() => {
      proc.kill();
      reject(new Error('timed out after 2 minutes'));
    }, 2 * 60 * 1000);

    proc.on('close', code => {
      clearTimeout(timer);
      if (code !== 0) {
        const tail = stderr.trim().split('\n').slice(-3).join('\n');
        return reject(new Error(tail || 'exited with code ' + code));
      }
      const line = stdout.trim().split('\n').filter(Boolean).pop();
      if (!line) return reject(new Error('no output'));
      try {
        resolve(JSON.parse(line));
      } catch (err) {
        reject(new Error('could not parse result'));
      }
    });
    proc.on('error', err => { clearTimeout(timer); reject(err); });
  });
}

function money(v) {
  const n = Number(v);
  return Number.isFinite(n) ? '$' + n.toFixed(2) : '$?';
}

function formatStatus(s) {
  const lines = Object.keys(s).map(name => {
    const a = s[name];
    if (!a.ok) return '• *' + name + '* — :warning: unreachable' + (a.error ? ' (`' + a.error + '`)' : '');
    const wallet = a.wallet_balance ?? a.wallet_api_balance;
    const recharge = a.auto_recharge ? ' (auto-recharge on)' : ' (auto-recharge off)';
    const wal = wallet !== null && wallet !== undefined ? ' ' + money(wallet) + recharge : '';
    const dom = a.domains || {};
    const act = a.active_mailboxes || {};
    const boxes = a.active_mailboxes
      ? ' · Google ' + (dom.GOOGLE ?? '?') + ' domains / ' + (act.GOOGLE ?? '?') + ' mailboxes'
        + ' · Outlook ' + (dom.MICROSOFT ?? '?') + ' domains / ' + (act.MICROSOFT ?? '?') + ' mailboxes'
      : (a.assigned_mailboxes !== undefined ? ' · ' + a.assigned_mailboxes + ' mailboxes' : '');
    const plan = a.active_plan ? ' · ' + a.active_plan : '';
    const credits = (a.placement_credits !== null && a.placement_credits !== undefined)
      ? ' · ' + a.placement_credits + ' placement credits' : '';
    return '• *' + name + '* — wallet' + wal + plan + boxes + credits;
  });
  return '*Zapmail fleet status* (read-only)' + NL + lines.join('\n');
}

function formatRenewals(rows, errors) {
  const warn = (errors || []).map(e => ':warning: could not check ' + e).join(NL);
  if (!rows.length) {
    return (warn ? warn + NL : '') + '*Zapmail renewals* — '
      + (warn ? 'nothing found on the accounts that answered.' : 'no domains expiring within 2 months.');
  }
  const byAccount = {};
  rows.forEach(r => {
    const k = r.account || 'unknown';
    if (!byAccount[k]) byAccount[k] = [];
    byAccount[k].push(r);
  });
  let msg = '*Zapmail renewals* (expiring in 2 months)' + NL;
  for (const [account, list] of Object.entries(byAccount)) {
    msg += NL + '*' + account + '* (' + list.length + ')' + NL;
    list.sort((a, b) => (a.expire_on || '').localeCompare(b.expire_on || ''));
    list.forEach(r => {
      const boxes = r.assigned_mailboxes ? ' · ' + r.assigned_mailboxes + ' inboxes' : '';
      const prov = r.provider === 'MICROSOFT' ? ' · Outlook' : '';
      msg += '• `' + r.domain + '` — ' + (r.expire_on || '?') + boxes + prov + NL;
    });
  }
  return (warn ? warn + NL : '') + msg;
}

function formatCrossCheck(cc) {
  const cap = (list, n) => list.slice(0, n).concat(list.length > n ? ['…and ' + (list.length - n) + ' more'] : []);
  let msg = '*Cross-check* — ' + (cc.zapmail_domains ?? '?') + ' Zapmail domains vs '
    + (cc.tracker_domains ?? '?') + ' in the /infra tracker' + NL;
  (cc.zapmail_errors || []).forEach(e => {
    msg += ':warning: could not read ' + e + ' — "only in tracker" is unreliable' + NL;
  });
  // Only what the team acts on (29 Sep review): current-client domains
  // missing from the tracker, blank or wrong dates. Past clients' domains,
  // lapsed ones, and tracker rows on other providers (Inboxkit, ScaledMail —
  // tracked by hand) are left out on purpose.
  const missing = (cc.only_in_zapmail || []).filter(x => !('client' in x) || x.client);
  msg += NL + ':warning: *Current-client domains not in the tracker* (' + missing.length + '):' + NL;
  if (missing.length) {
    cap(missing.map(x => '• `' + x.domain + '` — ' + (x.client || '?') + ' · expires ' + (x.expire_on || '?')), 25)
      .forEach(l => { msg += l + NL; });
    msg += '_Added by *Apply to tracker* (Tracker sync) or the 9:30 sync._' + NL;
  } else {
    msg += '_none_' + NL;
  }
  const noDate = cc.no_tracker_expiry || [];
  if (noDate.length) {
    msg += NL + ':memo: *Tracked without an expiry date* (' + noDate.length + ') — Zapmail has it:' + NL
      + cap(noDate.map(x => '• `' + x.domain + '` — Zapmail says ' + (x.expire_on || '?')), 10).join(NL) + NL
      + '_Filled in by *Apply to tracker* or the 9:30 sync._' + NL;
  }
  msg += NL + ':warning: *Date mismatch* (' + cc.date_mismatch.length + '):' + NL;
  if (cc.date_mismatch.length) {
    cc.date_mismatch.forEach(x => { msg += '• `' + x.domain + '` — zapmail ' + (x.zapmail_expire || '?') + ' vs tracker ' + (x.tracker_expire || '?') + NL; });
  } else {
    msg += '_none_';
  }
  return msg;
}

function formatDomain(r) {
  const hits = r.hits || [];
  if (!hits.length) return '`' + r.domain + '` is not in any connected Zapmail account.';
  return hits.map(h => {
    if (h.error) return '• *' + h.account + '* — :warning: ' + h.error;
    let m = '*`' + h.domain + '`* — *' + h.account + '* ('
      + (h.provider === 'MICROSOFT' ? 'Outlook' : 'Google') + ') · ' + (h.status || '?')
      + ' · expires ' + (h.expire_on || '?')
      + ' · auto-renew ' + (h.auto_renew ? 'on' : 'off')
      + (h.dns_shield ? ' · DNS Shield' : '');
    const boxes = h.mailboxes || [];
    m += NL + (boxes.length
      ? boxes.map(b => '   • ' + b.email + ' — ' + (b.status || '?') + (b.warmed_up ? ' · warmed' : '')).join(NL)
      : '   _no mailboxes_');
    return m;
  }).join(NL + NL);
}

function formatBatches(r) {
  if (!r.ledger_ok) return ':warning: Purchase ledger unreachable (Mongo).';
  const rows = (r.batches || []).slice().sort((a, b) => (a.earliest_date || '').localeCompare(b.earliest_date || ''));
  if (!rows.length) return '*Domain purchase plan* — empty. Stage one with `/domains buy …`.';
  const today = new Date().toISOString().slice(0, 10);
  const icon = { planned: ':calendar:', failed: ':x:', purchased: ':white_check_mark:', unknown: ':warning:', in_progress: ':hourglass:', partial: ':warning:' };
  const lines = rows.map(b => {
    const due = (b.status === 'planned' || b.status === 'failed') && (b.earliest_date || '') <= today ? ' · *due*' : '';
    return (icon[b.status] || '•') + ' `' + b.batch_id + '` ' + (b.earliest_date || '?') + ' · ' + (b.client || 'primary')
      + ' · ' + (b.domains || []).join(', ') + ' · ' + b.status + due;
  });
  return '*Domain purchase plan*' + NL + lines.join(NL)
    + NL + NL + '_Buying: the *Buy now* button below (approved buyers only), or `zapmail_buy.py --execute <batch_id> --approve`._';
}

/** Register the /zapmail command on an existing Bolt app (the /domains app). */
/** Turn a subcommand's JSON into a Slack message ({text, blocks?}). */
function render(sub, result) {
  const actions = require('./zapmail_actions');
  if (sub === 'status') return { text: formatStatus(result.status || {}) };
  if (sub === 'renewals') return { text: 'Zapmail renewals', blocks: actions.renewalBlocks(result) };
  if (sub === 'cross-check') return { text: formatCrossCheck(result.cross_check || {}) };
  if (sub === 'domain') return { text: 'Zapmail: ' + (result.domain || ''), blocks: actions.domainBlocks(result) };
  if (sub === 'prewarmed') return { text: actions.prewarmedText(result) };
  if (sub === 'sync') return { text: 'Tracker sync preview', blocks: actions.syncBlocks(result) };
  if (sub === 'batches') {
    // Due batches get a "Buy now" button — only when approvers exist;
    // the click handler checks the clicking user again.
    const { approvers, buyButtonBlocks } = require('./domain_suggest_command');
    const today = new Date().toISOString().slice(0, 10);
    const buttons = approvers().length && result.ledger_ok
      ? buyButtonBlocks(result.batches, today, null) : [];
    return { text: formatBatches(result),
      blocks: [{ type: 'section', text: { type: 'mrkdwn', text: formatBatches(result).slice(0, 2900) } }, ...buttons] };
  }
  return { text: result.text || 'No digest.' };
}

async function runSub(sub, arg, baseDir, respond) {
  const args = commandFor(sub, arg);
  if (!args) return respond({ response_type: 'ephemeral', text: ZAPMAIL_HELP });
  await respond({ response_type: 'ephemeral', replace_original: false,
    text: 'Checking Zapmail (`' + sub + '`)…' });
  try {
    const result = await runZapmailScript(args, baseDir);
    return respond({ response_type: 'ephemeral', replace_original: false, ...render(sub, result) });
  } catch (err) {
    console.error('[zapmail] failed:', err);
    return respond({ response_type: 'ephemeral', replace_original: false, text: ':x: ' + err.message });
  }
}

/**
 * Answer a typed Zapmail request ("", "status", "domain x.com", …).
 * Shared by `/zapmail` and `/domains zapmail …` — the second needs no extra
 * slash command registered in Slack, which only the app's owner can add.
 */
function handleZapmailText(text, baseDir, respond) {
  const tokens = (text || '').trim().toLowerCase().split(/\s+/).filter(Boolean);
  const sub = tokens[0];
  const arg = tokens[1] || '';
  if (!sub || sub === 'menu' || sub === 'home') {
    const { homeBlocks } = require('./zapmail_actions');
    return respond({ response_type: 'ephemeral', text: 'Zapmail', blocks: homeBlocks() });
  }
  if (sub === 'help') return respond({ response_type: 'ephemeral', text: ZAPMAIL_HELP });
  if (sub === 'domain' && !SAFE_DOMAIN_RE.test(arg)) {
    return respond({ response_type: 'ephemeral', text: 'Give a domain, e.g. `/domains zapmail domain example.com`' });
  }
  return runSub(sub, arg, baseDir, respond);
}

/** ZAPMAIL_SLASH_COMMAND renames it (e.g. `/zapmail-dev` on a local test app). */
function registerZapmailCommand(app, baseDir, opts = {}) {
  app.command(process.env.ZAPMAIL_SLASH_COMMAND || '/zapmail', async ({ ack, command, respond }) => {
    await ack();
    return handleZapmailText(command.text, baseDir, respond);
  });

  // Home-menu navigation buttons: zm_nav_<subcommand>.
  app.action(/^zm_nav_[a-z-]+$/, async ({ ack, action, respond }) => {
    await ack();
    const sub = String(action.action_id).replace(/^zm_nav_/, '');
    if (!commandFor(sub, '')) return;
    return runSub(sub, '', baseDir, respond);
  });

  require('./zapmail_actions').registerZapmailActions(app, baseDir, opts);
}

module.exports = {
  registerZapmailCommand,
  handleZapmailText,
  commandFor,
  render,
  ZAPMAIL_HELP,
  formatStatus,
  formatRenewals,
  formatCrossCheck,
  formatDomain,
  formatBatches
};
