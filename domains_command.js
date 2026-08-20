/**
 * `/domains` — cold email domain suggester (SUGGEST-ONLY, never buys).
 *
 * Wraps `smartlead_sync/domain_generator.py`, which applies the 2026 naming
 * rules: no permutations of the client's own brand, no phishing shapes, .com
 * only, and no name we already own anywhere in the estate.
 *
 * Slack usage:
 *   /domains bettrdata.io data,ingest,lineage,dedupe,accuracy
 *   /domains bettrdata.io data,ingest,lineage need=6
 *   /domains bettrdata.io data,ingest exclude=bought-not-connected.com
 *
 * Domains already live in Smartlead are excluded automatically, so the same
 * name is never suggested twice across clients. `exclude=` covers domains
 * bought but not yet connected, which Smartlead cannot know about.
 */

const path = require('path');
const { spawn } = require('child_process');
const { App } = require('@slack/bolt');

const DOMAIN_HELP = [
  '*Usage:* `/domains <client-main-domain> <words> [need=N]`',
  '',
  '*Example:* `/domains bettrdata.io data,ingest,lineage,dedupe,accuracy need=6`',
  '',
  '`<client-main-domain>` — their REAL domain. Only used to reject lookalikes; we never send from it.',
  '`<words>` — 5-6 words describing what they sell or the problem they fix.',
  '`need=N` — how many you want (default 8, max 15).',
  '',
  '*Word choice decides everything:*',
  '• *Outcome words* beat activity words — `placed`, `booked`, `dedupe`, not `search`, `data`',
  '• *Niche words* beat category words — `jobsite`, `catalog`, not `business`, `service`',
  '• Give 5-6 words. Three words only makes 6 names and most will be taken.',
  '• Their brand word is rejected automatically — that pattern is what gets domains blacklisted.',
  '',
  '*Nothing to maintain:* every domain in the infra tracker (`/infra add`) and',
  'in Smartlead is excluded automatically, across all clients.',
  '`/domains owned` — see what it checks against.',
  '`/domains own <domains>` — only for a domain in neither place yet.',
  '',
  '_Suggests only. Confirm in Zapmail before buying, and buy in the order shown._'
].join('\n');

// Zapmail allows 10 domain searches per 30 minutes. Asking for more than that
// just yields unchecked names, so cap the request rather than pretend.
const MAX_NEED = 15;

// Slack renders a literal newline inside these strings.
const NL = String.fromCharCode(10);

function parseDomainsArgs(text) {
  const tokens = (text || '').trim().split(/\s+/).filter(Boolean);
  if (!tokens.length) return { error: 'help' };

  // Estate bookkeeping: `own <domains>` records domains we already have so
  // they are never suggested again; `owned` lists what is recorded.
  const verb = tokens[0].toLowerCase();
  if (verb === 'own' || verb === 'add') {
    const domains = tokens.slice(1).join(',');
    if (!domains) {
      return { error: 'Give me the domains to record, e.g. `/domains own boughtlastweek.com,another.com`' };
    }
    return { mode: 'register', domains };
  }
  if (verb === 'owned' || verb === 'list') {
    return { mode: 'list' };
  }

  const opts = { need: 8, exclude: '', words: '', mainDomain: '', client: '' };
  const positional = [];

  for (const tok of tokens) {
    const m = tok.match(/^(need|exclude|client)=(.*)$/i);
    if (!m) { positional.push(tok); continue; }
    const key = m[1].toLowerCase();
    if (key === 'need') {
      const n = parseInt(m[2], 10);
      if (Number.isFinite(n) && n > 0) opts.need = Math.min(n, MAX_NEED);
    } else if (key === 'exclude') {
      opts.exclude = m[2];
    } else {
      opts.client = m[2];
    }
  }

  if (positional.length < 2) return { error: 'help' };

  opts.mainDomain = positional[0]
    .toLowerCase()
    .replace(/^https?:\/\//, '')
    .replace(/\/.*$/, '');

  if (!opts.mainDomain.includes('.')) {
    return {
      error: '`' + positional[0] + '` does not look like a domain. Put the client’s '
           + 'main domain first, e.g. `bettrdata.io`.'
    };
  }

  opts.words = positional.slice(1).join(',');
  const wordCount = opts.words.split(',').map(w => w.trim()).filter(Boolean).length;
  if (wordCount < 3) {
    return {
      error: 'Give 5-6 words describing what the client sells or the problem '
           + 'they fix. Example: '
           + '`/domains bettrdata.io data,ingest,lineage,dedupe,accuracy`'
    };
  }

  if (!opts.client) opts.client = opts.mainDomain.split('.')[0];
  return opts;
}

function buildGeneratorArgs(opts, user) {
  if (opts.mode === 'register') {
    const args = ['domain_generator.py', '--register', opts.domains, '--json'];
    if (user) args.push('--added-by', user);
    return args;
  }
  if (opts.mode === 'list') {
    return ['domain_generator.py', '--list-owned', '--json'];
  }
  const args = [
    'domain_generator.py',
    '--client', opts.client,
    '--main-domain', opts.mainDomain,
    '--value', opts.words,
    '--need', String(opts.need),
    '--json'
  ];
  if (opts.exclude) args.push('--exclude', opts.exclude);
  return args;
}

function runDomainGenerator(opts, baseDir, user) {
  return new Promise((resolve, reject) => {
    const syncDir = path.join(baseDir, 'smartlead_sync');
    const args = buildGeneratorArgs(opts, user);

    // In the container `python` is the image interpreter and has every
    // dependency. Locally it usually is not — the deps live in
    // smartlead_sync/.venv — so allow an override rather than failing with a
    // bare ModuleNotFoundError.
    const python = process.env.INFRABOT_PYTHON || 'python';

    const proc = spawn(python, args, {
      cwd: syncDir,
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
    });

    let stdout = '';
    let stderr = '';
    proc.stdout.on('data', d => { stdout += d; });
    proc.stderr.on('data', d => { stderr += d; process.stderr.write('[domains] ' + d); });

    // Zapmail pacing (2s between searches) plus DNSBL lookups can take a
    // couple of minutes on a full run.
    const timer = setTimeout(() => {
      proc.kill();
      reject(new Error('timed out after 4 minutes'));
    }, 4 * 60 * 1000);

    proc.on('close', code => {
      clearTimeout(timer);
      if (code !== 0) {
        const tail = stderr.trim().split('\n').slice(-3).join('\n');
        return reject(new Error(tail || 'exited with code ' + code));
      }
      // The payload is the last non-empty stdout line; progress goes to stderr.
      const line = stdout.trim().split('\n').filter(Boolean).pop();
      if (!line) {
        // Exit code 0 with no payload means the script died before emitting —
        // most often a missing Python dependency. Say which, not "undefined".
        const missing = /ModuleNotFoundError: No module named '([^']+)'/.exec(stderr);
        return reject(new Error(
          missing
            ? 'Python dependency missing: ' + missing[1]
              + '. Set INFRABOT_PYTHON to an interpreter with the smartlead_sync '
              + 'requirements installed.'
            : 'the generator produced no output'
        ));
      }
      try {
        resolve(JSON.parse(line));
      } catch (err) {
        reject(new Error('could not parse result: ' + String(line).slice(0, 200)));
      }
    });

    proc.on('error', err => { clearTimeout(timer); reject(err); });
  });
}

function formatDomainResult(r) {
  const blocks = [];
  const avail = r.purchasable || [];
  const passing = r.passing || [];

  blocks.push({
    type: 'section',
    text: {
      type: 'mrkdwn',
      text: '*Domain suggestions — ' + r.client + '*\n'
          + 'Main domain `' + r.main_domain + '` (never suggested, used only to reject lookalikes)\n'
          + passing.length + ' names passed the naming rules · ' + avail.length + ' available to buy'
    }
  });

  if (avail.length) {
    const lines = avail.map(d => {
      const price = (d.price !== null && d.price !== undefined) ? '$' + d.price.toFixed(2) : '—';
      return '`' + d.domain + '`  ' + price;
    });
    blocks.push({
      type: 'section',
      text: { type: 'mrkdwn', text: '*Available:*\n' + lines.join('\n') }
    });
  } else if (r.availability_checked) {
    // Almost always the same cause: common two-word English compounds in .com
    // were all registered years ago. Distinctive or coined words are the fix,
    // so say that rather than just reporting nothing.
    blocks.push({
      type: 'section',
      text: {
        type: 'mrkdwn',
        text: ':warning: *All ' + passing.length + ' names are already registered.*' + NL
            + NL + 'Common two-word `.com` compounds were taken years ago. '
            + 'Try words that are more specific to this client:' + NL
            + '• the niche they serve, not the category '
            + '(`_dental_`, `_hvac_`, not `_business_`)' + NL
            + '• the outcome, not the activity '
            + '(`_placed_`, `_retained_`, not `_search_`)' + NL
            + '• two-word coinages are fine — they only have to read like a brand'
      }
    });
  } else {
    blocks.push({
      type: 'section',
      text: {
        type: 'mrkdwn',
        text: ':warning: Availability was not checked (`ZAPMAIL_API_KEY` missing). '
            + 'The names below are unverified.'
      }
    });
  }

  if ((r.plan || []).length) {
    const planLines = r.plan.map(b => {
      const when = b.day_offset === 0 ? 'today' : 'day +' + b.day_offset;
      return '*' + when + '* (' + b.registrar + '): '
           + b.domains.map(d => '`' + d + '`').join(', ');
    });
    blocks.push({
      type: 'section',
      text: {
        type: 'mrkdwn',
        text: '*Buy in this order* — staggering avoids the batch fingerprint:\n'
            + planLines.join('\n')
            + '\n\n_Est. $' + r.estimated_annual_usd + '/yr._'
      }
    });
  }

  // Surface only the rejection the requester could not have predicted: name
  // quality is self-evident, but "we already own this" is real information.
  const owned = (r.rejected || []).filter(x => (x.reasons || []).some(s => s.includes('owned')));
  if (owned.length) {
    const shown = owned.slice(0, 6).map(o => o.domain).join(', ');
    blocks.push({
      type: 'context',
      elements: [{
        type: 'mrkdwn',
        text: 'Skipped ' + owned.length + ' name(s) we already own: '
            + shown + (owned.length > 6 ? '…' : '')
      }]
    });
  }

  if ((r.brand_fragments || []).length) {
    blocks.push({
      type: 'context',
      elements: [{
        type: 'mrkdwn',
        text: ':information_source: These words are part of the client’s own brand, so they were '
            + 'skipped: ' + r.brand_fragments.join(', ')
            + '. Use words about the *problem* or *outcome* instead.'
      }]
    });
  }

  const est = r.estate || {};
  const estTotal = Object.values(est).reduce(function (a, b) { return a + b; }, 0);
  if (estTotal) {
    blocks.push({
      type: 'context',
      elements: [{
        type: 'mrkdwn',
        text: (r.estate_complete === false ? ':warning: ' : '')
            + 'Checked against ' + estTotal + ' domains we already own ('
            + (est.asset_tracker || 0) + ' from the infra tracker, '
            + (est.smartlead || 0) + ' from Smartlead)'
            + (r.estate_complete === false
               ? ' — a source failed, so this may be incomplete.' : '.')
      }]
    });
  } else if (r.estate_complete === false) {
    blocks.push({
      type: 'context',
      elements: [{
        type: 'mrkdwn',
        text: ':warning: Could not read our owned-domain list, so these names '
            + 'were NOT checked against what we already have.'
      }]
    });
  }

  blocks.push({
    type: 'context',
    elements: [{
      type: 'mrkdwn',
      text: 'Availability and price are inferred from Zapmail’s search — '
          + '*confirm in the Zapmail UI before buying*. After purchase: connect on Zapmail, '
          + '2 inboxes per domain, then warm 2-3 weeks before the first send.'
    }]
  });

  return blocks;
}

/** Register the /domains command on an existing Bolt app. */
function registerDomainsCommand(app, baseDir) {
  app.command('/domains', async ({ ack, command, respond }) => {
    await ack();
    const opts = parseDomainsArgs(command.text);

    if (opts.error === 'help') {
      return respond({ response_type: 'ephemeral', text: DOMAIN_HELP });
    }
    if (opts.error) {
      return respond({ response_type: 'ephemeral', text: ':x: ' + opts.error });
    }

    const user = (command.user_name || command.user_id || '').toString();

    // Bookkeeping verbs answer immediately and stay ephemeral — they are
    // admin, not a result the channel needs.
    if (opts.mode === 'register' || opts.mode === 'list') {
      try {
        const res = await runDomainGenerator(opts, baseDir, user);
        if (res.error) {
          return respond({ response_type: 'ephemeral', text: ':x: ' + res.error });
        }
        if (opts.mode === 'list') {
          const seed = res.seed_file || [];
          const rec = res.registered || [];
          const tracker = res.asset_tracker || [];
          return respond({
            response_type: 'ephemeral',
            text: '*Domains we already own* (' + res.total + ' known)' + NL
                + NL + '*From the infra asset tracker* (' + tracker.length + ')'
                + (res.asset_tracker_ok ? '' : '  :warning: _read failed_')
                + ' — everything added with `/infra add`. No extra work needed.'
                + NL + '*Added ad hoc* (' + rec.length + '): '
                + (rec.join(', ') || '_none_')
                + NL + '*Seed file* (' + seed.length + '): '
                + (seed.join(', ') || '_none_')
                + NL + NL + '_Smartlead sending domains are excluded automatically too._'
          });
        }
        const saved = res.saved || [];
        const bad = res.rejected || [];
        let msg = saved.length
          ? ':white_check_mark: Recorded ' + saved.length + ' domain(s): '
            + saved.map(d => '`' + d + '`').join(', ')
            + '\nThese will never be suggested again, for any client.'
          : ':warning: Nothing was recorded.';
        if (bad.length) {
          msg += '\n:x: Skipped (not valid domains): ' + bad.join(', ');
        }
        return respond({ response_type: 'ephemeral', text: msg });
      } catch (err) {
        console.error('[domains] estate command failed:', err);
        return respond({
          response_type: 'ephemeral',
          text: ':x: Failed: ' + err.message
        });
      }
    }

    await respond({
      response_type: 'in_channel',
      text: 'Generating domains for *' + opts.client + '* — excluding '
          + opts.mainDomain + ' lookalikes and everything we already own. Takes a minute.'
    });

    try {
      const result = await runDomainGenerator(opts, baseDir, user);
      await respond({
        response_type: 'in_channel',
        blocks: formatDomainResult(result),
        text: 'Domain suggestions for ' + result.client
      });
    } catch (err) {
      console.error('[domains] failed:', err);
      await respond({
        response_type: 'ephemeral',
        text: ':x: Domain generation failed: ' + err.message
      });
    }
  });
}

/**
 * Start `/domains` on its OWN Slack app, in this same process.
 *
 * Why a second app rather than another command on the Infra Bot app: the
 * Infra Bot app is owned by a deactivated Slack user, so its Slash Commands
 * cannot be edited. A separate app has separate tokens, so registering
 * `/domains` there changes nothing about `/infra`, the crons, or the digests —
 * they keep running on the original app and the original socket.
 *
 * No-ops when the tokens are absent, so a deploy without them is not a crash.
 * Returns the started App, or null when it did not start.
 */
async function startDomainsApp(baseDir) {
  const token = process.env.DOMAINS_SLACK_BOT_TOKEN;
  const appToken = process.env.DOMAINS_SLACK_APP_TOKEN;

  if (!token || !appToken) {
    console.log('ℹ️  /domains not started (DOMAINS_SLACK_BOT_TOKEN / '
              + 'DOMAINS_SLACK_APP_TOKEN not set)');
    return null;
  }

  const domainsApp = new App({ token, appToken, socketMode: true });
  registerDomainsCommand(domainsApp, baseDir);

  // Same socket-mode race the main app guards against: a forced disconnect
  // during a deploy overlap must be logged, not thrown as an unhandled
  // rejection that takes the process down.
  try {
    const smClient = domainsApp.receiver && domainsApp.receiver.client;
    if (smClient && typeof smClient.on === 'function') {
      smClient.on('error', (err) => {
        console.warn('⚠️  /domains socket-mode error (auto-reconnecting):',
                     err?.message || err);
      });
    }
  } catch (err) {
    console.warn('⚠️  Could not attach /domains socket error listener:',
                 err?.message || err);
  }

  await domainsApp.start();
  console.log('✅ /domains running in socket mode (separate Slack app)');
  return domainsApp;
}

module.exports = {
  registerDomainsCommand,
  startDomainsApp,
  parseDomainsArgs,
  formatDomainResult,
  DOMAIN_HELP
};
