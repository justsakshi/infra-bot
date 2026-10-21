/**
 * "Whose are these?" (inbox intake, 15 Oct): when new inboxes land in the
 * Precise Leads Smartlead account on a domain nobody has a client for, intake
 * DMs Manveen with [Precise Leads] [Melior] [Not ours, skip]. A click saves
 * the answer and sets that domain up at once (inbox_intake_cli.py --answer),
 * then the DM says what happened. Skip means it is never asked about again.
 */

const DOMAIN_RE = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$/;
const CLIENT = { pl: 'Precise Leads', melior: 'Melior', skip: 'skip' };

function day(iso) {
  const d = new Date(String(iso) + 'T00:00:00Z');
  return Number.isNaN(d.getTime()) ? String(iso) : d.getUTCDate() + ' ' + d.toLocaleString('en-GB', { month: 'short', timeZone: 'UTC' });
}

function resultText(r, domain, client) {
  const { plainError } = require('./slack_text');
  if (!r || r.error) return ':x: Could not set up *' + domain + '*: ' + plainError((r && r.error) || 'no result');
  if (r.client === 'skip') return 'Okay, *' + domain + '* is not ours. The bot will leave it alone and not ask again.';
  const problems = (r.problems || []).length ? '\n\n:warning: ' + r.problems.join('\n:warning: ') : '';
  if (!r.set_up) return ':warning: Saved *' + domain + '* as ' + client + ', but nothing was set up yet.' + problems;
  return ':white_check_mark: Set up ' + r.set_up + ' inbox' + (r.set_up === 1 ? '' : 'es') + ' on *' + domain + '* for '
    + client + (r.ready_on ? ', ready to use from ' + day(r.ready_on) : '') + '.' + problems;
}

function registerIntakeSlack(app, baseDir) {
  app.action(/^intake_client_(pl|melior|skip)$/, async ({ ack, action, body, respond }) => {
    await ack();
    const choice = String(action.action_id).replace('intake_client_', '');
    const domain = String(action.value || '').trim().toLowerCase();
    if (!DOMAIN_RE.test(domain)) return;
    const client = CLIENT[choice];
    const user = (body.user && body.user.id) || '';
    await respond({ replace_original: true, text: choice === 'skip' ? 'Saving…'
      : ':hourglass: Setting up *' + domain + '* for ' + client + ' (limit, warmup, signature, tags; about a minute)…' });
    let r;
    try {
      r = await require('./domain_suggest_command').runPy(
        ['inbox_intake_cli.py', '--answer', domain, choice, '--by', user], baseDir, 6 * 60 * 1000);
    } catch (err) {
      r = { error: err.message };
    }
    await respond({ replace_original: true, text: resultText(r, domain, client) });
  });
}

module.exports = { registerIntakeSlack, resultText };
