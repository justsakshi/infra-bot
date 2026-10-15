/**
 * Weekly volume review (spec 2026-10-15): the Tuesday post has one Apply button
 * per client. Approvers only (ZAPMAIL_APPROVERS or SCALEDMAIL_APPROVERS). A
 * click runs volume_review.py --apply-client, which writes each new daily
 * limit, reads it back from Smartlead and records it; the result is posted
 * in the channel so the team sees who changed what.
 */

const REVIEW_RE = /^\d{4}-\d{2}-\d{2}$/;
const CLIENTS = ['Precise Leads', 'Melior', 'Bettrdata'];

function canApply(user) {
  return require('./zapmail_actions').isApprover(user) || require('./scaledmail_command').isApprover(user);
}

function registerVolumeSlack(app, baseDir) {
  app.action(/^volume_apply_\d+$/, async ({ ack, action, body, respond }) => {
    await ack();
    const user = (body.user && body.user.id) || '';
    if (!canApply(user)) {
      return respond({ response_type: 'ephemeral', replace_original: false,
        text: ':lock: Only approvers can change daily limits (ZAPMAIL_APPROVERS / SCALEDMAIL_APPROVERS).' });
    }
    let p = {};
    try { p = JSON.parse(action.value); } catch (e) { return; }
    if (!REVIEW_RE.test(String(p.r)) || !CLIENTS.includes(p.c)) return;
    await respond({ response_type: 'ephemeral', replace_original: false,
      text: ':hourglass: Changing daily limits for ' + p.c + ' (each one is read back from Smartlead)…' });
    let r;
    try {
      r = await require('./domain_suggest_command').runPy(
        ['volume_review.py', '--apply-client', p.c, '--review', p.r, '--by', user, '--json'], baseDir, 5 * 60 * 1000);
    } catch (err) {
      r = { text: ':x: ' + require('./slack_text').plainError(err.message) };
    }
    await respond({ response_type: 'in_channel', replace_original: false,
      text: '<@' + user + '> applied: ' + ((r && r.text) || ':x: no result') });
  });
}

module.exports = { registerVolumeSlack };
