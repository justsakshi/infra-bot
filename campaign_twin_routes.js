/**
 * `/api/twins/*` — Campaign twins (open-tracking clone of an existing
 * Smartlead campaign). Suggest-and-create only; the twin is NEVER started.
 *
 * Wraps `smartlead_sync/campaign_twin_executor.py`, run from cwd
 * `smartlead_sync`. Spawn pattern copied from `runDomainGenerator` in
 * domains_command.js: honour `process.env.INFRABOT_PYTHON || 'python'`,
 * collect stdout, parse the LAST non-empty stdout line as JSON, surface
 * stderr on failure, 4-minute timeout.
 */

const path = require('path');
const { spawn } = require('child_process');

function runCampaignTwinExecutor(args, baseDir) {
  return new Promise((resolve, reject) => {
    const syncDir = path.join(baseDir, 'smartlead_sync');

    // In the container `python` is the image interpreter and has every
    // dependency. Locally it usually is not — the deps live in
    // smartlead_sync/.venv — so allow an override rather than failing with a
    // bare ModuleNotFoundError.
    const python = process.env.INFRABOT_PYTHON || 'python';

    const proc = spawn(python, ['campaign_twin_executor.py', ...args], {
      cwd: syncDir,
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
    });

    let stdout = '';
    let stderr = '';
    proc.stdout.on('data', d => { stdout += d; });
    proc.stderr.on('data', d => { stderr += d; process.stderr.write('[twins] ' + d); });

    const timer = setTimeout(() => {
      proc.kill();
      reject(new Error('timed out after 4 minutes'));
    }, 4 * 60 * 1000);

    proc.on('close', code => {
      clearTimeout(timer);

      // A stderr line starting with "ERROR:" is a documented failure signal
      // even when the exit code is 0.
      const errLine = stderr.split('\n').find(l => l.trim().startsWith('ERROR:'));

      if (code !== 0 || errLine) {
        const tail = stderr.trim().split('\n').slice(-3).join('\n');
        return reject(new Error(errLine ? errLine.trim() : (tail || 'exited with code ' + code)));
      }

      // The payload is the last non-empty stdout line; progress goes to stderr.
      const line = stdout.trim().split('\n').filter(Boolean).pop();
      if (!line) {
        const missing = /ModuleNotFoundError: No module named '([^']+)'/.exec(stderr);
        return reject(new Error(
          missing
            ? 'Python dependency missing: ' + missing[1]
              + '. Set INFRABOT_PYTHON to an interpreter with the smartlead_sync '
              + 'requirements installed.'
            : 'the executor produced no output'
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

/** Register the `/api/twins/*` routes on an existing Express app. */
function registerCampaignTwinRoutes(expressApp, baseDir) {
  expressApp.get('/api/twins/accounts', async (req, res) => {
    try {
      const result = await runCampaignTwinExecutor(['--accounts', '--json'], baseDir);
      res.json(result);
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  });

  expressApp.get('/api/twins/campaigns', async (req, res) => {
    try {
      const account = (req.query.account || '').toString();
      if (!account) {
        return res.status(400).json({ error: 'account is required' });
      }
      const result = await runCampaignTwinExecutor(
        ['--list', '--account', account, '--json'], baseDir
      );
      res.json(result);
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  });

  expressApp.post('/api/twins', async (req, res) => {
    try {
      const account = (req.body && req.body.account || '').toString();
      const campaignId = (req.body && req.body.campaignId || '').toString();
      const dryRun = !!(req.body && req.body.dryRun);

      if (!account) {
        return res.status(400).json({ error: 'account is required' });
      }
      if (!/^\d+$/.test(campaignId)) {
        return res.status(400).json({ error: 'campaignId must be digits only' });
      }
      const createdBy = (req.body && req.body.createdBy || '').toString();
      // Values are separate argv entries (no shell), so there is no command
      // injection surface - but a value beginning with "-" would be read by
      // argparse as a flag rather than a value. Refuse those outright.
      if (account.startsWith('-') || createdBy.startsWith('-')) {
        return res.status(400).json({ error: 'values may not start with "-"' });
      }

      const args = ['--account', account, '--campaign', campaignId, '--json'];
      if (dryRun) args.push('--dry-run');
      if (createdBy) args.push('--created-by', createdBy);

      const result = await runCampaignTwinExecutor(args, baseDir);
      res.json(result);
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  });
}

module.exports = { registerCampaignTwinRoutes };
