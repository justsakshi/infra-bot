// Website login: every route needs it, the webhook and /healthz don't.
// Run: node test_web_auth.js   (starts a local server on a random port; no network)
const assert = require('assert');
const http = require('http');
const express = require('express');
const { requireLogin, singleFlight } = require('./web_auth');

function serve(env) {
  const app = express();
  app.post('/webhooks/zapmail/x', (req, res) => res.send('hook'));      // mounted before the login, as in index.js
  app.get('/healthz', (req, res) => res.send('ok'));
  app.use(requireLogin(env));
  app.get('/api/assets', (req, res) => res.send('assets'));
  app.delete('/api/assets/:n', (req, res) => res.send('deleted'));
  return new Promise(r => { const s = app.listen(0, () => r(s)); });
}

function req(port, method, path, auth) {
  return new Promise((resolve, reject) => {
    const headers = auth ? { Authorization: 'Basic ' + Buffer.from(auth).toString('base64') } : {};
    const r = http.request({ host: '127.0.0.1', port, method, path, headers }, res => {
      let body = '';
      res.on('data', d => { body += d; });
      res.on('end', () => resolve({ status: res.statusCode, body, www: res.headers['www-authenticate'] }));
    });
    r.on('error', reject);
    r.end();
  });
}

(async () => {
  let n = 0;
  const ok = (c, m) => { assert.ok(c, m); n++; };
  const s = await serve({ DASHBOARD_PASSWORD: 's3cret' });
  const p = s.address().port;
  ok((await req(p, 'GET', '/api/assets')).status === 401, 'no login -> 401');
  ok(/Basic/.test((await req(p, 'GET', '/api/assets')).www || ''), 'browser is asked to log in');
  ok((await req(p, 'DELETE', '/api/assets/x.com')).status === 401, 'delete without login refused');
  ok((await req(p, 'GET', '/api/assets', 'infra:wrong')).status === 401, 'wrong password refused');
  ok((await req(p, 'GET', '/api/assets', 'other:s3cret')).status === 401, 'wrong user refused');
  ok((await req(p, 'GET', '/api/assets', 'infra:s3cret')).body === 'assets', 'right login works');
  ok((await req(p, 'POST', '/webhooks/zapmail/x')).body === 'hook', 'signed webhook stays reachable');
  ok((await req(p, 'GET', '/healthz')).body === 'ok', 'health check stays open');
  s.close();

  const locked = await serve({});
  const lp = locked.address().port;
  ok((await req(lp, 'GET', '/api/assets', 'infra:')).status === 503, 'no password configured -> everything refused');
  locked.close();

  const f = singleFlight();
  ok(f.tryStart('sync') && !f.tryStart('sync'), 'second trigger refused while the first runs');
  f.done('sync');
  ok(f.tryStart('sync'), 'allowed again after it finishes');
  console.log(n + ' passed');
})().catch(e => { console.error(e); process.exit(1); });
