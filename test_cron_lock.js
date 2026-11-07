// The once-a-day lock must last the whole IST day. A 2-hour TTL on cronlocks (the old schema's
// createdAt expiry) would let a restart re-post Kill warnings, re-run copy sync --apply and the final
// placement report, and make a second daily health post (review of the bot-health branch, 7 Nov 2026).
// Run: node test_cron_lock.js   (no network, no Mongo)
const assert = require('assert');
const fs = require('fs');
const path = require('path');
const mongoose = require('mongoose');
const { CRON_LOCK_FIELDS, lockDoc, istDay, slotKey } = require('./bot_health');

let n = 0;
const ok = (cond, msg) => { assert.ok(cond, msg); n++; };
const eq = (a, b, msg) => { assert.deepStrictEqual(a, b, msg); n++; };

// The schema index.js builds has no TTL index: mongoose's autoIndex would build one on start.
const indexes = new mongoose.Schema(CRON_LOCK_FIELDS).indexes();
ok(indexes.every(([, opts]) => opts.expireAfterSeconds === undefined && opts.expires === undefined),
  `cronlocks schema declares a TTL index: ${JSON.stringify(indexes)}`);
eq(indexes.map(([keys]) => Object.keys(keys)), [['jobName']], 'only the unique jobName index');
ok(!('createdAt' in CRON_LOCK_FIELDS), 'no createdAt field for an old TTL index to cover');
// Sanity: the check above does catch the old schema.
const old = new mongoose.Schema({ jobName: String, createdAt: { type: Date, expires: '2h' } }).indexes();
ok(old.some(([, opts]) => opts.expireAfterSeconds === 7200), 'sanity: mongoose reports the old 2h TTL');

// The lock doc: the key is the _id, the time is takenAt, never createdAt.
const at = new Date('2026-11-10T08:00:00Z');
eq(lockDoc('kill-report-2026-11-10', at), { _id: 'kill-report-2026-11-10', jobName: 'kill-report-2026-11-10', takenAt: at });
ok(lockDoc('x').takenAt instanceof Date, 'takenAt defaults to now');

// index.js uses both, and every lock key carries the IST day or hour (with no TTL, a key without a
// date would block its job for good).
const src = fs.readFileSync(path.join(__dirname, 'index.js'), 'utf8');
ok(/const CronLockSchema = new mongoose\.Schema\(health\.CRON_LOCK_FIELDS\);/.test(src), 'index.js builds CronLockSchema from CRON_LOCK_FIELDS');
const takeLockFn = src.slice(src.indexOf('async function takeLock('), src.indexOf('\n}\n', src.indexOf('async function takeLock(')));
ok(/insertOne\(health\.lockDoc\(key\)\)/.test(takeLockFn), 'takeLock inserts health.lockDoc(key)');
ok(!/createdAt/.test(takeLockFn), 'takeLock never writes createdAt');
ok(!/expires:\s*'2h'/.test(src), 'no 2h expiry left in index.js');
const keys = [...src.matchAll(/takeLock\((`[^`]*`)\)/g)].map(m => m[1]);
eq(keys, ['`${job}-${health.istDay(now)}`', '`bot-health-restart-${health.slotKey(now).slice(0, 13)}`'],
  'every takeLock key is dated (onceToday: IST day; restart post: IST hour)');
eq(src.split('takeLock(').length - 1, keys.length + 1, 'no other takeLock call (the +1 is its definition)');
eq(istDay(new Date('2026-11-09T20:00:00Z')), '2026-11-10', 'IST day: 01:30 IST Tuesday is Tuesday');
eq(slotKey(new Date('2026-11-10T08:00:00Z')).slice(0, 13), '2026-11-10T13', 'IST hour');

console.log(`cron lock: ${n} passed`);
