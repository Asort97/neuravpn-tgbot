#!/usr/bin/env python3
"""Preview by default. --apply enables five slots for existing and future accounts.

Run as root on the bot host. Requires Python 3.11+, psql and pg_dump.
Only the primary panel registry is changed; merged links use its existing gate.
"""
import argparse
import collections
import datetime
import http.cookiejar
import json
import os
import pathlib
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request


def load_env(path):
    raw = subprocess.check_output(
        ['bash', '-c', 'set -a; source "$1"; env -0', 'env', str(path)])
    return dict(part.decode().split('=', 1) for part in raw.split(b'\0') if b'=' in part)


def sql(env, query):
    p = subprocess.run(['psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-d', env['DB_DSN']],
                       input=query, text=True, capture_output=True, timeout=45)
    if p.returncode:
        raise RuntimeError('PostgreSQL operation failed; no secrets printed')
    return p.stdout.strip()


def accounts(env):
    raw = sql(env, """SELECT COALESCE(json_agg(json_build_object('id',u.id,
      'extra',COALESCE(p.extra_slots,0),'enabled',COALESCE(p.enabled,FALSE))), '[]')
      FROM users u LEFT JOIN device_plans p ON p.user_id=u.id
      WHERE u.id ~ '^[1-9][0-9]*$';""")
    return {v['id']: v for v in json.loads(raw)}


class SameOriginRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        old, new = urllib.parse.urlsplit(req.full_url), urllib.parse.urlsplit(newurl)
        if new.scheme != 'https' or new.netloc != old.netloc:
            raise RuntimeError('unexpected panel redirect')
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class Panel:
    def __init__(self, env):
        host = urllib.parse.urlsplit(env['XRAY_HOST'] if '://' in env['XRAY_HOST'] else 'https://' + env['XRAY_HOST'])
        port = env.get('XRAY_PORT') or host.port or 443
        self.base = 'https://' + host.hostname + ':' + str(port) + '/' + env.get('XRAY_WEB_BASE_PATH', '').strip('/')
        self.base = self.base.rstrip('/')
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()), SameOriginRedirect())
        self.headers = {'Accept': 'application/json'}
        token = env.get('XRAY_API_TOKEN', '')
        if token:
            self.headers['Authorization'] = 'Bearer ' + token
        else:
            csrf = self.request('GET', '/csrf-token')
            self.headers['X-CSRF-Token'] = csrf
            self.request('POST', '/login', {'username': env['XRAY_USERNAME'], 'password': env['XRAY_PASSWORD']})

    def request(self, method, path, data=None, binary=False):
        headers = dict(self.headers)
        payload = None
        if data is not None:
            payload = json.dumps(data).encode()
            headers['Content-Type'] = 'application/json'
        req = urllib.request.Request(self.base + path, data=payload, headers=headers, method=method)
        try:
            with self.opener.open(req, timeout=60) as r:
                body = r.read(128 * 1024 * 1024 + 1)
                if r.status != 200 or len(body) > 128 * 1024 * 1024:
                    raise RuntimeError('invalid panel response')
        except Exception:
            raise RuntimeError('panel request failed; stop and rerun preview before retrying') from None
        if binary:
            if not body.startswith(b'SQLite format 3\x00'):
                raise RuntimeError('panel database backup invalid')
            return body
        result = json.loads(body)
        if result.get('success') is not True:
            raise RuntimeError('panel did not confirm operation')
        return result.get('obj')

    def snapshot(self):
        return self.request('GET', '/panel/api/server/getDb', binary=True)


def read_snapshot(raw):
    # A native backup can retain WAL mode. deserialize() cannot open its WAL
    # from an in-memory DB; inspect a private temporary copy instead.
    with tempfile.TemporaryDirectory(prefix='neuravpn-panel-') as folder:
        path = pathlib.Path(folder) / 'snapshot.db'
        path.write_bytes(raw)
        db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
        try:
            db.row_factory = sqlite3.Row
            if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                raise RuntimeError('panel backup integrity check failed')
            clients = [dict(v) for v in db.execute('SELECT * FROM clients')]
            devices = [dict(v) for v in db.execute('SELECT * FROM client_hwids')]
            return clients, devices
        finally:
            db.close()


def make_plan(users, clients, devices):
    targets, subs, per_user = [], {}, collections.defaultdict(set)
    all_owners = collections.defaultdict(set)
    for c in clients:
        if c['sub_id']:
            all_owners[c['sub_id']].add(str(c['tg_id']))
        uid = str(c['tg_id'])
        if uid not in users or not c['sub_id']:
            continue
        extra = users[uid]['extra']
        if not isinstance(extra, int) or not 0 <= extra <= 95:
            raise RuntimeError('invalid extra slot quantity')
        target = dict(c, target_limit=5 + extra)
        targets.append(target)
        subs[c['sub_id']] = target
        per_user[uid].add(c['sub_id'])
    if any(len(v) > 1 for v in per_user.values()):
        raise RuntimeError('an account has multiple subscription IDs; consolidate before global rollout')
    if any(len(all_owners[s]) != 1 for s in subs):
        raise RuntimeError('subscription ID shared by different accounts; refusing rollout')
    count = collections.Counter(d['sub_id'] for d in devices)
    removed = sum(max(0, count[s] - c['target_limit']) for s, c in subs.items())
    return targets, subs, removed


def apply_batch(panel, emails, limit):
    # Only this field: do not reset flow, expiry, enabled state or credentials.
    result = panel.request('POST', '/panel/api/clients/bulkAdjust', {'emails': emails, 'limitHwid': limit})
    if not isinstance(result, dict) or result.get('adjusted') != len(emails) or result.get('skipped'):
        raise RuntimeError('incomplete panel batch; PostgreSQL/global policy not enabled by this run')


def excess_devices(subs, devices):
    by_sub = collections.defaultdict(list)
    for d in devices:
        by_sub[d['sub_id']].append(d)
    for sub, c in subs.items():
        ordered = sorted(by_sub[sub], key=lambda d: (d['last_seen'], d['id']), reverse=True)
        for d in ordered[c['target_limit']:]:
            yield c, d


def enrollment_sql(users, targets):
    verified = {str(c['tg_id']) for c in targets}
    expected = json.dumps([{'id': uid, 'extra': v['extra'],
                           'verified': uid in verified}
                          for uid, v in users.items()])
    expected_sql = "'" + expected.replace("'", "''") + "'::jsonb"
    return """BEGIN;
SET LOCAL lock_timeout='10s';
LOCK TABLE users IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE device_plans IN SHARE ROW EXCLUSIVE MODE;
CREATE TEMP TABLE rollout_expected ON COMMIT DROP AS
SELECT id,extra,verified FROM jsonb_to_recordset(""" + expected_sql + """) AS x(id text,extra int,verified boolean);
DO $$ BEGIN
 IF (SELECT count(*) FROM device_limit_policy WHERE singleton) <> 1 THEN
 RAISE EXCEPTION 'policy row missing'; END IF;
 IF EXISTS(SELECT 1 FROM rollout_expected e LEFT JOIN device_plans p ON p.user_id=e.id
 WHERE e.extra <> COALESCE(p.extra_slots,0)) THEN RAISE EXCEPTION 'slot quantity changed'; END IF;
END $$;
INSERT INTO device_plans(user_id,enabled) SELECT id,TRUE FROM users WHERE id ~ '^[1-9][0-9]*$'
ON CONFLICT(user_id) DO UPDATE SET enabled=TRUE,updated_at=NOW();
UPDATE device_plans p SET synced_extra_slots=p.extra_slots,last_synced_at=NOW()
FROM rollout_expected e WHERE p.user_id=e.id AND e.verified AND p.extra_slots=e.extra;
UPDATE device_limit_policy SET enabled=TRUE WHERE singleton;
COMMIT;
SELECT count(*) FROM device_plans WHERE enabled;
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--env', default='/srv/vpnbot/.env')
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise RuntimeError('run as root on the bot server')
    os.umask(0o077)
    # One operator at a time; never let duplicate commands race on snapshots.
    import fcntl
    lock = open('/run/neuravpn-device-rollout.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    env = load_env(args.env)
    if not env.get('DB_DSN'):
        env['DB_DSN'] = load_env('/opt/neuravpn-site/.env.web')['DB_DSN']
    # Refuse an older deployment before any panel mutation.
    policy = sql(env, 'SELECT enabled FROM device_limit_policy WHERE singleton; SELECT synced_extra_slots,last_synced_at FROM device_plans LIMIT 0;')
    if policy not in ('t', 'f'):
        raise RuntimeError('policy row missing')
    users = accounts(env)
    panel = Panel(env)
    before = panel.snapshot()
    clients, devices = read_snapshot(before)
    targets, subs, removed = make_plan(users, clients, devices)
    if not targets:
        raise RuntimeError('no managed subscriptions found')
    print(f'accounts: {len(users)}; panel clients: {len(targets)}; excess HWID records: {removed}', flush=True)
    print('keep: most recently used; limit: 5 + purchased slots; future accounts: enabled after successful apply', flush=True)
    if not args.apply:
        print('PREVIEW ONLY: no limits or business data changed')
        return
    if shutil.disk_usage('/root').free < 512 * 1024 * 1024:
        raise RuntimeError('less than 512 MiB free; free disk space before applying')
    backup = pathlib.Path(tempfile.mkdtemp(prefix='device-rollout-' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ-'), dir='/root'))
    (backup / 'panel-before.db').write_bytes(before)
    (backup / 'plans-before.json').write_text(json.dumps(users))
    p = subprocess.run(['pg_dump', '-d', env['DB_DSN'], '-Fc', '-f', str(backup / 'postgres-before.dump')], capture_output=True, timeout=120)
    if p.returncode or (backup / 'postgres-before.dump').stat().st_size == 0:
        raise RuntimeError('PostgreSQL backup failed; nothing applied')
    print('backup:', backup, flush=True)
    groups = collections.defaultdict(list)
    for c in targets:
        # Reapplying trims active subscriptions even if their limit is unchanged.
        groups[c['target_limit']].append(c['email'])
    for limit, emails in sorted(groups.items()):
        for start in range(0, len(emails), 100):
            batch = emails[start:start + 100]
            apply_batch(panel, batch, limit)
            print(f'panel: {start + len(batch)}/{len(emails)}, limit={limit}', flush=True)
            time.sleep(0.15)
    after_raw = panel.snapshot()
    (backup / 'panel-after.db').write_bytes(after_raw)
    after_clients, after_devices = read_snapshot(after_raw)
    by_id = {v['id']: v for v in after_clients}
    for c in targets:
        now = by_id.get(c['id'])
        if now is None or now['limit_hwid'] != c['target_limit']:
            raise RuntimeError('native limit verification failed; global policy not enabled')
        for field in ['uuid', 'password', 'auth', 'email', 'sub_id', 'tg_id', 'flow', 'security']:
            if now[field] != c[field]:
                raise RuntimeError('client identity changed concurrently; global policy not enabled')
    # Disabled subscriptions have effective native limit zero and are not trimmed
    # by bulkAdjust. Explicit deletion remains scoped to their subscription only.
    remaining = list(excess_devices(subs, after_devices))
    for c, d in remaining:
        panel.request('DELETE', '/panel/api/clients/hwids/' + urllib.parse.quote(c['email'], safe='') + '/' + str(d['id']))
    if remaining:
        final_raw = panel.snapshot()
        (backup / 'panel-after.db').write_bytes(final_raw)
        _, final_devices = read_snapshot(final_raw)
        if list(excess_devices(subs, final_devices)):
            raise RuntimeError('HWID removal not verified; global policy not enabled')
    fresh = accounts(env)
    if any(uid not in fresh or fresh[uid]['extra'] != v['extra'] for uid, v in users.items()):
        raise RuntimeError('paid slot quantities changed; rerun preview/apply; quantities were not overwritten')
    # Commit existing enrollment and future-user policy together. The users lock
    # closes the concurrent signup gap; paid quantities are never reset to zero.
    enabled = sql(env, enrollment_sql(users, targets))
    print('ENABLED EXISTING AND FUTURE ACCOUNTS; enabled count:', enabled, flush=True)
    print('HWID records removed beyond the most recently used 5 + paid slots. Saved tunnel keys are not revoked.')


if __name__ == '__main__':
    try:
        main()
    except (Exception, KeyboardInterrupt) as error:
        # Never expose response bodies, credentials or subscription URLs.
        print('STOPPED:', str(error) if isinstance(error, RuntimeError) else type(error).__name__, file=sys.stderr)
        sys.exit(1)
