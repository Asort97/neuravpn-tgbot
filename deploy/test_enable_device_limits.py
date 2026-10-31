import unittest
import pathlib
import sqlite3
import tempfile

from enable_device_limits import apply_batch, excess_devices, make_plan, enrollment_sql, read_snapshot


def client(uid, sub, email='test@example.invalid'):
    return {'tg_id': uid, 'sub_id': sub, 'email': email}


class RolloutTests(unittest.TestCase):
    def test_reads_native_wal_backup(self):
        with tempfile.TemporaryDirectory() as folder:
            path = pathlib.Path(folder) / 'source.db'
            db = sqlite3.connect(path)
            try:
                db.execute('PRAGMA journal_mode=WAL')
                db.executescript('CREATE TABLE clients(id INTEGER); CREATE TABLE client_hwids(id INTEGER);')
                db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
                self.assertEqual(read_snapshot(path.read_bytes()), ([], []))
            finally:
                db.close()

    def test_paid_slots_and_unmanaged_accounts(self):
        users = {'42': {'extra': 2}, '43': {'extra': 0}}
        clients = [client('42', 'a'), client('43', 'b'), client('99', 'c')]
        devices = [{'sub_id': 'a'}] * 10 + [{'sub_id': 'b'}] * 6 + [{'sub_id': 'c'}] * 30
        targets, subs, removed = make_plan(users, clients, devices)
        self.assertEqual([v['target_limit'] for v in targets], [7, 5])
        self.assertEqual(set(subs), {'a', 'b'})
        self.assertEqual(removed, 4)

    def test_ambiguous_ownership_is_rejected(self):
        for clients in [[client('42', 'a'), client('42', 'b')],
                        [client('42', 'a'), client('99', 'a')]]:
            with self.assertRaises(RuntimeError):
                make_plan({'42': {'extra': 0}}, clients, [])

    def test_keeps_latest_with_deterministic_tie_break(self):
        devices = [{'id': i, 'last_seen': i // 2, 'sub_id': 'a'} for i in range(10)]
        devices.append({'id': 99, 'last_seen': 0, 'sub_id': 'unmanaged'})
        subs = {'a': dict(client('42', 'a'), target_limit=5)}
        self.assertEqual([d['id'] for _, d in excess_devices(subs, devices)], [4, 3, 2, 1, 0])

    def test_bulk_mutates_only_limit(self):
        class Panel:
            def request(self, method, path, data):
                self.call = (method, path, data)
                return {'adjusted': 2, 'skipped': []}
        p = Panel()
        apply_batch(p, ['a', 'b'], 7)
        self.assertEqual(p.call, ('POST', '/panel/api/clients/bulkAdjust',
                                  {'emails': ['a', 'b'], 'limitHwid': 7}))

    def test_partial_batch_aborts(self):
        class Panel:
            def request(self, *args):
                return {'adjusted': 1, 'skipped': [{'email': 'b'}]}
        with self.assertRaises(RuntimeError):
            apply_batch(Panel(), ['a', 'b'], 5)

    def test_enrollment_preserves_paid_quantity(self):
        query = enrollment_sql({'42': {'extra': 2}}, [client('42', 'a')])
        self.assertIn('ON CONFLICT(user_id) DO UPDATE SET enabled=TRUE', query)
        self.assertNotIn('SET extra_slots=', query)
        self.assertIn('slot quantity changed', query)
        self.assertIn('UPDATE device_limit_policy SET enabled=TRUE', query)
        self.assertIn('"extra": 2', query)


if __name__ == '__main__':
    unittest.main()
