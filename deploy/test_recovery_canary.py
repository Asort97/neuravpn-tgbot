import unittest

from recovery_canary import select_member, TITLE, transition


class CanaryTests(unittest.TestCase):
    def test_alert_edges(self):
        state, alerts = {}, []
        def send(k,v):
            alerts.append((k,v))
            return True
        for v in ("transfer","ok","transfer","transfer","transfer","ok","ok","ok"):
            transition(state,"DE",v,send)
        self.assertEqual(alerts,[("DE","transfer"),("DE","ok")])

    def test_failed_delivery_not_marked(self):
        s={}
        for _ in range(2): transition(s,"DE","missing",lambda *_:False)
        self.assertEqual(s["DE"]["announced"],"ok")
        transition(s,"DE","missing",lambda *_:True)
        self.assertEqual(s["DE"]["announced"],"missing")

    def test_reject_stale_endpoint(self):
        self.assertIsNone(select_member([{"remarks":TITLE,"outbounds":[{"tag":"trial-de",
               "settings":{"vnext":[{"address":"217.60.27.164","port":444}]}}]}],"DE"))
