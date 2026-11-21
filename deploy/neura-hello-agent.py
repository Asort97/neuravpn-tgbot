#!/usr/bin/env python3
"""neura-hello agent: pushes a signed node summary to the bot once a minute.

Counters only. No client IP address ever leaves the node: connections are
counted per local port, unique peers are counted, and the ClientHello
statistics come from the hourly neura-hello capture, aggregated by ISP.

Config (/etc/neura-hello-agent.conf, KEY=VALUE):
  NODE=lv
  URL=https://webhook.example.com/neura-hello/report
  SECRET=<shared secret>
  PORTS="443 10001 10002"
  HELLO_CSV=/var/log/neura-hello.csv          (optional)
  HELLO_ASN_CSV=/var/log/neura-hello-asn.csv  (optional)
  CONNTRACK_DST=89.40.6.125  (relays: count kernel-forwarded connections)
"""
import collections
import csv
import hashlib
import hmac
import io
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone

CONF = os.environ.get("NEURA_HELLO_AGENT_CONF", "/etc/neura-hello-agent.conf")
STATE = "/var/lib/neura-hello-agent/net.json"
VIRTUAL_IFACES = ("lo", "docker", "veth", "br-", "virbr", "tun", "tap", "wg", "sing", "hy", "awg", "kube", "cni", "flannel")
SCAN_MIN_FLOWS = 50
SCAN_MIN_IPS = 30
HELLO_MAX_AGE = timedelta(hours=3)
TAIL_BYTES = 256 * 1024


def load_conf(path):
    conf = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            conf[key.strip()] = value.strip().strip('"').strip("'")
    return conf


def established(ports):
    out = subprocess.run(["ss", "-Htn", "state", "established"], capture_output=True, text=True, timeout=20).stdout
    conns = collections.Counter({p: 0 for p in ports})
    peers = set()
    for line in out.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        local_port = parts[2].rsplit(":", 1)[-1]
        if local_port in conns:
            conns[local_port] += 1
            peers.add(parts[3].rsplit(":", 1)[0])
    return dict(conns), len(peers)


def forwarded(ports, dst):
    """Connections a relay forwards in the kernel: they have no local socket,
    so count conntrack entries addressed to this host on the relayed ports."""
    out = subprocess.run(["conntrack", "-L"], capture_output=True, text=True, timeout=20).stdout
    conns = collections.Counter({p: 0 for p in ports})
    peers = set()
    for line in out.splitlines():
        fields = line.split()
        if not fields or fields[0] not in ("tcp", "udp"):
            continue
        if fields[0] == "tcp" and "ESTABLISHED" not in fields:
            continue
        orig = {}
        for f in fields:
            if "=" in f:
                k, v = f.split("=", 1)
                orig.setdefault(k, v)  # first occurrence is the original direction
        if orig.get("dst") == dst and orig.get("dport") in conns:
            conns[orig["dport"]] += 1
            peers.add(orig.get("src"))
    return dict(conns), len(peers)


def net_mbps():
    rx = tx = 0
    with open("/proc/net/dev") as f:
        for line in f.readlines()[2:]:
            name, data = line.split(":", 1)
            name = name.strip()
            if name.startswith(VIRTUAL_IFACES):
                continue
            fields = data.split()
            rx += int(fields[0])
            tx += int(fields[8])
    now = time.time()
    prev = None
    try:
        with open(STATE) as f:
            prev = json.load(f)
    except (OSError, ValueError):
        pass
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    with open(STATE, "w") as f:
        json.dump({"t": now, "rx": rx, "tx": tx}, f)
    if not prev or now <= prev["t"] or rx < prev["rx"] or tx < prev["tx"]:
        return 0.0, 0.0
    dt = now - prev["t"]
    return (rx - prev["rx"]) * 8 / dt / 1e6, (tx - prev["tx"]) * 8 / dt / 1e6


def mem_pct():
    info = {}
    with open("/proc/meminfo") as f:
        for line in f:
            key, value = line.split(":", 1)
            info[key] = int(value.split()[0])
    total = info.get("MemTotal", 0)
    return 100.0 * (total - info.get("MemAvailable", total)) / total if total else 0.0


def tail_rows(path):
    """Rows of a CSV with header, reading only the tail of a growing file."""
    with open(path, encoding="utf-8", errors="replace") as f:
        header = f.readline()
        f.seek(0, os.SEEK_END)
        size = f.tell()
        f.seek(max(len(header), size - TAIL_BYTES))
        chunk = f.read()
    if size > TAIL_BYTES:
        chunk = chunk.split("\n", 1)[-1]
    return list(csv.DictReader(io.StringIO(header + chunk)))


def latest(rows):
    if not rows:
        return None, []
    newest = max(r["time"] for r in rows)
    return newest, [r for r in rows if r["time"] == newest]


def hello_summary(path):
    try:
        at, rows = latest(tail_rows(path))
    except OSError:
        return None, {}
    if not at or datetime.now(timezone.utc) - datetime.fromisoformat(at.replace("Z", "+00:00")) > HELLO_MAX_AGE:
        return None, {}
    ports = collections.defaultdict(lambda: {"flows": 0, "stalled": 0, "frag": 0, "no_data": 0})
    for r in rows:
        p = ports[r["port"]]
        n = int(r["flows"])
        p["flows"] += n
        cls = r["class"]
        if cls == "big_stalled":
            p["stalled"] += n
        elif cls in ("single_packet", "fragmented"):
            p["frag"] += n
        elif cls == "no_data":
            p["no_data"] += n
    return at, dict(ports)


def scans(path):
    try:
        at, rows = latest(tail_rows(path))
    except OSError:
        return []
    if not at or datetime.now(timezone.utc) - datetime.fromisoformat(at.replace("Z", "+00:00")) > HELLO_MAX_AGE:
        return []
    found = []
    for r in rows:
        flows, ips = int(r["flows"]), int(r["client_ips"])
        if r["class"] == "no_data" and flows >= SCAN_MIN_FLOWS and ips >= SCAN_MIN_IPS:
            found.append({"asn": r["asn"], "name": r["name"][:60], "flows": flows, "ips": ips})
    return sorted(found, key=lambda s: -s["flows"])[:5]


def build_report(conf):
    ports = conf.get("PORTS", "").split()
    if conf.get("CONNTRACK_DST"):
        conns, clients = forwarded(ports, conf["CONNTRACK_DST"])
    else:
        conns, clients = established(ports)
    rx, tx = net_mbps()
    disk = shutil.disk_usage("/")
    report = {
        "node": conf["NODE"],
        "load1": round(os.getloadavg()[0], 2),
        "cpus": os.cpu_count() or 1,
        "mem_pct": round(mem_pct(), 1),
        # Same as df: blocks reserved for root count as unavailable.
        "disk_pct": round(100.0 * disk.used / (disk.used + disk.free), 1),
        "rx_mbps": round(rx, 1),
        "tx_mbps": round(tx, 1),
        "conns": conns,
        "clients": clients,
    }
    if conf.get("HELLO_CSV"):
        at, hello = hello_summary(conf["HELLO_CSV"])
        if at:
            report["hello_at"], report["hello"] = at, hello
    if conf.get("HELLO_ASN_CSV"):
        report["scans"] = scans(conf["HELLO_ASN_CSV"])
    return report


def push(conf, report):
    body = json.dumps(report, separators=(",", ":")).encode()
    ts = str(int(time.time()))
    sig = hmac.new(conf["SECRET"].encode(), ts.encode() + b"\n" + body, hashlib.sha256).hexdigest()
    req = urllib.request.Request(conf["URL"], data=body, method="POST", headers={
        "Content-Type": "application/json",
        "X-Neura-Timestamp": ts,
        "X-Neura-Signature": sig,
        "User-Agent": "neura-hello-agent/1",
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        return resp.status


def main():
    conf = load_conf(CONF)
    report = build_report(conf)
    if "--dry-run" in sys.argv:
        print(json.dumps(report, ensure_ascii=False, indent=1))
        return
    status = push(conf, report)
    if status != 204:
        sys.exit(f"unexpected status {status}")


if __name__ == "__main__":
    main()
