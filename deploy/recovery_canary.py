"""Read-only owner pilot: verify the issued endpoints from the bot server."""
import contextlib
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.request


TITLE = "Автовыбор · тест стабильности"
FIXTURE = "https://speed.cloudflare.com/__down?bytes=16384"
TARGETS = {"LV": ("213.21.208.254", 443), "DE": ("217.60.27.166", 444)}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None


def select_member(profiles, label):
    host, port = TARGETS[label]
    trial = next((p for p in profiles if p.get("remarks") == TITLE), {})
    for ob in trial.get("outbounds", []):
        tag = ob.get("tag", "")
        if label == "DE" and tag != "trial-de":
            continue
        if label == "LV" and not tag.endswith("-split"):
            continue
        ep = (ob.get("settings", {}).get("vnext") or [{}])[0]
        if (ep.get("address"), ep.get("port")) != (host, port):
            continue
        s = ob["streamSettings"]
        if (ob.get("protocol"), s.get("security"), s.get("network")) != ("vless", "reality", "xhttp"):
            continue
        # Never execute arbitrary routing, listeners, paths or dial options from the feed.
        reality = {k: v for k, v in s["realitySettings"].items() if k in
                   ("publicKey", "password", "shortId", "serverName", "fingerprint", "spiderX") and isinstance(v, str)}
        xhttp = {k: v for k, v in s["xhttpSettings"].items() if k in ("host", "path", "mode") and isinstance(v, str)}
        stream = dict(network="xhttp", security="reality", realitySettings=reality, xhttpSettings=xhttp)
        mask = {"tcp": [{"type": "fragment", "settings": {"packets": "tlshello", "length": "10-20", "interval": "10-20"}}]}
        if s.get("finalmask") != mask:
            continue
        stream["finalmask"] = mask
        u = ep["users"][0]
        return {"protocol": "vless", "settings": {"vnext": [{"address": host, "port": port,
                "users": [{"id": u["id"], "encryption": "none"}]}]}, "streamSettings": stream}
    return None


def download(proxy=None):
    cmd = ["/usr/bin/curl", "-q", "-fsS", "--noproxy", "" if proxy else "*",
           "--max-time", "12", "--max-filesize", "16384", "-w", "%{http_code}"]
    if proxy:
        cmd += ["--proxy", proxy]
    p = subprocess.run(cmd + [FIXTURE], capture_output=True, timeout=15)
    return p.returncode == 0 and len(p.stdout) == 16387 and p.stdout[-3:] == b"200"


def check_member(ob, binary):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    # An unguessable SOCKS login prevents other local processes borrowing this peer.
    password = os.urandom(16).hex()
    cfg = {"inbounds": [{"listen": "127.0.0.1", "port": port, "protocol": "socks",
            "settings": {"auth": "password", "accounts": [{"user": "probe", "pass": password}]}}], "outbounds": [ob]}
    with tempfile.TemporaryDirectory() as td:
        f = Path(td) / "config.json"
        f.write_text(json.dumps(cfg))
        proc = subprocess.Popen([binary, "run", "-c", str(f)], cwd=td, env={"PATH": "/usr/bin:/bin"},
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            for _ in range(40):
                if proc.poll() is not None:
                    return False
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=.1):
                        break
                except OSError:
                    time.sleep(.05)
            else:
                return False
            return download(f"socks5h://probe:{password}@127.0.0.1:{port}")
        finally:
            if proc.poll() is None:
                proc.terminate()
            try:
                proc.wait(3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


def transition(state, key, result, send):
    row = state.setdefault(key, {"announced": "ok"})
    row["count"] = min(2, row.get("count", 0) + 1) if row.get("last") == result else 1
    row["last"] = result
    if row["count"] == 2 and row["announced"] != result and send(key, result):
        row["announced"] = result


def main():
    os.umask(0o077)
    cfg = json.loads((Path(os.environ["CREDENTIALS_DIRECTORY"]) / "pilot").read_text())
    statefile = Path(os.environ["STATE_DIRECTORY"]) / "state.json"
    state = json.loads(statefile.read_text()) if statefile.exists() else {}
    http = urllib.request.build_opener(NoRedirect, urllib.request.ProxyHandler({}))

    def alert(key, result):
        descriptions = {"ok": "проверка снова проходит", "transfer": "контрольный файл не получен",
                        "missing": "в выданной подписке нет ожидаемого профиля или адреса",
                        "feed": "не удалось получить тестовую подписку", "control": "не прошла прямая контрольная загрузка"}
        msg = "проверка подписки · " + key + "\nс сервера бота: " + descriptions[result]
        data = json.dumps({"chat_id": cfg["chat"], "text": msg}).encode()
        try:
            req = urllib.request.Request("https://api.telegram.org/bot" + cfg["token"] + "/sendMessage",
                                         data=data, headers={"Content-Type": "application/json"})
            with http.open(req, timeout=8) as res:
                return json.load(res).get("ok") is True
        except Exception:
            return False

    results = {"контроль": "ok" if download() else "control"}
    try:
        req = urllib.request.Request(cfg["url"], headers={"User-Agent": "Happ/4.8.0", "X-HWID": cfg["hwid"]})
        with http.open(req, timeout=15) as resp:
            data = resp.read(4*1024*1024+1)
        if len(data)>4*1024*1024: raise ValueError("too large")
        profiles = json.loads(data)
        if not isinstance(profiles,list) or not any(p.get("remarks")==TITLE for p in profiles):
            raise ValueError("no trial")
        results["выдача"] = "ok"
    except Exception:
        results["выдача"] = "feed"
    if all(r=="ok" for r in results.values()):
        for label in TARGETS:
            try:
                ob=select_member(profiles,label)
                result="missing" if ob is None else "ok" if check_member(ob,cfg["binary"]) else "transfer"
            except (OSError,KeyError,IndexError,TypeError,ValueError,subprocess.TimeoutExpired):
                result="transfer"
            results[label]=result
    for key in set(state)-results.keys():
        state[key]["count"]=0
    for key,result in results.items():
        transition(state,key,result,alert)
    temp=statefile.with_suffix(".tmp")
    temp.write_text(json.dumps(state))
    temp.replace(statefile)
    print(json.dumps(results,ensure_ascii=False))


if __name__=="__main__":
    main()
