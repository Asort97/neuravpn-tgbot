#!/usr/bin/env python3
"""
Normalize 3x-ui v3.1 client records after upgrading from per-inbound clients.

The script fixes the common broken state where the same Telegram user exists as
several settings.clients entries across target inbounds. 3x-ui v3.1 expects one
row in clients and one row per inbound in client_inbounds.
"""

import argparse
import json
import os
import re
import shutil
import sqlite3
import time
from collections import defaultdict


TG_EMAIL_RE = re.compile(r"(?:^|[+_])tg(\d+)(?:[+_@]|$)", re.IGNORECASE)
TG_COMMENT_RE = re.compile(r"^tg:(\d+)$", re.IGNORECASE)
SUB_RE = re.compile(r"^sub(\d+)$", re.IGNORECASE)


def parse_inbounds(raw):
    out = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    if not out:
        raise SystemExit("no inbound ids provided")
    return out


def load_settings(raw):
    if raw is None:
        return {}
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    data = json.loads(raw or "{}")
    if isinstance(data, str):
        data = json.loads(data or "{}")
    if not isinstance(data, dict):
        raise ValueError("settings is not an object")
    clients = data.get("clients")
    if not isinstance(clients, list):
        data["clients"] = []
    return data


def dump_settings(settings):
    return json.dumps(settings, ensure_ascii=False, separators=(",", ":"))


def int_or_zero(value):
    if value is None:
        return 0
    if isinstance(value, bool):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def first_text(*values):
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def telegram_id(client):
    tg = int_or_zero(client.get("tgId"))
    if tg:
        return str(tg)

    comment = first_text(client.get("comment"))
    m = TG_COMMENT_RE.match(comment)
    if m:
        return m.group(1)

    sub = first_text(client.get("subId"))
    m = SUB_RE.match(sub)
    if m:
        return m.group(1)

    email = first_text(client.get("email"))
    m = TG_EMAIL_RE.search(email)
    if m:
        return m.group(1)

    return ""


def group_key(client):
    tg = telegram_id(client)
    if tg:
        return "tg:" + tg
    uuid = first_text(client.get("id"), client.get("uuid"))
    if uuid:
        return "uuid:" + uuid.lower()
    email = first_text(client.get("email")).lower()
    if email:
        return "email:" + email
    return "anon:" + str(id(client))


def client_sort_key(item, inbound_order):
    inbound_id, client = item
    expiry = int_or_zero(client.get("expiryTime"))
    enabled = 1 if bool(client.get("enable")) else 0
    order = inbound_order.get(inbound_id, 999999)
    return (-enabled, -expiry, order)


def canonical_client(items, inbound_order):
    chosen_inbound, chosen = sorted(items, key=lambda item: client_sort_key(item, inbound_order))[0]
    merged = dict(chosen)

    expiry = max(int_or_zero(c.get("expiryTime")) for _, c in items)
    total_values = [int_or_zero(c.get("totalGB")) for _, c in items]
    total = 0 if 0 in total_values else max(total_values or [0])
    limit_ip = max(int_or_zero(c.get("limitIp")) for _, c in items)
    reset = max(int_or_zero(c.get("reset")) for _, c in items)
    tg = telegram_id(chosen) or next((telegram_id(c) for _, c in items if telegram_id(c)), "")

    merged["expiryTime"] = expiry
    merged["totalGB"] = total
    merged["limitIp"] = limit_ip
    merged["reset"] = reset
    merged["enable"] = any(bool(c.get("enable")) for _, c in items)
    if tg:
        merged["tgId"] = int(tg)
        if not first_text(merged.get("subId")):
            merged["subId"] = "sub" + tg
        if not first_text(merged.get("comment")):
            merged["comment"] = "tg:" + tg

    for key in ("id", "password", "auth", "email", "subId", "flow", "security", "comment"):
        merged[key] = first_text(merged.get(key), *(c.get(key) for _, c in items))

    created = [int_or_zero(c.get("created_at") or c.get("createdAt")) for _, c in items]
    updated = [int_or_zero(c.get("updated_at") or c.get("updatedAt")) for _, c in items]
    created = [v for v in created if v > 0]
    updated = [v for v in updated if v > 0]
    if created:
        merged["created_at"] = min(created)
    if updated:
        merged["updated_at"] = max(updated)

    return chosen_inbound, merged


def record_from_client(client, now_ms):
    reverse = client.get("reverse")
    if isinstance(reverse, (dict, list)):
        reverse = json.dumps(reverse, ensure_ascii=False, separators=(",", ":"))
    elif reverse is None:
        reverse = ""

    return {
        "email": first_text(client.get("email")),
        "sub_id": first_text(client.get("subId")),
        "uuid": first_text(client.get("id"), client.get("uuid")),
        "password": first_text(client.get("password")),
        "auth": first_text(client.get("auth")),
        "flow": first_text(client.get("flow")),
        "security": first_text(client.get("security")),
        "reverse": first_text(reverse),
        "limit_ip": int_or_zero(client.get("limitIp")),
        "total_gb": int_or_zero(client.get("totalGB")),
        "expiry_time": int_or_zero(client.get("expiryTime")),
        "enable": 1 if bool(client.get("enable")) else 0,
        "tg_id": int_or_zero(client.get("tgId")),
        "comment": first_text(client.get("comment")),
        "reset": int_or_zero(client.get("reset")),
        "created_at": int_or_zero(client.get("created_at") or client.get("createdAt")) or now_ms,
        "updated_at": int_or_zero(client.get("updated_at") or client.get("updatedAt")) or now_ms,
    }


def table_exists(conn, table):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def upsert_client(conn, client, now_ms):
    rec = record_from_client(client, now_ms)
    if not rec["email"]:
        raise ValueError("canonical client has empty email")

    row = conn.execute("SELECT id FROM clients WHERE email = ?", (rec["email"],)).fetchone()
    columns = [
        "email", "sub_id", "uuid", "password", "auth", "flow", "security", "reverse",
        "limit_ip", "total_gb", "expiry_time", "enable", "tg_id", "comment", "reset",
        "created_at", "updated_at",
    ]
    if row:
        client_id = int(row[0])
        assignments = ", ".join(f"{c}=?" for c in columns[1:])
        conn.execute(
            f"UPDATE clients SET {assignments} WHERE id=?",
            [rec[c] for c in columns[1:]] + [client_id],
        )
        return client_id

    placeholders = ",".join("?" for _ in columns)
    conn.execute(
        f"INSERT INTO clients ({','.join(columns)}) VALUES ({placeholders})",
        [rec[c] for c in columns],
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def merge_traffic(conn, canonical_email, old_emails):
    if not table_exists(conn, "client_traffics"):
        return
    emails = [e for e in old_emails if e]
    if not emails:
        return
    rows = conn.execute(
        "SELECT inbound_id, enable, email, up, down, expiry_time, total, reset, last_online "
        f"FROM client_traffics WHERE email IN ({','.join('?' for _ in emails)})",
        emails,
    ).fetchall()
    if not rows:
        return
    up = sum(int_or_zero(r[3]) for r in rows)
    down = sum(int_or_zero(r[4]) for r in rows)
    expiry = max(int_or_zero(r[5]) for r in rows)
    totals = [int_or_zero(r[6]) for r in rows]
    total = 0 if 0 in totals else max(totals or [0])
    reset = max(int_or_zero(r[7]) for r in rows)
    last_online = max(int_or_zero(r[8]) for r in rows)
    inbound_id = int_or_zero(rows[0][0])
    enable = 1 if any(bool(r[1]) for r in rows) else 0

    conn.execute("DELETE FROM client_traffics WHERE email IN (" + ",".join("?" for _ in emails) + ")", emails)
    conn.execute(
        "INSERT INTO client_traffics (inbound_id, enable, email, up, down, expiry_time, total, reset, last_online) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (inbound_id, enable, canonical_email, up, down, expiry, total, reset, last_online),
    )


def migrate(db_path, inbound_ids, dry_run):
    if not os.path.exists(db_path):
        raise SystemExit(f"db not found: {db_path}")

    backup_path = db_path + ".bak-v31-migrate-" + time.strftime("%Y%m%d-%H%M%S")
    if not dry_run:
        shutil.copy2(db_path, backup_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        now_ms = int(time.time() * 1000)
        inbound_order = {inbound_id: i for i, inbound_id in enumerate(inbound_ids)}

        inbound_rows = conn.execute(
            "SELECT id, settings FROM inbounds WHERE id IN (" + ",".join("?" for _ in inbound_ids) + ")",
            inbound_ids,
        ).fetchall()
        if len(inbound_rows) != len(set(inbound_ids)):
            found = {int(r[0]) for r in inbound_rows}
            missing = [str(i) for i in inbound_ids if i not in found]
            raise SystemExit("missing inbound ids: " + ",".join(missing))

        settings_by_inbound = {}
        groups = defaultdict(list)
        old_emails_by_group = defaultdict(set)
        for inbound_id, raw_settings in inbound_rows:
            inbound_id = int(inbound_id)
            settings = load_settings(raw_settings)
            settings_by_inbound[inbound_id] = settings
            for client in settings.get("clients", []):
                if not isinstance(client, dict):
                    continue
                key = group_key(client)
                groups[key].append((inbound_id, client))
                email = first_text(client.get("email"))
                if email:
                    old_emails_by_group[key].add(email)

        new_clients_by_inbound = defaultdict(list)
        canonical_by_key = {}
        for key, items in groups.items():
            _, canonical = canonical_client(items, inbound_order)
            canonical_by_key[key] = canonical
            seen_inbounds = set()
            for inbound_id, original in sorted(items, key=lambda item: inbound_order.get(item[0], 999999)):
                if inbound_id in seen_inbounds:
                    continue
                seen_inbounds.add(inbound_id)
                per_inbound = dict(canonical)
                flow = first_text(original.get("flow"), canonical.get("flow"))
                if flow:
                    per_inbound["flow"] = flow
                new_clients_by_inbound[inbound_id].append(per_inbound)

        print(f"db: {db_path}")
        print(f"inbounds: {','.join(str(i) for i in inbound_ids)}")
        print(f"groups: {len(groups)}")
        for inbound_id in inbound_ids:
            before = len(settings_by_inbound[inbound_id].get("clients", []))
            after = len(new_clients_by_inbound[inbound_id])
            print(f"inbound {inbound_id}: clients {before} -> {after}")

        if dry_run:
            print("dry-run only; no changes written")
            return

        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "DELETE FROM client_inbounds WHERE inbound_id IN (" + ",".join("?" for _ in inbound_ids) + ")",
            inbound_ids,
        )

        linked = set()
        for key, canonical in canonical_by_key.items():
            canonical_email = first_text(canonical.get("email"))
            client_id = upsert_client(conn, canonical, now_ms)
            merge_traffic(conn, canonical_email, old_emails_by_group[key])

            for inbound_id, original in groups[key]:
                link = (client_id, inbound_id)
                if link in linked:
                    continue
                linked.add(link)
                conn.execute(
                    "INSERT OR IGNORE INTO client_inbounds (client_id, inbound_id, flow_override, created_at) "
                    "VALUES (?, ?, ?, ?)",
                    (client_id, inbound_id, first_text(original.get("flow"), canonical.get("flow")), now_ms),
                )

        old_emails = sorted({email for emails in old_emails_by_group.values() for email in emails})
        canonical_emails = {first_text(c.get("email")) for c in canonical_by_key.values()}
        stale_emails = [email for email in old_emails if email and email not in canonical_emails]
        for email in stale_emails:
            row = conn.execute("SELECT id FROM clients WHERE email = ?", (email,)).fetchone()
            if not row:
                continue
            client_id = int(row[0])
            links = conn.execute("SELECT COUNT(*) FROM client_inbounds WHERE client_id = ?", (client_id,)).fetchone()[0]
            if int(links) == 0:
                conn.execute("DELETE FROM clients WHERE id = ?", (client_id,))

        for inbound_id, settings in settings_by_inbound.items():
            settings["clients"] = new_clients_by_inbound[inbound_id]
            conn.execute(
                "UPDATE inbounds SET settings = ? WHERE id = ?",
                (dump_settings(settings), inbound_id),
            )

        conn.commit()
        print(f"backup: {backup_path}")
        print("done")
    except Exception:
        if not dry_run:
            conn.rollback()
        raise
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="/etc/x-ui/x-ui.db")
    parser.add_argument("--inbounds", required=True, help="comma separated inbound ids, e.g. 5,6,7")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    migrate(args.db, parse_inbounds(args.inbounds), args.dry_run)


if __name__ == "__main__":
    main()
