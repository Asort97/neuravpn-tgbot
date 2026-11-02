# Device slots

Device limits are opt-in per account. Schema migrations do not enable limits.
The initial production pilot is Telegram user `623290294` only. Do not run a
bulk UPDATE or set panel defaults during this pilot.

## Behaviour

- Five HWID registrations are included; extra slots cost 50 RUB each.
- A slot purchase adds one place, not subscription days. It lasts with the
  current VPN access and its quantity is retained for subsequent renewals.
- Every renewal includes 50 RUB per retained extra slot, regardless of the
  selected plan duration. Promocodes discount the base plan only.
- Slot changes disable autopay. Re-enabling requires confirming the current
  price; stale confirmations and payment quotes are rejected.
- Removing a paid slot requires device count to fit the reduced limit and
  confirmation. No refund for the current period is performed.
- Disabled rollout accounts remain unlimited and are not offered slot payments.

The primary 3x-ui panel owns the HWID registry for the complete merged URL.
Enabled accounts are checked before serving any cache entry. Native panel
denials become HTTP 200 base64 subscriptions with a documentation-only IP,
port 1, unrelated UUID, and `Достигнут лимит устройств` title. Missing HWID
has a separate explanatory placeholder. Infrastructure failures return 503,
not an incorrect limit message. Known HWIDs can refresh at full capacity.
Merged server links are never appended to a denied primary response.

This controls subscription retrieval, not active Xray connections. Existing
copied configurations and shared/spoofed HWIDs are not revoked by this feature.
Deleting through the bot or cabinet also records a persistent delivery ban for
that HWID. It can register again after explicit unblocking in Deleted devices,
subject to the current limit. Direct deletion in 3x-ui does not record this ban.
See [Device revocations](device-revocations.md). Per-device tunnel credentials
would be a separate feature.

## Operations

### Permanent free slots

An admin can send `/addslots 123456789 3` in a private chat with the bot to add
three free slots permanently, or `/addslots 123456789,987654321 3` for a batch.
Up to 20 existing Telegram IDs are accepted. A single grant and the cumulative
free bonus are limited to 95 slots; paid extras remain separately capped at 95.
The total capacity is `5 + bonus_slots + extra_slots`. Only `extra_slots` is billed.
The command does not change days, payment data, autopay or the user's limit flag.
It also stores bonuses for currently unlimited accounts, for future enrollment.

Each Telegram message ID is applied once, with an audit record in
`device_bonus_grants`. Sending a NEW command intentionally adds slots again.
Batches are transactional: unknown IDs or exceeding the cap roll back all IDs.
The panel sync worker is woken after a grant and retries offline panels. Both
bot and cabinet display the bonus separately. Global rollout preserves bonuses.
Deploy the bot/schema before the site. Rolling back to older code that ignores
bonuses can lower native limits; do not roll back after issuing real grants
without accounting for the increased limits.

Back up the primary panel database before changing limits. A smaller native
limit can remove older registrations. Save the previous client limit as well.
After deploying both bot and site, enable only the selected pilot account:

```sql
INSERT INTO device_plans(user_id, enabled, extra_slots)
VALUES ('623290294', TRUE, 0);
```

The worker checks up to 50 pending accounts each minute, prioritizing quantity
changes. Successfully synchronized accounts are rechecked no sooner than one
hour later. The first subscription fetch also initializes a missing native
limit before releasing any configurations. Other client fields are preserved.
A payment is checked through the bot shop's YooKassa API,
then applied transactionally exactly once. Pending saved payment IDs are
reconciled after restarts. No background job charges for slots automatically.

## Global activation

Deployment alone does not activate the policy. On the bot host, preview first:

```bash
sudo python3 /srv/vpnbot/deploy/enable_device_limits.py
```

At the announced activation time, run once:

```bash
sudo python3 /srv/vpnbot/deploy/enable_device_limits.py --apply
```

The command backs up PostgreSQL and the primary panel under a private
`/root/device-rollout-*` directory, applies only `limitHwid` through the panel's
bulk API, and keeps the most recent `last_seen` registrations (descending ID
breaks ties). Paid and free quantities are preserved: the limit is 5 + bonus + extras.
It also trims disabled subscriptions' excess HWIDs with the scoped delete API.
Ambiguous/shared subscription ownership or a concurrent paid-quantity change
stops the operation rather than overwriting data. Earlier panel batches may
already have changed; review the backup and rerun preview before retrying.

Only after panel verification does one PostgreSQL transaction enable current
accounts and `device_limit_policy`. Its insert trigger enrolls future accounts,
including those created by the site. No bot/site restart is required at
activation. Merged nodes share the primary gate, not separate five-device quotas.
This does not revoke already saved tunnel credentials or disconnect sessions.

To stop future enrollment, set `device_limit_policy.enabled=FALSE`; existing
plans and native limits are separate and are not undone by that flag.

Check `device_plans`, `device_slot_orders`, and `[devices]` log lines. Never print
subscription tokens, raw HWIDs, payment credentials, or complete panel records.
To pause a rollout, set `enabled=FALSE` for the affected account and separately
restore its previous native panel limit. Disabling the database flag alone does
not clear a limit already configured in 3x-ui. Do not restore a whole panel DB
over customer changes; use the backup to restore only the affected record.

Tests: `go test . ./clients/...`. PostgreSQL concurrency tests require a separate
database whose name starts with `neuravpn_auth_test`; they refuse production DBs.
CLI unit tests: `python -m unittest discover -s deploy -p 'test_*.py'`.
