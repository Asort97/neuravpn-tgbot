# Deleted devices

Deletion from the bot or cabinet atomically persists a delivery ban in
`device_revocations` and queues native HWID cleanup for every configured panel.
Offline panels retry independently. Queue completion never removes the ban.
Existing historical deletion jobs are not converted into bans by migration.
Subscription days, account IDs, payment data, slots and tunnel keys are unchanged.

## Delivery

The custom `/merged-sub/` endpoint checks the ban before limits, panel requests
and cache lookup, then checks again before writing a cached or built response.
The check applies to unlimited accounts too. A database failure returns 503 and
does not release cached keys. A revoked device receives HTTP 200, `no-store`,
and exactly one base64-encoded VLESS placeholder, never appended merged configs.
The placeholder uses `192.0.2.1:1` and a nonworking UUID. Its exact label is:

> устройство было удалено. если хотите добавить это устройство, то разблокируйте его в списках удаленных

Identity matches 3x-ui: trim `X-HWID`, SHA-256 (case-sensitive), first 12 hex
digits for the panel fingerprint. Full-hash panel fingerprints also match.
Raw HWIDs are not persisted. Owners cannot see or unblock another user's records.

The bot's Deleted devices screen and the cabinet's Deleted view read only the
local database. Unblocking cancels pending native cleanup and invalidates the
subscription/device caches. Old unblock buttons cannot undo a newer deletion.
The single bot process serializes worker cleanup with bot/web device actions
using the existing per-user DevicesMu. Do not run multiple bot workers against
the same queue without adding a cross-process lock for panel cleanup/unblocking.

## Limits

This is delivery revocation, not remote deletion of an app profile or a tunnel
credential. An already-saved key can still work, and client behavior on refresh
varies. A client can change/spoof its HWID; this is not hardware attestation.
Clients without HWID retain the existing limit policy: enabled limits reject
unsupported clients, while unlimited accounts still allow them. A ban cannot
identify a request that omits or changes its HWID. Direct native subscription
URLs and deleting records directly in 3x-ui bypass the custom delivery gate.

Unblocking does not reserve a slot or recreate a native registration. The user
must refresh the subscription and fit the current device limit.

## Rollout and verification

Back up PostgreSQL. Deploy the bot first (startup creates the new table without
backfill), then the site API and assets. No bulk enrollment command is needed.
The new site routes are authenticated GET `/api/devices/deleted` and POST
`/api/devices/restore` with a user-scoped opaque generation ID. The signed
loopback bridge uses actions `deleted` and `restore`.

Tests:

- Bot: `go test ./...`; PostgreSQL integration tests use only a database named
  `neuravpn_auth_test*`, supplied through `AUTH_TEST_DATABASE_DSN`.
- Site: `go test ./...`, `node --test tests/*.test.cjs`,
  `python tests/device-revocations-browser.py` (Python Playwright and Chrome).
- Use a synthetic account to delete, refresh, restart, refresh again and unblock.
  Verify one placeholder and no merged links, then normal delivery after unblock.

Rolling back to an older bot disables enforcement even though database records
remain. Do not drop `device_revocations` during a rollback.
