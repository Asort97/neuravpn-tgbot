# Global Sales

Admin-only commands in a private bot chat:

```text
/create_sales BLACKFRIDAY 50% 2026-11-30 23:59
/sales
/stop_sales BLACKFRIDAY
```

The end time is Moscow time (UTC+03:00), exclusive. Percent is 1..99;
names use ASCII letters, digits, `_` or `-`, up to 48 characters.
Creation activates immediately. A second active sale is rejected, including
concurrent admin commands. Stop the existing sale explicitly before replacing it.
Commands create/stop an admin chat audit entry, not a customer broadcast.

## Pricing

- All users automatically qualify; no promo activation is required.
- Manual subscription payments, recurring card charges and additional-device
  purchases use the current sale. Any still-enabled legacy traffic checkout
  also applies it; this does not re-enable discontinued traffic packs.
- A manual subscription checkout compares the existing personal promo total
  (promo applies only to the base plan) with the sale total (plan plus devices).
  The cheaper total wins; discounts do not stack.
- Stars compare the existing Stars price with the sale price, rounded to whole
  Stars. At pre-checkout the current price and device count are revalidated.
  If they changed, the user must create a new invoice.
- Minimum new sale payment is 1 RUB / 1 Star. Free access is not granted by sales.
- Expiry is checked by PostgreSQL on reads, with no cron or per-user updates.
  Restarts do not reactivate expired or stopped sales.
- Existing YooKassa invoices retain their original price. Device orders persist
  the price before contacting YooKassa and reuse it on retries, verification and
  accounting. Existing orders migrate with their previous 50 RUB price.
- Site/mini-app quotes are signed and expire no later than the chosen promotion.
  A changed/expired quote returns 409, never a silent full-price payment.
- Payment creation fails closed if the sale lookup fails. Autopay skips the
  charge instead of silently using the undiscounted price.

## Deployment

Bot changes are in this repository; matching website changes are in the
`codex/global-sales` worktree at
`C:/Users/Asort/.codex/worktrees/neuravpn-site-sales`.

Deploy both before using `/create_sales`. Both initialize the shared
`global_sale` table; bot additionally migrates `device_slot_orders.amount`.
No active sale is inserted by the migration. No service restart, production
schema change or sale activation is performed by local tests.

Back up PostgreSQL and both deployed revisions before rollout. After accepting
discounted device orders, do not roll back to code that validates every slot
payment against 50 RUB: stop the sale and retain the new verification code
until those orders are reconciled.

Tests: `go test ./...`, `go vet ./...` in both repositories and
`node --test tests/*.test.cjs` in the site repository. Database integration tests
require a separate database named `neuravpn_auth_test*` via
`AUTH_TEST_DATABASE_DSN`; never point tests at the production database.
