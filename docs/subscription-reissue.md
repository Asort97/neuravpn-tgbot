# Subscription reissue

Reissue is available to all authenticated account owners in the bot's Devices
screen and in the cabinet/Mini App. Viewing the button or confirmation dialog
does not rotate anything. A successful confirmation rotates only that account.

The primary subscription must exist before enqueueing a rotation. Generation
checks make repeated confirmations idempotent. Requests for another Telegram
account cannot override the authenticated owner; bot callbacks are private-chat
only. Locks are scoped to account and panel, not shared by all customers.

Old merged URLs are invalidated when the generation changes. Pending rotations
do not publish new links; once the primary has been verified, the new link is
available. Offline merged panels retry independently and are excluded until
their new credentials are ready. Existing credentials on an offline panel may
remain valid until that panel is reachable and rotation finishes.

Account identity, days, paid/free device slots, autopay data, quotas and inbound
attachments are preserved. This is not a bulk reset and does not change device
limit policy. Users must add the new subscription link on their devices.

Deploy site support before enabling the bot worker/UI for all users. The site
and bot must both read subscription_reissues for every account. Do not roll back
to pilot-only code after a non-pilot rotation: that could revive legacy URLs.
Take a database backup before rollout. Tests cover token/cache isolation,
explicit confirmation, missing primary clients, queued merged nodes and stored
entitlements. No real customer rotation is needed to enable the feature.
