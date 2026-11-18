# Multi-transport merged node

Add the panel to the existing `MERGED_XRAY_NODES_JSON` array. Keep existing
entries. Set `inbound_ids` to the enabled inbounds, starting with the VLESS
XHTTP/REALITY fallback, and `server_address` to their advertised address.
Set `balanced_remark` to the customer-facing name and `balanced_json_url`
to the panel's private JSON subscription prefix (without an account ID).
The JSON listener must be reached over loopback, for example by forwarding the
panel and subscription ports through an authenticated SSH tunnel. Never expose
either management listener just to integrate a node.

The bot provisions and revokes accounts through the normal merged-node path.
Use a separate panel for mixed-protocol accounts. Enable Vision for the Vision
inbound only; disable flow on the other inbounds. Configure the JSON subscription
to return one document per attached inbound. The credentials for the
password-based transports are per account, not shared.

Clients negotiating full Xray JSON receive one named profile. Only the proxy
outbound of each native document is imported; the client does not inherit the
panel's routing, DNS, or direct fallback. Checks use the device's network at a
two-minute interval. If the JSON export cannot be verified, the ordinary
authorized VLESS fallback stays in the subscription. URI-only clients also get
that fallback. A failing merged node does not replace the primary subscription.

To withdraw the node, remove only its entry and restart the bot. To stop its
previously issued credentials as well, disable the inbounds on the node.
