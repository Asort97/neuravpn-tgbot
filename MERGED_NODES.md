# Additional merged Xray nodes

The legacy `MERGED_XRAY_*` variables remain supported and describe the current
white-list node. Do not remove or rename them during migration.

To add any number of additional 3x-ui panels, set one single-line JSON array
in the bot service environment:

```dotenv
MERGED_XRAY_NODES_JSON=[{"name":"de","panel_url":"https://de-panel.example.com/secret/","api_token":"REPLACE_WITH_API_TOKEN","inbound_ids":[3,4],"subscription_order":10,"server_address":"de.example.com","server_port":443}]
```

Multiple nodes are separate objects in the same array:

```dotenv
MERGED_XRAY_NODES_JSON=[{"name":"de","panel_url":"https://de-panel.example.com/secret/","api_token":"TOKEN_DE","inbound_ids":[3,4],"server_address":"de.example.com","server_port":443},{"name":"ru-backup","panel_url":"https://ru-panel.example.com/secret/","username":"panel-user","password":"PANEL_PASSWORD","inbound_ids":[7],"server_address":"ru-backup.example.com","server_port":443}]
```

Required fields for every new node:

- `panel_url`, or all three API fields: `host`, `port`, `web_base_path`;
- either `api_token`, or both `username` and `password`;
- `inbound_ids`: exact inbound IDs to bind on this panel;
- `server_address`: public host or IP users must connect to.

`server_port` is optional. It is only a fallback; for every Reality inbound the
bot reads that inbound's actual listening port from 3x-ui and writes it into
the generated VLESS link. For example, one node may use `server_port:443` as
the fallback while its links correctly use ports `443`, `10001`, and `10002`.

## Subscription order

Use `subscription_order` to order complete nodes in the merged subscription:
the lower non-zero number appears first. Nodes without this field keep their
old order after explicitly ordered nodes. The order inside one node is exactly
the order in `inbound_ids`.

For the legacy white-list node, set `MERGED_XRAY_SUBSCRIPTION_ORDER`. For
example, set the German node to `"subscription_order":10` and legacy white
lists to `MERGED_XRAY_SUBSCRIPTION_ORDER=20` to show Germany first.

`server_name`, `public_key`, `short_id`, `spider_x`, and `fingerprint` are
optional. When omitted, the bot reads Reality parameters from the first listed
inbound on the panel.

After restarting `vpnbot`, the bot logs every connected node. New payments and
renewals synchronize a user across all nodes. When an existing user refreshes
their merged subscription, a missing client record on a new node is created and
the new links are appended to the same subscription. A failing extra node does
not remove working links from the other nodes.
