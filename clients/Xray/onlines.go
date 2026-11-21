package xray

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// OnlineClients returns the emails of clients the panel currently sees as
// connected, the same list the 3x-ui dashboard shows as "online".
// Older panels serve it under /inbounds, newer ones under /clients.
func (x *XRayClient) OnlineClients(ctx context.Context) ([]string, error) {
	var lastErr error
	for _, path := range []string{"/panel/api/inbounds/onlines", "/panel/api/clients/onlines"} {
		status, body, err := x.doAPIRequestContext(ctx, "POST", x.serverURL+path, nil, map[string]string{"Accept": "application/json"}, true)
		if err != nil {
			return nil, err
		}
		if status == http.StatusNotFound {
			lastErr = fmt.Errorf("%s: not found", path)
			continue
		}
		if status < 200 || status >= 300 {
			return nil, fmt.Errorf("unexpected status=%d body=%s", status, responseSnippet(body))
		}
		var raw struct {
			Success bool     `json:"success"`
			Msg     string   `json:"msg"`
			Obj     []string `json:"obj"`
		}
		if err := json.Unmarshal(body, &raw); err != nil {
			return nil, fmt.Errorf("%w; body=%s", err, responseSnippet(body))
		}
		if !raw.Success {
			return nil, fmt.Errorf("API returned success=false: %s", raw.Msg)
		}
		return raw.Obj, nil
	}
	return nil, lastErr
}
