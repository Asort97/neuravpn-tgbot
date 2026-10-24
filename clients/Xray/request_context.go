package xray

import (
	"context"
	"time"
)

// WithContext scopes a sequence of panel calls without changing the shared
// client's timeout or copying its mutex. Transport and cookie jar remain shared;
// authentication refresh state belongs to the scoped client.
func (x *XRayClient) WithContext(ctx context.Context) (*XRayClient, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for !x.authMu.TryLock() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	defer x.authMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &XRayClient{
		username: x.username, password: x.password,
		host: x.host, port: x.port, webBasePath: x.webBasePath,
		serverURL: x.serverURL, apiToken: x.apiToken,
		httpClient: x.httpClient, csrfToken: x.csrfToken,
		requestContext: ctx,
	}, nil
}

func (x *XRayClient) defaultRequestContext() context.Context {
	if x.requestContext != nil {
		return x.requestContext
	}
	return context.Background()
}
