package broker

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/oarkflow/fh"
)

// HTTPClientConfig configures the broker's outbound fh HTTP clients.
type HTTPClientConfig struct {
	// Timeout bounds a whole exchange, including reading the response. Zero
	// selects 30s; a negative value disables the deadline (for streams), in
	// which case callers bound each request through its context.
	Timeout   time.Duration
	TLSConfig *tls.Config
	// AllowInsecureTLS must be set for a TLSConfig with InsecureSkipVerify.
	AllowInsecureTLS    bool
	MaxIdleConns        int
	MaxIdleConnsPerHost int
	MaxConnsPerHost     int
	// DisableRedirects refuses to follow any redirect; the request fails
	// instead. Peer forwarding uses it so a redirect cannot move an
	// authenticated request to another host.
	DisableRedirects bool
}

// noDeadline stands in for "no client deadline": fh treats a zero timeout as
// its 30s default.
const noDeadline = 100 * 365 * 24 * time.Hour

// NewHTTPClient returns an fh client with broker semantics:
//
//   - no automatic retries: every caller already owns its failover and retry
//     policy, and a transparent retry of a publish could duplicate it;
//   - the response-header wait is bounded only by Timeout, so long-polls that
//     legitimately hold the response are not cut off early;
//   - no implicit response-size cap: callers bound bodies they read;
//   - no per-request failure logging, since failures are returned as errors.
func NewHTTPClient(cfg HTTPClientConfig) *fh.Client {
	timeout := cfg.Timeout
	switch {
	case timeout == 0:
		timeout = 30 * time.Second
	case timeout < 0:
		timeout = noDeadline
	}
	redirect := fh.RedirectPolicy{Max: 10, StripSensitiveHeaders: true}
	if cfg.DisableRedirects {
		// fh counts prior hops: with Max 1 the first redirect already exceeds
		// the limit. (Max <= 0 selects fh's default of 10.)
		redirect = fh.RedirectPolicy{Max: 1, SameHostOnly: true, HTTPSOnly: true}
	}
	return fh.NewClient(fh.ClientConfig{
		UserAgent:             "oarkflow-broker",
		Timeout:               timeout,
		ResponseHeaderTimeout: timeout,
		TLSConfig:             cfg.TLSConfig,
		AllowInsecureTLS:      cfg.AllowInsecureTLS,
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		MaxConnsPerHost:       cfg.MaxConnsPerHost,
		Retry:                 fh.NoRetry(),
		Redirect:              redirect,
		ResponseBodyLimit:     -1,
		Logger:                fh.NewNoopLogger(),
	})
}

// SendHTTP issues method url through client with an optional body and request
// headers. The caller must Close the returned response.
func SendHTTP(ctx context.Context, client *fh.Client, method, url string, body []byte, headers map[string]string) (*fh.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	req := client.R().Headers(headers)
	if body != nil {
		req = req.Bytes(body)
	}
	return req.Send(ctx, method, url)
}

// jsonRequestHeaders is the header set for a JSON request body.
var jsonRequestHeaders = map[string]string{"Content-Type": "application/json"}
