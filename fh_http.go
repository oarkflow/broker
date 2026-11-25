package broker

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/fh"
)

// HTTPServerOptions are the transport limits shared by every fh application the
// broker builds. Zero values select the documented defaults.
type HTTPServerOptions struct {
	ReadTimeout       time.Duration
	ReadHeaderTimeout time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	// MaxBodyBytes is the transport-level request body ceiling. Route
	// middleware applies tighter per-request limits through fh.LimitRequestBody.
	MaxBodyBytes int64
}

// NewHTTPApp builds an fh application with the broker's transport defaults.
// The startup banner is disabled because brokerd writes structured logs.
func NewHTTPApp(o HTTPServerOptions, extra ...fh.Option) *fh.App {
	opts := []fh.Option{
		fh.WithStartupBannerDisabled(true),
		fh.WithReadTimeout(durationOr(o.ReadTimeout, 15*time.Second)),
		fh.WithReadHeaderTimeout(durationOr(o.ReadHeaderTimeout, 5*time.Second)),
		fh.WithWriteTimeout(durationOr(o.WriteTimeout, 30*time.Second)),
		fh.WithIdleTimeout(durationOr(o.IdleTimeout, 60*time.Second)),
		fh.WithErrorHandler(fhErrorHandler),
		// Route parameters end up as map keys and in persisted records, so they
		// must not alias fh's pooled request buffers.
		fh.WithSafeParams(true),
	}
	if o.MaxBodyBytes > 0 {
		opts = append(opts, fh.WithMaxRequestBodySize(int(o.MaxBodyBytes)))
	}
	return fh.New(append(opts, extra...)...)
}

func durationOr(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}

// fhErrorHandler renders errors that escape a handler (body limits, framework
// rejections) with the broker wire envelope so clients see one error format.
func fhErrorHandler(c fh.Ctx, err error) {
	var he *fh.HTTPError
	if errors.As(err, &he) && he.Status == fh.StatusPayloadTooLarge {
		_ = WriteBrokerError(c, errf(ErrMessageTooLarge, "request body too large"))
		return
	}
	if errors.Is(err, fh.ErrBodyTooLarge) {
		_ = WriteBrokerError(c, errf(ErrMessageTooLarge, "request body too large"))
		return
	}
	if errors.As(err, &he) {
		_ = httpError(c, he.Message, he.Status)
		return
	}
	_ = httpError(c, "internal server error", fh.StatusInternalServerError)
}

// ServeHTTPApp serves app on ln, wrapping it in TLS when tlsConfig is non-nil.
func ServeHTTPApp(app *fh.App, ln net.Listener, tlsConfig *tls.Config) error {
	if tlsConfig != nil {
		return app.ServeTLS(ln, tlsConfig)
	}
	return app.Serve(ln)
}

// ListenHTTPApp listens on addr and serves app until it is shut down.
func ListenHTTPApp(app *fh.App, addr string, tlsConfig *tls.Config) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return ServeHTTPApp(app, ln, tlsConfig)
}

// ShutdownHTTPApp drains app within ctx.
func ShutdownHTTPApp(ctx context.Context, app *fh.App) error {
	if app == nil {
		return nil
	}
	return app.ShutdownWithContext(ctx)
}

// ---- request helpers ----

// decodeJSON decodes the first JSON value of the request body, matching
// json.NewDecoder(r.Body).Decode semantics (trailing data is ignored and an
// empty body reports io.EOF).
func decodeJSON(c fh.Ctx, v any) error {
	return json.NewDecoder(bytes.NewReader(c.Body())).Decode(v)
}

// pathParam returns a percent-decoded route parameter. fh exposes raw path
// segments; handlers historically saw net/http's decoded URL.Path.
func pathParam(c fh.Ctx, name string) string {
	raw := strings.Clone(c.Params(name))
	if !strings.Contains(raw, "%") {
		return raw
	}
	if decoded, err := url.PathUnescape(raw); err == nil {
		return decoded
	}
	return raw
}

// pathPartition parses a non-negative partition route parameter.
func pathPartition(c fh.Ctx, name string) (int, bool) {
	partition, err := strconv.Atoi(c.Params(name))
	if err != nil || partition < 0 {
		return 0, false
	}
	return partition, true
}

// queryString returns a query parameter that is safe to retain after the
// request: fh returns query values that alias its pooled request buffer.
func queryString(c fh.Ctx, name string) string {
	return strings.Clone(c.Query(name))
}

func queryLimit(c fh.Ctx, d int) int {
	if raw := queryString(c, "limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			if n > 10000 {
				n = 10000
			}
			return n
		}
	}
	return d
}

func queryDuration(c fh.Ctx, key string, d time.Duration) time.Duration {
	raw := queryString(c, key)
	if raw == "" {
		return d
	}
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return time.Duration(n)
	}
	if v, err := time.ParseDuration(raw); err == nil {
		return v
	}
	return d
}

func preferMinimal(c fh.Ctx) bool {
	return strings.Contains(strings.ToLower(c.Get("Prefer")), "return=minimal")
}

func bearerToken(c fh.Ctx) string {
	return bearerTokenFromHeader(c.Get("Authorization"))
}

func bearerTokenFromHeader(auth string) string {
	if len(auth) < len("Bearer ") || !strings.EqualFold(auth[:len("Bearer ")], "Bearer ") {
		return ""
	}
	return strings.TrimSpace(auth[len("Bearer "):])
}

// verifiedPeerCertificate returns the verified client leaf certificate, if the
// connection is mutually authenticated.
func verifiedPeerCertificate(c fh.Ctx) *x509.Certificate {
	state, ok := fh.RequestTLSState(c)
	if !ok || len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
		return nil
	}
	return state.VerifiedChains[0][0]
}

// ---- response helpers ----

func writeBrokerJSON(c fh.Ctx, v any) error {
	return writeBrokerJSONStatus(c, fh.StatusOK, v)
}

// writeBrokerJSONStatus writes v exactly as json.Encoder would (including the
// trailing newline) so the wire format is unchanged by the fh migration.
func writeBrokerJSONStatus(c fh.Ctx, status int, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return httpError(c, err.Error(), fh.StatusInternalServerError)
	}
	b = append(b, '\n')
	c.Set("Content-Type", "application/json")
	return c.Status(status).Send(b)
}

func writeBrokerErr(c fh.Ctx, err error) error {
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, map[string]bool{"ok": true})
}

// httpError writes a plain-text error, matching net/http's http.Error output.
func httpError(c fh.Ctx, msg string, status int) error {
	c.Set("Content-Type", "text/plain; charset=utf-8")
	c.Set("X-Content-Type-Options", "nosniff")
	return c.Status(status).SendString(msg + "\n")
}

func notFound(c fh.Ctx) error { return httpError(c, "404 page not found", fh.StatusNotFound) }

func methodBroker(c fh.Ctx) error {
	return httpError(c, "method not allowed", fh.StatusMethodNotAllowed)
}

// allowOnly answers 405 for a static path that only accepts methods, shadowing
// a sibling parameter route that would otherwise capture it.
func allowOnly(methods ...string) fh.HandlerFunc {
	allow := strings.Join(methods, ", ")
	return func(c fh.Ctx) error {
		c.Set("Allow", allow)
		return methodBroker(c)
	}
}

// plainNotFound is the router's NotFound handler.
func plainNotFound(c fh.Ctx) error { return notFound(c) }

// plainMethodNotAllowed is the router's MethodNotAllowed handler.
func plainMethodNotAllowed(c fh.Ctx, allowed []string) error {
	c.Set("Allow", strings.Join(allowed, ", "))
	return methodBroker(c)
}
