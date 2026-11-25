package broker

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/textproto"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

// Test HTTP toolkit built on fh: servers are fh applications on loopback
// listeners and clients are fh clients. The request and response types mirror
// the field names tests historically used (StatusCode, Body, Header).

// testHeader is a canonicalized multi-value header map.
type testHeader map[string][]string

func (h testHeader) Get(key string) string {
	if v := h[textproto.CanonicalMIMEHeaderKey(key)]; len(v) > 0 {
		return v[0]
	}
	return ""
}

func (h testHeader) Set(key, value string) {
	h[textproto.CanonicalMIMEHeaderKey(key)] = []string{value}
}

func (h testHeader) Values(key string) []string { return h[textproto.CanonicalMIMEHeaderKey(key)] }

// testRequest is an outbound test request.
type testRequest struct {
	Method string
	URL    *url.URL
	Header testHeader
	Body   []byte
}

// newTestRequest builds a request; target may be a path (resolved against the
// server it is sent to) or an absolute URL.
func newTestRequest(method, target string, body io.Reader) *testRequest {
	req, err := newHTTPRequest(method, target, body)
	if err != nil {
		panic(err)
	}
	return req
}

func newHTTPRequest(method, target string, body io.Reader) (*testRequest, error) {
	if len(target) > 0 && target[0] == '/' {
		target = "http://example.com" + target
	}
	u, err := url.Parse(target)
	if err != nil {
		return nil, err
	}
	req := &testRequest{Method: method, URL: u, Header: testHeader{}}
	if body != nil {
		if req.Body, err = io.ReadAll(body); err != nil {
			return nil, err
		}
	}
	return req, nil
}

// testResponse is a received test response. The caller closes Body.
type testResponse struct {
	StatusCode    int
	Status        string
	ContentLength int64
	Header        testHeader
	Body          io.ReadCloser
}

// testClient sends test requests through an fh client.
type testClient struct{ c *fh.Client }

func newTestClient(timeout time.Duration) *testClient {
	return &testClient{c: NewHTTPClient(HTTPClientConfig{Timeout: timeout})}
}

// testHTTP is the shared test client.
var testHTTP = newTestClient(30 * time.Second)

func (c *testClient) Do(req *testRequest) (*testResponse, error) {
	headers := make(map[string]string, len(req.Header))
	for k, v := range req.Header {
		if len(v) > 0 {
			headers[k] = v[0]
		}
	}
	body := req.Body
	if body == nil && req.Method != fh.MethodGET && req.Method != fh.MethodHEAD {
		body = []byte{}
	}
	resp, err := SendHTTP(context.Background(), c.c, req.Method, req.URL.String(), body, headers)
	if err != nil {
		return nil, err
	}
	return &testResponse{StatusCode: resp.StatusCode(), Status: resp.Raw.Status, ContentLength: resp.Raw.ContentLength, Header: testHeader(resp.Header()), Body: resp.Reader()}, nil
}

func (c *testClient) Get(target string) (*testResponse, error) {
	req, err := newHTTPRequest(fh.MethodGET, target, nil)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

func (c *testClient) Post(target, contentType string, body io.Reader) (*testResponse, error) {
	req, err := newHTTPRequest(fh.MethodPOST, target, body)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return c.Do(req)
}

// testServer is a running fh application on an ephemeral loopback port.
type testServer struct {
	URL string
	app *fh.App
}

func newTestServer(t testing.TB, app *fh.App) *testServer {
	t.Helper()
	s := startTestApp(t, app)
	t.Cleanup(s.Close)
	return s
}

func startTestApp(t testing.TB, app *fh.App) *testServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = app.Serve(ln) }()
	return &testServer{URL: "http://" + ln.Addr().String(), app: app}
}

func (s *testServer) Client() *testClient { return testHTTP }

func (s *testServer) Close() { _ = s.app.ShutdownWithTimeout(2 * time.Second) }

// newCatchAllServer serves every request with h, for fake upstream receivers
// (webhooks, OTLP collectors) that outbound clients talk to.
func newCatchAllServer(t testing.TB, h fh.HandlerFunc) *testServer {
	t.Helper()
	return newTestServer(t, fh.New(fh.WithStartupBannerDisabled(true), fh.WithNotFoundHandler(fh.NotFoundHandler(h))))
}

// testRecorder captures one exchange with an fh application.
type testRecorder struct {
	Code      int
	Body      *bytes.Buffer
	HeaderMap testHeader
}

func (r *testRecorder) Header() testHeader { return r.HeaderMap }

var (
	servedAppsMu sync.Mutex
	servedApps   = map[*fh.App]*testServer{}
)

// serveTest sends req to app and records the response. Each app is served
// once on a loopback listener for the rest of the process: an fh App cannot be
// served again after shutdown, and subtests often share one app.
func serveTest(t testing.TB, app *fh.App, req *testRequest) *testRecorder {
	t.Helper()
	servedAppsMu.Lock()
	srv, ok := servedApps[app]
	if !ok {
		srv = startTestApp(t, app)
		servedApps[app] = srv
	}
	servedAppsMu.Unlock()
	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	req.URL.Scheme, req.URL.Host = target.Scheme, target.Host
	resp, err := testHTTP.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return &testRecorder{Code: resp.StatusCode, Body: bytes.NewBuffer(body), HeaderMap: resp.Header}
}
