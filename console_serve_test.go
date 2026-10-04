package broker

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/oarkflow/broker/console"
)

// The console must be reachable, must not be a way around auth, and must not be
// able to execute anything the browser did not already ship.
func TestConsoleIsServedFromEmbeddedAssets(t *testing.T) {
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(t.Context())

	admin := AdminServer{Broker: b, InsecureDev: true}
	srv := httptest.NewServer(admin.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/console/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("console: got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("console content type = %q", ct)
	}
	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	body := string(buf[:n])
	for _, want := range []string{"oarkflow", "app.css", "app.js"} {
		if !strings.Contains(body, want) {
			t.Fatalf("console shell missing %q", want)
		}
	}

	for _, asset := range []string{"/console/app.css", "/console/app.js"} {
		r, err := http.Get(srv.URL + asset)
		if err != nil {
			t.Fatal(err)
		}
		if r.StatusCode != http.StatusOK {
			t.Fatalf("%s: got %d", asset, r.StatusCode)
		}
		if r.ContentLength == 0 {
			t.Fatalf("%s served empty", asset)
		}
		r.Body.Close()
	}
}

// A console that permits inline or foreign script is an XSS vector, because it
// renders broker-controlled strings (message payloads, consumer ids, error text).
func TestConsoleSetsStrictCSP(t *testing.T) {
	b, _ := New(Config{})
	_ = b.Start(t.Context())
	defer b.Stop(t.Context())

	admin := AdminServer{Broker: b, InsecureDev: true}
	srv := httptest.NewServer(admin.Handler())
	defer srv.Close()

	for _, path := range []string{"/console/", "/console/app.js"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		csp := resp.Header.Get("Content-Security-Policy")
		resp.Body.Close()
		if csp == "" {
			t.Fatalf("%s: no CSP header", path)
		}
		if strings.Contains(csp, "unsafe-inline") || strings.Contains(csp, "unsafe-eval") {
			t.Fatalf("%s: CSP permits unsafe script: %s", path, csp)
		}
		if !strings.Contains(csp, "default-src 'self'") {
			t.Fatalf("%s: CSP does not pin default-src: %s", path, csp)
		}
		if !strings.Contains(csp, "object-src 'none'") {
			t.Fatalf("%s: CSP allows plugins: %s", path, csp)
		}
	}
}

// The console is a management surface. If it were mounted outside the auth
// wrapper it would hand an unauthenticated operator a view of broker state, so
// this asserts the mount point is actually behind wrap.
func TestConsoleRequiresAdminAuth(t *testing.T) {
	// The console resolves to PermissionBrokerRead, so a token with no permissions
	// is refused. Grant the read permission the console actually needs.
	acl := NewACL(AdminToken{Name: "operator", Token: "secret-token", Permissions: []string{PermissionBrokerRead}})
	b := MustNew(Config{Store: NewMemoryStore(), ACL: acl})
	if err := b.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(t.Context())

	admin := AdminServer{Broker: b, ACL: acl}
	srv := httptest.NewServer(admin.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/console/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatal("console served without a token; it must sit behind admin auth")
	}

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/console/", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	ok, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer ok.Body.Close()
	if ok.StatusCode != http.StatusOK {
		t.Fatalf("console with valid token: got %d", ok.StatusCode)
	}
}

// Reloading a client-side route must not 404 into a broken page.
func TestConsoleServesShellForUnknownSubpath(t *testing.T) {
	b, _ := New(Config{})
	_ = b.Start(t.Context())
	defer b.Stop(t.Context())

	admin := AdminServer{Broker: b, InsecureDev: true}
	srv := httptest.NewServer(admin.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/console/consumers/deep/link")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("deep link: got %d", resp.StatusCode)
	}
}

// A missing asset must be an honest 404 rather than the HTML shell, otherwise a
// typo in a script URL fails as a syntax error instead of a clear miss.
func TestConsoleMissingAssetIs404(t *testing.T) {
	b, _ := New(Config{})
	_ = b.Start(t.Context())
	defer b.Stop(t.Context())

	admin := AdminServer{Broker: b, InsecureDev: true}
	srv := httptest.NewServer(admin.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/console/does-not-exist.js")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("missing asset: got %d, want 404", resp.StatusCode)
	}
}

// Every endpoint the console calls must exist and answer 200, otherwise a view
// renders "could not load". This caught two real defects: the console called
// /queues/{name}, which has no detail route and always 404s, and it called /lag
// with no stream parameter, which always 400s.
func TestConsoleEveryEndpointItCallsResolves(t *testing.T) {
	b := MustNew(Config{Store: NewMemoryStore()})
	ctx := t.Context()
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)

	if err := b.DeclareQueue(ctx, QueueConfig{Name: "orders"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().Publish(ctx, "orders", []byte(`{"id":1}`), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().Publish(ctx, "events", []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().SubmitTask(ctx, "demo", map[string]any{"k": "v"}, WithTaskQueue("orders")); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(AdminServer{Broker: b, InsecureDev: true}.Handler())
	defer srv.Close()

	for _, p := range []string{
		"/v1/broker", "/stats", "/healthz", "/audit?limit=50",
		"/queues", "/queues/orders/messages?limit=5",
		"/topics", "/v1/topics/events?limit=5",
		"/streams", "/cluster/status", "/v1/consumers",
		"/v1/tasks?limit=50", "/dlq", "/console/",
	} {
		resp, err := http.Get(srv.URL + p)
		if err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s -> %d, console view would break", p, resp.StatusCode)
		}
	}

	resp, err := http.Get(srv.URL + "/v1/tasks?limit=50")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(raw), "demo") {
		t.Errorf("task list missing the submitted task: %s", raw)
	}
}

// TestConsoleDeclaredEndpointsExist closes the drift risk in the test above: it
// derives the endpoint list from the console's own source instead of a
// hand-maintained list, so a route renamed in app.js fails here rather than
// silently breaking a view in the browser.
func TestConsoleDeclaredEndpointsExist(t *testing.T) {
	b := MustNew(Config{Store: NewMemoryStore()})
	ctx := t.Context()
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "orders"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().Publish(ctx, "events", []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().SubmitTask(ctx, "demo", nil, WithTaskQueue("orders")); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(AdminServer{Broker: b, InsecureDev: true}.Handler())
	defer srv.Close()

	seen := map[string]bool{}
	src, err := console.AppJS()
	if err != nil {
		t.Fatal(err)
	}
	// Match api('/literal' optionally followed by a concatenation. Only literal
	// paths are asserted: a path built as '/queues/' + name + '/purge' cannot be
	// probed without inventing a resource, and a 404 for an absent resource is
	// indistinguishable from a missing route. The literal set is the part that
	// silently breaks a whole view when a route is renamed.
	re := regexp.MustCompile(`api\('([^']*)'(\s*\+)?`)
	// Params the server requires before it will answer at all.
	needsParam := map[string]bool{"/lag": true}
	var bad []string
	for _, m := range re.FindAllStringSubmatch(string(src), -1) {
		if m[2] != "" {
			continue // dynamic tail
		}
		raw := m[1]
		if raw == "" || seen[raw] {
			continue
		}
		seen[raw] = true
		path := raw
		if i := strings.IndexByte(path, '?'); i >= 0 {
			path = path[:i]
		}
		if path == "" || needsParam[path] {
			continue
		}
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			bad = append(bad, path+": "+err.Error())
			continue
		}
		code := resp.StatusCode
		resp.Body.Close()
		// 405 means the route matched and refused this method; 400 means it
		// matched and wanted a parameter. Both still prove the route exists, which
		// is the property under test. Only 404 (no route) and 5xx are failures.
		switch code {
		case http.StatusOK, http.StatusBadRequest, http.StatusMethodNotAllowed:
		default:
			bad = append(bad, fmt.Sprintf("%s -> %d", path, code))
		}
	}
	if len(bad) > 0 {
		t.Errorf("console calls literal endpoints that do not resolve to a route:\n  %s", strings.Join(bad, "\n  "))
	}
	if len(seen) < 8 {
		t.Fatalf("only %d api() calls found in console source; the extractor is probably broken", len(seen))
	}
	t.Logf("verified %d literal console endpoints", len(seen))
}

// TestConsoleResponseShapes pins the exact JSON shape the console parses. Every
// earlier console bug was a shape bug that a status-code check cannot see:
// /stats nests under "store", queues/topics/dlq are name->count maps rather than
// objects, and consumers are keyed by "consumer_id" with no "queue" field. Each of
// those rendered a plausible-looking table of dashes.
func TestConsoleResponseShapes(t *testing.T) {
	b := MustNew(Config{Store: NewMemoryStore()})
	ctx := t.Context()
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "orders"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().Publish(ctx, "orders", []byte("m"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().Publish(ctx, "events", []byte("e"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().SubmitTask(ctx, "demo", nil, WithTaskQueue("orders")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.RegisterRemoteConsumer(ctx, ConsumerRegistration{ConsumerID: "c1", Queues: []string{"orders"}, Group: "g1"}); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(AdminServer{Broker: b, InsecureDev: true}.Handler())
	defer srv.Close()

	get := func(path string) map[string]any {
		t.Helper()
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var m map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		return m
	}

	// /stats: everything the overview reads lives under "store".
	st := get("/stats")
	ss, ok := st["store"].(map[string]any)
	if !ok {
		t.Fatalf("/stats has no store object: %v", st)
	}
	for _, k := range []string{"queues", "topics", "streams", "dlq"} {
		if _, ok := ss[k]; !ok {
			t.Errorf("/stats store missing %q: %v", k, ss)
		}
	}
	if _, ok := st["broker_state"]; !ok {
		t.Errorf("/stats missing broker_state: %v", st)
	}
	// queues must be name -> number, not name -> object.
	qs, ok := ss["queues"].(map[string]any)
	if !ok {
		t.Fatalf("/stats store.queues is not an object: %T", ss["queues"])
	}
	if _, ok := qs["orders"].(float64); !ok {
		t.Errorf("store.queues[orders] = %#v, want a number the console can render", qs["orders"])
	}

	// Consumers: consumer_id, and no queue field.
	resp, err := http.Get(srv.URL + "/v1/consumers")
	if err != nil {
		t.Fatal(err)
	}
	var cs []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&cs); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(cs) == 0 {
		t.Fatal("no consumers registered")
	}
	c := cs[0]
	if _, ok := c["consumer_id"].(string); !ok {
		t.Errorf("consumer missing consumer_id: %v", c)
	}
	if _, ok := c["state"]; !ok {
		t.Errorf("consumer missing state: %v", c)
	}
	if _, bad := c["queue"]; bad {
		t.Errorf("consumer unexpectedly exposes queue; the console must not render it: %v", c)
	}

	// Tasks and topics: id-keyed rows.
	var tasks []map[string]any
	resp2, err := http.Get(srv.URL + "/v1/tasks?limit=10")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewDecoder(resp2.Body).Decode(&tasks); err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if len(tasks) == 0 || tasks[0]["id"] == nil {
		t.Errorf("task rows must be keyed by id: %v", tasks)
	}
}
