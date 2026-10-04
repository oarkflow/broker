// Package console serves an embedded operator web console for a broker.
//
// The console is a static single-page app. It holds no broker data of its own:
// every value it renders is fetched at runtime from the same admin and /v1
// endpoints an operator would use with curl, carrying the same bearer token and
// subject to the same ACL. That keeps a single authorisation path instead of a
// privileged back door that happens to live in the UI.
//
// Assets are compiled into the binary, so the console needs no build step, no
// Node toolchain and no files on disk. That matters for the distroless images
// and for reproducible_build.sh.
package console

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed assets
var assets embed.FS

// Handler returns the console handler. Mount it behind the admin auth wrapper so
// the console cannot become an unauthenticated window onto broker state:
//
//	mux.Handle("/console/", admin.Wrap(console.Handler()))
//
// The handler only ever serves its own embedded bytes, so it reads nothing from
// the broker and is safe to mount without auth if a deployment prefers to put it
// behind a separate authenticating proxy.
func Handler() http.Handler {
	sub, err := fs.Sub(assets, "assets")
	if err != nil {
		// Unreachable: the assets directory is embedded at build time and its
		// presence is covered by TestAssetsPresent.
		panic("console: embedded assets missing: " + err.Error())
	}
	files := http.FileServer(http.FS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The console loads only same-origin scripts, styles and its own fetch
		// calls. No inline script and no external origin is permitted, which
		// means a payload rendered into the DOM cannot execute.
		w.Header().Set("Content-Security-Policy", strings.Join([]string{
			"default-src 'self'",
			"script-src 'self'",
			"style-src 'self'",
			"img-src 'self' data:",
			"connect-src 'self'",
			"font-src 'self'",
			"object-src 'none'",
			"base-uri 'none'",
			"form-action 'none'",
			"frame-ancestors 'none'",
		}, "; "))
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")

		switch r.URL.Path {
		case "/", "/index.html":
			w.Header().Set("Cache-Control", "no-store")
			serveIndex(w, r, sub)
			return
		}
		if !looksLikeAsset(r.URL.Path) {
			// Client-side route: fall back to the shell so a reload or a deep link
			// survives instead of 404ing into a dead page.
			w.Header().Set("Cache-Control", "no-store")
			serveIndex(w, r, sub)
			return
		}
		// Assets are immutable for the life of the binary.
		w.Header().Set("Cache-Control", "public, max-age=3600")
		files.ServeHTTP(w, r)
	})
}

// looksLikeAsset reports whether a path names a file rather than an app route.
// A missing asset must stay a 404, otherwise a typo in a script URL surfaces as a
// JavaScript syntax error instead of an obvious miss.
func looksLikeAsset(p string) bool {
	base := p[strings.LastIndex(p, "/")+1:]
	if !strings.Contains(base, ".") {
		return false
	}
	switch {
	case strings.HasSuffix(base, ".js"), strings.HasSuffix(base, ".mjs"),
		strings.HasSuffix(base, ".css"), strings.HasSuffix(base, ".map"),
		strings.HasSuffix(base, ".json"), strings.HasSuffix(base, ".png"),
		strings.HasSuffix(base, ".svg"), strings.HasSuffix(base, ".ico"),
		strings.HasSuffix(base, ".woff2"), strings.HasSuffix(base, ".txt"):
		return true
	}
	return false
}

func serveIndex(w http.ResponseWriter, _ *http.Request, sub fs.FS) {
	body, err := fs.ReadFile(sub, "index.html")
	if err != nil {
		http.Error(w, "console unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(body)
}

// AppJS returns the console's script source. It exists so tests can assert that
// every endpoint the console calls actually exists, which is otherwise only
// discoverable by loading the page in a browser.
func AppJS() ([]byte, error) {
	return assets.ReadFile("assets/app.js")
}
