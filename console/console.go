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
	"mime"
	"path"
	"strings"

	"github.com/oarkflow/fh"
)

//go:embed assets
var assets embed.FS

var consoleCSP = strings.Join([]string{
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
}, "; ")

// Handler returns the console's fh handler. Register it on a wildcard route
// behind the admin auth middleware so the console cannot become an
// unauthenticated window onto broker state:
//
//	app.Get("/console/*path", authorize, console.Handler("path"))
//
// param names the wildcard route parameter holding the path below the mount
// point. The handler only ever serves its own embedded bytes, so it reads
// nothing from the broker and is safe to mount without auth if a deployment
// prefers to put it behind a separate authenticating proxy.
func Handler(param string) fh.HandlerFunc {
	sub, err := fs.Sub(assets, "assets")
	if err != nil {
		// Unreachable: the assets directory is embedded at build time and its
		// presence is covered by TestAssetsPresent.
		panic("console: embedded assets missing: " + err.Error())
	}
	return func(c fh.Ctx) error {
		// The console loads only same-origin scripts, styles and its own fetch
		// calls. No inline script and no external origin is permitted, which
		// means a payload rendered into the DOM cannot execute.
		c.Set("Content-Security-Policy", consoleCSP)
		c.Set("X-Content-Type-Options", "nosniff")
		c.Set("Referrer-Policy", "no-referrer")

		p := "/" + strings.TrimPrefix(c.Params(param), "/")
		if p == "/" || p == "/index.html" || !looksLikeAsset(p) {
			// Client-side routes fall back to the shell so a reload or a deep
			// link survives instead of 404ing into a dead page.
			c.Set("Cache-Control", "no-store")
			return serveIndex(c, sub)
		}
		name := strings.TrimPrefix(path.Clean(p), "/")
		body, err := fs.ReadFile(sub, name)
		if err != nil {
			c.Set("Content-Type", "text/plain; charset=utf-8")
			return c.Status(fh.StatusNotFound).SendString("404 page not found\n")
		}
		// Assets are immutable for the life of the binary.
		c.Set("Cache-Control", "public, max-age=3600")
		if ct := mime.TypeByExtension(path.Ext(name)); ct != "" {
			c.Set("Content-Type", ct)
		}
		return c.Send(body)
	}
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

func serveIndex(c fh.Ctx, sub fs.FS) error {
	body, err := fs.ReadFile(sub, "index.html")
	if err != nil {
		c.Set("Content-Type", "text/plain; charset=utf-8")
		return c.Status(fh.StatusInternalServerError).SendString("console unavailable\n")
	}
	c.Set("Content-Type", "text/html; charset=utf-8")
	return c.Send(body)
}

// AppJS returns the console's script source. It exists so tests can assert that
// every endpoint the console calls actually exists, which is otherwise only
// discoverable by loading the page in a browser.
func AppJS() ([]byte, error) {
	return assets.ReadFile("assets/app.js")
}
