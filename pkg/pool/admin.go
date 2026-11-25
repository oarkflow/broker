package pool

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"log"
	"time"

	"github.com/oarkflow/fh"
)

type AdminServer struct {
	Pool  *Pool
	Token string
}

// App builds the pool admin application.
func (a AdminServer) App() *fh.App {
	app := fh.New(
		fh.WithStartupBannerDisabled(true),
		fh.WithReadHeaderTimeout(5*time.Second),
		fh.WithNotFoundHandler(func(c fh.Ctx) error { return httpError(c, "404 page not found", fh.StatusNotFound) }),
		fh.WithMethodNotAllowedHandler(func(c fh.Ctx, _ []string) error { return method(c) }),
	)
	app.Use(a.auth)
	app.Get("/healthz", func(c fh.Ctx) error { return c.SendString("ok\n") })
	app.Get("/readyz", a.ready)
	app.Get("/stats", a.stats)
	app.Get("/metrics", a.prometheus)
	app.Post("/pause", a.pause)
	app.Post("/resume", a.resume)
	app.Post("/terminate", a.terminate)
	app.Get("/jobs", a.jobs)
	app.Get("/jobs/:id", a.job)
	app.Post("/jobs/:id/cancel", a.cancelJob)
	app.Get("/dlq", a.dlq)
	app.Post("/dlq/replay", a.replayDLQ)
	app.Post("/dlq/purge", a.purgeDLQ)
	return app
}

// ListenAndServe serves the admin application on addr in the background.
func (a AdminServer) ListenAndServe(addr string) *fh.App {
	app := a.App()
	go func() {
		if err := app.Listen(addr); err != nil {
			log.Printf("[pool] admin server stopped: %v", err)
		}
	}()
	return app
}

func (a AdminServer) Shutdown(ctx context.Context, app *fh.App) error {
	return app.ShutdownWithContext(ctx)
}

func (a AdminServer) auth(c fh.Ctx) error {
	if a.Token != "" && subtle.ConstantTimeCompare([]byte(c.Get("Authorization")), []byte("Bearer "+a.Token)) != 1 {
		return httpError(c, "unauthorized", fh.StatusUnauthorized)
	}
	return c.Next()
}

func (a AdminServer) ready(c fh.Ctx) error {
	if a.Pool == nil || !(a.Pool.IsRunning() || a.Pool.IsPaused()) {
		return httpError(c, "not ready", fh.StatusServiceUnavailable)
	}
	return c.SendString("ready\n")
}

func (a AdminServer) stats(c fh.Ctx) error { return writeJSON(c, a.Pool.Stats()) }

func (a AdminServer) prometheus(c fh.Ctx) error {
	c.Set("Content-Type", "text/plain; version=0.0.4")
	return c.SendString(a.Pool.Prometheus())
}

func (a AdminServer) pause(c fh.Ctx) error     { return writeErr(c, a.Pool.Pause()) }
func (a AdminServer) resume(c fh.Ctx) error    { return writeErr(c, a.Pool.Resume()) }
func (a AdminServer) terminate(c fh.Ctx) error { return writeErr(c, a.Pool.Terminate()) }
func (a AdminServer) jobs(c fh.Ctx) error      { return writeJSON(c, a.Pool.ActiveJobs()) }

func (a AdminServer) job(c fh.Ctx) error {
	info, ok := a.Pool.InspectJob(c.Params("id"))
	if !ok {
		return httpError(c, "404 page not found", fh.StatusNotFound)
	}
	return writeJSON(c, info)
}

func (a AdminServer) cancelJob(c fh.Ctx) error {
	return writeJSON(c, map[string]any{"cancelled": a.Pool.CancelJob(c.Params("id"))})
}

func (a AdminServer) dlq(c fh.Ctx) error { return writeJSON(c, a.Pool.DeadLetters()) }

func (a AdminServer) replayDLQ(c fh.Ctx) error {
	n, err := a.Pool.ReplayDeadLetters()
	if err != nil {
		return writeJSON(c, map[string]any{"replayed": n, "error": err.Error()})
	}
	return writeJSON(c, map[string]any{"replayed": n})
}

func (a AdminServer) purgeDLQ(c fh.Ctx) error {
	return writeJSON(c, map[string]any{"purged": a.Pool.PurgeDeadLetters()})
}

func writeJSON(c fh.Ctx, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return httpError(c, err.Error(), fh.StatusInternalServerError)
	}
	c.Set("Content-Type", "application/json")
	return c.Send(append(b, '\n'))
}

func writeErr(c fh.Ctx, err error) error {
	if err != nil {
		return httpError(c, err.Error(), fh.StatusConflict)
	}
	return writeJSON(c, map[string]bool{"ok": true})
}

func method(c fh.Ctx) error { return httpError(c, "method not allowed", fh.StatusMethodNotAllowed) }

func httpError(c fh.Ctx, msg string, status int) error {
	c.Set("Content-Type", "text/plain; charset=utf-8")
	c.Set("X-Content-Type-Options", "nosniff")
	return c.Status(status).SendString(msg + "\n")
}
