package client

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

// A server that resets the first connections before reading anything must not
// surface as an error: the request was never delivered, so it is resent.
func TestSendRetriesWriteReset(t *testing.T) {
	app := fh.New(fh.WithStartupBannerDisabled(true))
	app.Post("/x", func(c fh.Ctx) error { return c.SendStatus(204) })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var accepted atomic.Int32
	proxy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.Serve(ln) }()
	go func() {
		for {
			c, err := proxy.Accept()
			if err != nil {
				return
			}
			if accepted.Add(1) <= 2 {
				_ = c.Close()
				continue
			}
			up, err := net.Dial("tcp", ln.Addr().String())
			if err != nil {
				_ = c.Close()
				continue
			}
			go func() { _, _ = io.Copy(up, c); _ = up.Close() }()
			go func() { _, _ = io.Copy(c, up); _ = c.Close() }()
		}
	}()
	defer proxy.Close()
	defer ln.Close()
	c := New(Config{BrokerURL: "http://" + proxy.Addr().String()})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	body := make([]byte, 1<<20) // large enough that the write itself hits the reset
	resp, err := c.send(ctx, fh.MethodPOST, c.urls[0]+"/x", body, nil)
	if err != nil {
		t.Fatalf("send: %v (accepted=%d)", err, accepted.Load())
	}
	_ = resp.Close()
}
