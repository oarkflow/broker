package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/oarkflow/broker"
	"github.com/oarkflow/fh"
)

// Request-path buffers. A request body and the response it produces are both
// transient for JSON calls: the body is only read while the request is in
// flight and json.Unmarshal copies everything it keeps. Reusing the buffers
// removes the largest per-call allocations.
var requestBufferPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

// maxPooledBuffer keeps an occasional very large response from pinning memory
// in the pool.
const maxPooledBuffer = 1 << 20

func getRequestBuffer() *bytes.Buffer {
	buf := requestBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func putRequestBuffer(buf *bytes.Buffer) {
	if buf == nil || buf.Cap() > maxPooledBuffer {
		return
	}
	requestBufferPool.Put(buf)
}

// encodeJSONBody encodes v into a pooled buffer. The trailing newline written
// by json.Encoder is harmless to the broker's decoder.
func encodeJSONBody(v any) (*bytes.Buffer, error) {
	buf := getRequestBuffer()
	if err := json.NewEncoder(buf).Encode(v); err != nil {
		putRequestBuffer(buf)
		return nil, err
	}
	return buf, nil
}

// readBoundedInto reads at most maxBytes of body into dst, sized up front
// from the response's Content-Length when it is known. ReadFrom always wants
// bytes.MinRead of spare room, so the hint reserves that too.
func readBoundedInto(dst *bytes.Buffer, body io.Reader, maxBytes, sizeHint int64) error {
	if maxBytes <= 0 {
		maxBytes = defaultMaxResponseBytes
	}
	if sizeHint > maxBytes {
		return fmt.Errorf("%w: limit is %d bytes", ErrResponseTooLarge, maxBytes)
	}
	if sizeHint > 0 {
		dst.Grow(int(sizeHint) + bytes.MinRead)
	}
	limit := maxBytes
	if limit < int64(^uint64(0)>>1) {
		limit++
	}
	if _, err := dst.ReadFrom(io.LimitReader(body, limit)); err != nil {
		return fmt.Errorf("read broker response: %w", err)
	}
	if int64(dst.Len()) > maxBytes {
		return fmt.Errorf("%w: limit is %d bytes", ErrResponseTooLarge, maxBytes)
	}
	return nil
}

// readBoundedResponseHint returns a freshly allocated body for callers that
// keep the bytes (frame batches decode views into them). With a known
// Content-Length it reads in one exactly sized allocation.
func readBoundedResponseHint(body io.Reader, maxBytes, sizeHint int64) ([]byte, error) {
	if maxBytes <= 0 {
		maxBytes = defaultMaxResponseBytes
	}
	if sizeHint > maxBytes {
		return nil, fmt.Errorf("%w: limit is %d bytes", ErrResponseTooLarge, maxBytes)
	}
	if sizeHint > 0 {
		data := make([]byte, sizeHint)
		if _, err := io.ReadFull(body, data); err != nil {
			return nil, fmt.Errorf("read broker response: %w", err)
		}
		// A body longer than its declared length is a protocol error; the
		// HTTP layer already enforces Content-Length, so this is a backstop.
		var probe [1]byte
		if n, _ := body.Read(probe[:]); n > 0 {
			return nil, fmt.Errorf("read broker response: body exceeds Content-Length %d", sizeHint)
		}
		return data, nil
	}
	buf := new(bytes.Buffer)
	if err := readBoundedInto(buf, body, maxBytes, 0); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// headerSets caches the request header maps for a client. The token is fixed
// for the client's life, so each (content type, Prefer) combination needs one
// read-only map; fh copies it into its own request headers.
type headerSets struct {
	token string
	sets  sync.Map // key: contentType + "\x00" + prefer -> map[string]string
}

func (h *headerSets) get(contentType, prefer string) map[string]string {
	key := contentType + "\x00" + prefer
	if v, ok := h.sets.Load(key); ok {
		return v.(map[string]string)
	}
	m := make(map[string]string, 3)
	if contentType != "" {
		m["Content-Type"] = contentType
	}
	if h.token != "" {
		m["Authorization"] = "Bearer " + h.token
	}
	if prefer != "" {
		m["Prefer"] = prefer
	}
	v, _ := h.sets.LoadOrStore(key, m)
	return v.(map[string]string)
}

var errBrokerUnavailable = errors.New("broker unavailable")

// maxWriteRetries bounds resends after a connection-level write failure.
const maxWriteRetries = 3

// isWriteReset reports whether err is a failure to write the request onto a
// connection the peer had already closed or reset, or that the transport
// closed under the writer (stale keep-alive
// connection, or an accept-backlog overflow under a connection burst). The
// request never reached the server in full, so it cannot have been executed
// and resending it is safe for any method, idempotent or not.
func isWriteReset(err error) bool {
	if err == nil {
		return false
	}
	// net/http reports a keep-alive connection the server closed while idle
	// as a plain string error; it is only returned before any response byte
	// was read on a reused connection, and net/http itself retries it for
	// every method.
	if strings.Contains(err.Error(), "server closed idle connection") {
		return true
	}
	for e := err; e != nil; e = errors.Unwrap(e) {
		var op *net.OpError
		if !errors.As(e, &op) {
			return false
		}
		// "readfrom" is the body copy onto the socket, i.e. still a write.
		if (op.Op == "write" || op.Op == "readfrom") &&
			(errors.Is(op.Err, syscall.EPIPE) || errors.Is(op.Err, syscall.ECONNRESET) || errors.Is(op.Err, net.ErrClosed)) {
			return true
		}
		e = op
	}
	return false
}

// send issues one request, transparently resending when the connection broke
// before the request could be written.
func (c *Client) send(ctx context.Context, method, url string, body []byte, headers map[string]string) (*fh.Response, error) {
	for attempt := 0; ; attempt++ {
		resp, err := broker.SendHTTP(ctx, c.http, method, url, body, headers)
		if err == nil || attempt >= maxWriteRetries || !isWriteReset(err) {
			return resp, err
		}
		// Short, growing pause so a connection burst can drain.
		t := time.NewTimer(time.Duration(attempt+1) * 2 * time.Millisecond)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, err
		case <-t.C:
		}
	}
}
