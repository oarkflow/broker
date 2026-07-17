package broker

import (
	"bytes"
	"net"
	"testing"
	"time"
)

func netListenLocal(s *NativeStreamServer) (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	s.ln = ln
	go func() {
		for {
			c, e := ln.Accept()
			if e != nil {
				return
			}
			s.wg.Add(1)
			go s.serveConn(c)
		}
	}()
	return ln.Addr().String(), nil
}

func TestNativeStreamRestartAndOffsets(t *testing.T) {
	dir := t.TempDir()
	s, err := NewNativeStreamServer(NativeStreamConfig{Dir: dir, Partitions: 2, SegmentBytes: 128, GroupCommitDelay: 50 * time.Microsecond, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := netListenLocal(s)
	if err != nil {
		t.Fatal(err)
	}
	c, err := DialNativeStream(ln, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	payloads := [][]byte{[]byte("a"), []byte("bb"), []byte("ccc")}
	if _, err = c.Produce("events", 1, payloads); err != nil {
		t.Fatal(err)
	}
	got, next, err := c.Fetch("events", 1, 0, 10, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if next != 3 || len(got) != 3 || !bytes.Equal(got[2], payloads[2]) {
		t.Fatalf("unexpected fetch next=%d got=%q", next, got)
	}
	if err = c.Commit("events", "g1", 1, next); err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	_ = s.Close()
	s2, err := NewNativeStreamServer(NativeStreamConfig{Dir: dir, Partitions: 2, SegmentBytes: 128, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	off, err := func() (uint64, error) {
		addr, err := netListenLocal(s2)
		if err != nil {
			return 0, err
		}
		c2, err := DialNativeStream(addr, 2*time.Second)
		if err != nil {
			return 0, err
		}
		defer c2.Close()
		records, next, err := c2.Fetch("events", 1, 0, 10, 1024)
		if err != nil {
			return 0, err
		}
		if len(records) != 3 || next != 3 {
			t.Fatalf("recovery len=%d next=%d", len(records), next)
		}
		return c2.Offset("events", "g1", 1)
	}()
	if err != nil || off != 3 {
		t.Fatalf("offset=%d err=%v", off, err)
	}
}

func TestNativeStreamFetchRaw(t *testing.T) {
	dir := t.TempDir()
	s, err := NewNativeStreamServer(NativeStreamConfig{Dir: dir, Partitions: 2, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := netListenLocal(s)
	if err != nil {
		t.Fatal(err)
	}
	c, err := DialNativeStream(addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	payloads := [][]byte{[]byte("a"), []byte("bb"), []byte("ccc")}
	if _, err := c.Produce("raw", 0, payloads); err != nil {
		t.Fatal(err)
	}
	raw, count, next, err := c.FetchRaw("raw", 0, 0, 10, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 || next != 3 {
		t.Fatalf("count=%d next=%d", count, next)
	}
	var got [][]byte
	if err := VisitFetchRaw(raw, func(v []byte) error { got = append(got, append([]byte(nil), v...)); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || string(got[0]) != "a" || string(got[2]) != "ccc" {
		t.Fatalf("got=%q", got)
	}
	_ = c.Close()
	_ = s.Close()
}
