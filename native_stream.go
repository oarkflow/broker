package broker

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	NativeStreamMagic  uint32 = 0x42535431 // BST1
	nativeVersion      byte   = 1
	nativeOpProduce    byte   = 1
	nativeOpFetch      byte   = 2
	nativeOpCommit     byte   = 3
	nativeOpOffset     byte   = 4
	nativeOpPing       byte   = 5
	nativeOpFetchRaw   byte   = 6
	nativeResponseFlag byte   = 0x80
	nativeHeaderSize          = 20

	NativeAuthAnonymous byte = 0x00
	NativeAuthToken     byte = 0x01
	NativeAuthMTLS      byte = 0x02

	NativeStatusSuccess      byte = 0x00
	NativeStatusUnauthorized byte = 0x01
	NativeStatusForbidden    byte = 0x02
	NativeStatusThrottled    byte = 0x03
	NativeStatusErrProtocol  byte = 0x04
)

type NativeStreamConfig struct {
	Dir              string
	Partitions       int
	SegmentBytes     int64
	GroupCommitDelay time.Duration
	GroupCommitBytes int
	MaxFrameBytes    int
	Sync             bool
	ACL              *ACL
	InsecureDev      bool
	TLSConfig        *tls.Config
}

type NativeStreamServer struct {
	cfg       NativeStreamConfig
	ln        net.Listener
	partsMu   sync.RWMutex
	parts     map[string]*nativePartition
	offsetsMu sync.RWMutex
	offsets   map[string]uint64
	closed    atomic.Bool
	wg        sync.WaitGroup
}

type nativeRecordPos struct {
	seg int
	pos int64
	n   uint32
}
type nativeSegment struct {
	base uint64
	path string
	file *os.File
	size int64
}
type nativeAppend struct {
	payloads [][]byte
	done     chan nativeAppendResult
}
type nativeAppendResult struct {
	base uint64
	err  error
}

type nativePartition struct {
	id       int
	dir      string
	cfg      NativeStreamConfig
	mu       sync.RWMutex
	segments []*nativeSegment
	index    []nativeRecordPos
	appendCh chan nativeAppend
	stop     chan struct{}
	wg       sync.WaitGroup
}

func NewNativeStreamServer(cfg NativeStreamConfig) (*NativeStreamServer, error) {
	if cfg.Dir == "" {
		return nil, errors.New("native stream directory is required")
	}
	if cfg.Partitions <= 0 {
		cfg.Partitions = 8
	}
	if cfg.SegmentBytes <= 0 {
		cfg.SegmentBytes = 1 << 30
	}
	if cfg.GroupCommitDelay <= 0 {
		cfg.GroupCommitDelay = 750 * time.Microsecond
	}
	if cfg.GroupCommitBytes <= 0 {
		cfg.GroupCommitBytes = 4 << 20
	}
	if cfg.MaxFrameBytes <= 0 {
		cfg.MaxFrameBytes = 128 << 20
	}
	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		return nil, err
	}
	s := &NativeStreamServer{cfg: cfg, offsets: make(map[string]uint64), parts: make(map[string]*nativePartition)}
	if err := s.loadOffsets(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func safeNativeStreamName(stream string) string {
	if stream == "" {
		return "default"
	}
	var b strings.Builder
	for _, r := range stream {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	name := b.String()
	if name == "" || name == "." || name == ".." {
		return "default"
	}
	return name
}

func (s *NativeStreamServer) partition(stream string, id int) (*nativePartition, error) {
	if stream == "" || id < 0 || id >= s.cfg.Partitions {
		return nil, errors.New("invalid stream partition")
	}
	key := stream + "\x00" + strconv.Itoa(id)
	s.partsMu.RLock()
	p := s.parts[key]
	s.partsMu.RUnlock()
	if p != nil {
		return p, nil
	}
	s.partsMu.Lock()
	defer s.partsMu.Unlock()
	if p = s.parts[key]; p != nil {
		return p, nil
	}
	var err error
	p, err = openNativePartition(s.cfg, stream, id)
	if err != nil {
		return nil, err
	}
	s.parts[key] = p
	return p, nil
}

func openNativePartition(cfg NativeStreamConfig, stream string, id int) (*nativePartition, error) {
	dir := filepath.Join(cfg.Dir, "streams", safeNativeStreamName(stream), fmt.Sprintf("partition-%04d", id))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	p := &nativePartition{id: id, dir: dir, cfg: cfg, appendCh: make(chan nativeAppend, 4096), stop: make(chan struct{})}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		base, err := strconv.ParseUint(strings.TrimSuffix(e.Name(), ".log"), 10, 64)
		if err != nil {
			continue
		}
		path := filepath.Join(dir, e.Name())
		f, err := os.OpenFile(path, os.O_RDWR, 0o600)
		if err != nil {
			return nil, err
		}
		st, _ := f.Stat()
		p.segments = append(p.segments, &nativeSegment{base: base, path: path, file: f, size: st.Size()})
	}
	if len(p.segments) == 0 {
		if err := p.rotateLocked(0); err != nil {
			return nil, err
		}
	}
	// directory names are zero-padded, so lexical order equals base order.
	for i := 0; i < len(p.segments)-1; i++ {
		for j := i + 1; j < len(p.segments); j++ {
			if p.segments[j].base < p.segments[i].base {
				p.segments[i], p.segments[j] = p.segments[j], p.segments[i]
			}
		}
	}
	for si, seg := range p.segments {
		if err := p.recoverSegment(si, seg); err != nil {
			return nil, err
		}
		if _, err := seg.file.Seek(0, io.SeekEnd); err != nil {
			return nil, err
		}
	}
	p.wg.Add(1)
	go p.writer()
	return p, nil
}

func (p *nativePartition) recoverSegment(si int, seg *nativeSegment) error {
	var pos int64
	var hdr [4]byte
	for {
		_, err := seg.file.ReadAt(hdr[:], pos)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if err := seg.file.Truncate(pos); err != nil {
				return err
			}
			seg.size = pos
			return nil
		}
		if err != nil {
			return err
		}
		n := binary.LittleEndian.Uint32(hdr[:])
		if n == 0 || n > uint32(p.cfg.MaxFrameBytes) {
			if err := seg.file.Truncate(pos); err != nil {
				return err
			}
			seg.size = pos
			return nil
		}
		if pos+4+int64(n) > seg.size {
			if err := seg.file.Truncate(pos); err != nil {
				return err
			}
			seg.size = pos
			return nil
		}
		p.index = append(p.index, nativeRecordPos{seg: si, pos: pos + 4, n: n})
		pos += 4 + int64(n)
		if pos == seg.size {
			return nil
		}
	}
}

func (p *nativePartition) rotateLocked(base uint64) error {
	path := filepath.Join(p.dir, fmt.Sprintf("%020d.log", base))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	p.segments = append(p.segments, &nativeSegment{base: base, path: path, file: f})
	return nil
}

func (p *nativePartition) writer() {
	defer p.wg.Done()
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	pending := make([]nativeAppend, 0, 128)
	for {
		var first nativeAppend
		select {
		case first = <-p.appendCh:
		case <-p.stop:
			return
		}
		pending = append(pending, first)
		bytesN := 0
		for _, v := range first.payloads {
			bytesN += 4 + len(v)
		}
		timer.Reset(p.cfg.GroupCommitDelay)
	collect:
		for bytesN < p.cfg.GroupCommitBytes {
			select {
			case req := <-p.appendCh:
				pending = append(pending, req)
				for _, v := range req.payloads {
					bytesN += 4 + len(v)
				}
			case <-timer.C:
				break collect
			case <-p.stop:
				break collect
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		p.flushPending(pending)
		pending = pending[:0]
	}
}

func (p *nativePartition) flushPending(reqs []nativeAppend) {
	p.mu.Lock()
	defer p.mu.Unlock()

	bases := make([]uint64, len(reqs))
	var firstErr error
	touched := make(map[int]struct{}, 2)
	var batch []byte
	batch = make([]byte, 0, minInt(p.cfg.GroupCommitBytes, 8<<20))
	batchStart := int64(0)
	batchSegment := -1
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		seg := p.segments[batchSegment]
		touched[batchSegment] = struct{}{}
		n, err := seg.file.Write(batch)
		if err == nil && n != len(batch) {
			err = io.ErrShortWrite
		}
		if err == nil {
			seg.size += int64(n)
		}
		batch = batch[:0]
		batchSegment = -1
		return err
	}

	for ri, req := range reqs {
		bases[ri] = uint64(len(p.index))
		for _, payload := range req.payloads {
			recordBytes := 4 + len(payload)
			seg := p.segments[len(p.segments)-1]
			if seg.size+int64(len(batch))+int64(recordBytes) > p.cfg.SegmentBytes && seg.size+int64(len(batch)) > 0 {
				if err := flush(); err != nil {
					firstErr = err
					break
				}
				if err := p.rotateLocked(uint64(len(p.index))); err != nil {
					firstErr = err
					break
				}
				seg = p.segments[len(p.segments)-1]
			}
			if batchSegment < 0 {
				batchSegment = len(p.segments) - 1
				batchStart = seg.size
			}
			pos := batchStart + int64(len(batch))
			batch = binary.LittleEndian.AppendUint32(batch, uint32(len(payload)))
			batch = append(batch, payload...)
			p.index = append(p.index, nativeRecordPos{seg: batchSegment, pos: pos + 4, n: uint32(len(payload))})
		}
		if firstErr != nil {
			break
		}
	}
	if firstErr == nil {
		firstErr = flush()
	}
	if firstErr == nil && p.cfg.Sync {
		// Sync only segments written by this group. Scanning the complete index
		// here makes append cost grow with retained history.
		for segID := range touched {
			if err := p.segments[segID].file.Sync(); err != nil {
				firstErr = err
				break
			}
		}
	}
	for i, req := range reqs {
		req.done <- nativeAppendResult{base: bases[i], err: firstErr}
	}
}

func (p *nativePartition) append(ctx context.Context, payloads [][]byte) (uint64, error) {
	done := make(chan nativeAppendResult, 1)
	req := nativeAppend{payloads: payloads, done: done}
	select {
	case p.appendCh <- req:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	select {
	case r := <-done:
		return r.base, r.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (p *nativePartition) fetch(offset uint64, maxRecords, maxBytes int) ([][]byte, uint64, error) {
	if maxRecords <= 0 {
		maxRecords = 1024
	}
	if maxBytes <= 0 {
		maxBytes = 4 << 20
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if offset >= uint64(len(p.index)) {
		return nil, offset, nil
	}
	out := make([][]byte, 0, minInt(maxRecords, len(p.index)-int(offset)))
	used := 0
	next := offset
	for next < uint64(len(p.index)) && len(out) < maxRecords {
		rp := p.index[next]
		if used+int(rp.n) > maxBytes && len(out) > 0 {
			break
		}
		b := make([]byte, int(rp.n))
		if _, err := p.segments[rp.seg].file.ReadAt(b, rp.pos); err != nil {
			return nil, next, err
		}
		out = append(out, b)
		used += len(b)
		next++
	}
	return out, next, nil
}

// fetchRaw returns the exact on-disk [u32 length][payload] record sequence.
// It performs one contiguous read per segment and does not allocate per record.
func (p *nativePartition) fetchRaw(offset uint64, maxRecords, maxBytes int) ([]byte, uint64, int, error) {
	if maxRecords <= 0 {
		maxRecords = 1024
	}
	if maxBytes <= 0 {
		maxBytes = 4 << 20
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if offset >= uint64(len(p.index)) {
		return nil, offset, 0, nil
	}

	start := int(offset)
	end := start
	payloadBytes := 0
	wireBytes := 0
	for end < len(p.index) && end-start < maxRecords {
		rp := p.index[end]
		if payloadBytes+int(rp.n) > maxBytes && end > start {
			break
		}
		payloadBytes += int(rp.n)
		wireBytes += 4 + int(rp.n)
		end++
	}
	if end == start {
		return nil, offset, 0, nil
	}
	out := make([]byte, wireBytes)
	dst := 0
	for i := start; i < end; {
		segID := p.index[i].seg
		first := p.index[i]
		j := i + 1
		for j < end && p.index[j].seg == segID {
			j++
		}
		last := p.index[j-1]
		readStart := first.pos - 4
		readEnd := last.pos + int64(last.n)
		n := int(readEnd - readStart)
		if _, err := p.segments[segID].file.ReadAt(out[dst:dst+n], readStart); err != nil {
			return nil, uint64(i), i - start, err
		}
		dst += n
		i = j
	}
	return out, uint64(end), end - start, nil
}

func (s *NativeStreamServer) ListenAndServe(addr string) error {
	var ln net.Listener
	var err error
	if s.cfg.TLSConfig != nil {
		ln, err = tls.Listen("tcp", addr, s.cfg.TLSConfig)
	} else {
		ln, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return err
	}
	s.ln = ln
	for {
		c, err := ln.Accept()
		if err != nil {
			if s.closed.Load() {
				return nil
			}
			continue
		}
		s.wg.Add(1)
		go s.serveConn(c)
	}
}

func writeHandshakeResponse(w io.Writer, statusCode byte, principal string) error {
	var h [8]byte
	binary.BigEndian.PutUint32(h[:4], NativeStreamMagic)
	h[4] = nativeVersion
	h[5] = statusCode
	binary.BigEndian.PutUint16(h[6:8], uint16(len(principal)))
	if _, err := w.Write(h[:]); err != nil {
		return err
	}
	if len(principal) > 0 {
		_, err := io.WriteString(w, principal)
		return err
	}
	return nil
}

func (s *NativeStreamServer) serveConn(c net.Conn) {
	defer s.wg.Done()
	defer c.Close()
	r := bufio.NewReaderSize(c, 1<<20)
	w := bufio.NewWriterSize(c, 1<<20)

	// Perform BST1 authenticated handshake
	var h [8]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return
	}
	if binary.BigEndian.Uint32(h[:4]) != NativeStreamMagic || h[4] != nativeVersion {
		_ = writeHandshakeResponse(w, NativeStatusErrProtocol, "")
		_ = w.Flush()
		return
	}
	authType := h[5]
	tokenLen := int(binary.BigEndian.Uint16(h[6:8]))
	token := ""
	if tokenLen > 0 {
		tokenBytes := make([]byte, tokenLen)
		if _, err := io.ReadFull(r, tokenBytes); err != nil {
			return
		}
		token = string(tokenBytes)
	}

	var principal AdminPrincipal
	statusCode := NativeStatusUnauthorized

	switch authType {
	case NativeAuthAnonymous:
		if s.cfg.InsecureDev || s.cfg.ACL == nil {
			principal = AdminPrincipal{Name: "anonymous", Permissions: []string{PermissionAdminAll}}
			statusCode = NativeStatusSuccess
		}
	case NativeAuthToken:
		if s.cfg.InsecureDev {
			principal = AdminPrincipal{Name: "development", Permissions: []string{PermissionAdminAll}}
			statusCode = NativeStatusSuccess
		} else if s.cfg.ACL != nil {
			if p, ok := s.cfg.ACL.Authenticate(token); ok {
				principal = p
				statusCode = NativeStatusSuccess
			}
		}
	case NativeAuthMTLS:
		if tc, ok := c.(*tls.Conn); ok {
			state := tc.ConnectionState()
			if len(state.VerifiedChains) > 0 && len(state.VerifiedChains[0]) > 0 && s.cfg.ACL != nil {
				if p, ok := s.cfg.ACL.AuthenticateCertificate(state.VerifiedChains[0][0]); ok {
					principal = p
					statusCode = NativeStatusSuccess
				}
			}
		}
	}

	if statusCode != NativeStatusSuccess {
		_ = writeHandshakeResponse(w, statusCode, "")
		_ = w.Flush()
		return
	}

	var release func()
	if s.cfg.ACL != nil {
		var err error
		release, err = s.cfg.ACL.AdmitConnection(principal)
		if err != nil {
			_ = writeHandshakeResponse(w, NativeStatusThrottled, "")
			_ = w.Flush()
			return
		}
	}
	if release != nil {
		defer release()
	}

	if err := writeHandshakeResponse(w, NativeStatusSuccess, principal.Name); err != nil {
		return
	}
	if err := w.Flush(); err != nil {
		return
	}

	for {
		op, corr, body, err := readNativeFrame(r, s.cfg.MaxFrameBytes)
		if err != nil {
			return
		}
		respOp := op | nativeResponseFlag
		var resp []byte
		switch op {
		case nativeOpPing:
		case nativeOpProduce:
			if s.cfg.ACL != nil && !s.cfg.ACL.Authorize(principal, PermissionStreamWrite) {
				resp = appendNativeError(nil, "permission denied")
			} else {
				resp = s.handleProduce(body)
			}
		case nativeOpFetch:
			if s.cfg.ACL != nil && !s.cfg.ACL.Authorize(principal, PermissionStreamRead) {
				resp = appendNativeError(nil, "permission denied")
			} else {
				resp = s.handleFetch(body)
			}
		case nativeOpFetchRaw:
			if s.cfg.ACL != nil && !s.cfg.ACL.Authorize(principal, PermissionStreamRead) {
				resp = appendNativeError(nil, "permission denied")
			} else {
				resp = s.handleFetchRaw(body)
			}
		case nativeOpCommit:
			if s.cfg.ACL != nil && !s.cfg.ACL.Authorize(principal, PermissionStreamCommit) {
				resp = appendNativeError(nil, "permission denied")
			} else {
				resp = s.handleCommit(body)
			}
		case nativeOpOffset:
			resp = s.handleOffset(body)
		default:
			resp = appendNativeError(nil, "unsupported operation")
		}
		if err := writeNativeFrame(w, respOp, corr, resp); err != nil {
			return
		}
		if err := w.Flush(); err != nil {
			return
		}
	}
}

func (s *NativeStreamServer) handleProduce(body []byte) []byte {
	d := nativeDecoder{b: body}
	stream := d.str()
	part := int(d.uvar())
	count := int(d.uvar())
	if d.err != nil || stream == "" || part < 0 || part >= s.cfg.Partitions || count <= 0 {
		return appendNativeError(nil, "invalid produce request")
	}
	payloads := make([][]byte, count)
	for i := range payloads {
		n := int(d.uvar())
		payloads[i] = d.bytes(n)
	}
	if d.err != nil || d.off != len(body) {
		return appendNativeError(nil, "invalid produce batch")
	}
	p, err := s.partition(stream, part)
	if err != nil {
		return appendNativeError(nil, err.Error())
	}
	base, err := p.append(context.Background(), payloads)
	if err != nil {
		return appendNativeError(nil, err.Error())
	}
	out := []byte{0}
	out = binary.AppendUvarint(out, base)
	out = binary.AppendUvarint(out, uint64(count))
	return out
}
func (s *NativeStreamServer) handleFetch(body []byte) []byte {
	d := nativeDecoder{b: body}
	stream := d.str()
	part := int(d.uvar())
	off := d.uvar()
	maxR := int(d.uvar())
	maxB := int(d.uvar())
	if d.err != nil || part < 0 || part >= s.cfg.Partitions {
		return appendNativeError(nil, "invalid fetch request")
	}
	p, err := s.partition(stream, part)
	if err != nil {
		return appendNativeError(nil, err.Error())
	}
	recs, next, err := p.fetch(off, maxR, maxB)
	if err != nil {
		return appendNativeError(nil, err.Error())
	}
	out := []byte{0}
	out = binary.AppendUvarint(out, next)
	out = binary.AppendUvarint(out, uint64(len(recs)))
	for _, v := range recs {
		out = binary.AppendUvarint(out, uint64(len(v)))
		out = append(out, v...)
	}
	return out
}

func (s *NativeStreamServer) handleFetchRaw(body []byte) []byte {
	d := nativeDecoder{b: body}
	stream := d.str()
	part := int(d.uvar())
	off := d.uvar()
	maxR := int(d.uvar())
	maxB := int(d.uvar())
	if d.err != nil || stream == "" || part < 0 || part >= s.cfg.Partitions {
		return appendNativeError(nil, "invalid raw fetch request")
	}
	p, err := s.partition(stream, part)
	if err != nil {
		return appendNativeError(nil, err.Error())
	}
	raw, next, count, err := p.fetchRaw(off, maxR, maxB)
	if err != nil {
		return appendNativeError(nil, err.Error())
	}
	out := make([]byte, 1, 1+20+len(raw))
	out[0] = 0
	out = binary.AppendUvarint(out, next)
	out = binary.AppendUvarint(out, uint64(count))
	out = append(out, raw...)
	return out
}

func offsetKey(stream, group string, part int) string {
	return stream + "\x00" + group + "\x00" + strconv.Itoa(part)
}
func (s *NativeStreamServer) handleCommit(body []byte) []byte {
	d := nativeDecoder{b: body}
	stream := d.str()
	group := d.str()
	part := int(d.uvar())
	off := d.uvar()
	if d.err != nil {
		return appendNativeError(nil, "invalid commit")
	}
	s.offsetsMu.Lock()
	k := offsetKey(stream, group, part)
	if off > s.offsets[k] {
		s.offsets[k] = off
	}
	err := s.persistOffsetsLocked()
	s.offsetsMu.Unlock()
	if err != nil {
		return appendNativeError(nil, err.Error())
	}
	return []byte{0}
}
func (s *NativeStreamServer) handleOffset(body []byte) []byte {
	d := nativeDecoder{b: body}
	stream := d.str()
	group := d.str()
	part := int(d.uvar())
	if d.err != nil {
		return appendNativeError(nil, "invalid offset")
	}
	s.offsetsMu.RLock()
	off := s.offsets[offsetKey(stream, group, part)]
	s.offsetsMu.RUnlock()
	out := []byte{0}
	return binary.AppendUvarint(out, off)
}
func (s *NativeStreamServer) offsetsPath() string {
	return filepath.Join(s.cfg.Dir, "consumer.offsets")
}
func (s *NativeStreamServer) loadOffsets() error {
	b, err := os.ReadFile(s.offsetsPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	d := nativeDecoder{b: b}
	n := int(d.uvar())
	for i := 0; i < n; i++ {
		k := d.str()
		s.offsets[k] = d.uvar()
	}
	return d.err
}
func (s *NativeStreamServer) persistOffsetsLocked() error {
	b := binary.AppendUvarint(nil, uint64(len(s.offsets)))
	for k, v := range s.offsets {
		b = appendNativeString(b, k)
		b = binary.AppendUvarint(b, v)
	}
	tmp := s.offsetsPath() + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	f, err := os.OpenFile(tmp, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if s.cfg.Sync {
		err = f.Sync()
	}
	_ = f.Close()
	if err != nil {
		return err
	}
	return os.Rename(tmp, s.offsetsPath())
}
func (s *NativeStreamServer) Close() error {
	if s == nil || s.closed.Swap(true) {
		return nil
	}
	if s.ln != nil {
		_ = s.ln.Close()
	}
	s.partsMu.Lock()
	for _, p := range s.parts {
		if p == nil {
			continue
		}
		close(p.stop)
		p.wg.Wait()
		p.mu.Lock()
		for _, seg := range p.segments {
			_ = seg.file.Sync()
			_ = seg.file.Close()
		}
		p.mu.Unlock()
	}
	s.partsMu.Unlock()
	s.wg.Wait()
	return nil
}

type nativeDecoder struct {
	b   []byte
	off int
	err error
}

func (d *nativeDecoder) uvar() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b[d.off:])
	if n <= 0 {
		d.err = io.ErrUnexpectedEOF
		return 0
	}
	d.off += n
	return v
}
func (d *nativeDecoder) str() string { n := int(d.uvar()); return string(d.bytes(n)) }
func (d *nativeDecoder) bytes(n int) []byte {
	if d.err != nil {
		return nil
	}
	if n < 0 || d.off+n > len(d.b) {
		d.err = io.ErrUnexpectedEOF
		return nil
	}
	v := d.b[d.off : d.off+n]
	d.off += n
	return v
}
func appendNativeString(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}
func appendNativeError(dst []byte, msg string) []byte {
	dst = append(dst, 1)
	return appendNativeString(dst, msg)
}
func readNativeFrame(r io.Reader, max int) (byte, uint64, []byte, error) {
	var h [nativeHeaderSize]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return 0, 0, nil, err
	}
	if binary.BigEndian.Uint32(h[:4]) != NativeStreamMagic || h[4] != nativeVersion {
		return 0, 0, nil, errors.New("invalid native stream frame")
	}
	n := int(binary.BigEndian.Uint32(h[16:20]))
	if n < 0 || n > max {
		return 0, 0, nil, errors.New("native frame too large")
	}
	b := make([]byte, n)
	_, err := io.ReadFull(r, b)
	return h[5], binary.BigEndian.Uint64(h[8:16]), b, err
}
func writeNativeFrame(w io.Writer, op byte, corr uint64, body []byte) error {
	var h [nativeHeaderSize]byte
	binary.BigEndian.PutUint32(h[:4], NativeStreamMagic)
	h[4] = nativeVersion
	h[5] = op
	binary.BigEndian.PutUint64(h[8:16], corr)
	binary.BigEndian.PutUint32(h[16:20], uint32(len(body)))
	if _, err := w.Write(h[:]); err != nil {
		return err
	}
	_, err := w.Write(body)
	return err
}

// NativeStreamClient is a persistent, correlation-safe client. One client may
// be used concurrently; requests are serialized intentionally to avoid a
// goroutine and allocation per in-flight response. Scale with a small pool.
type NativeStreamClient struct {
	conn      net.Conn
	r         *bufio.Reader
	w         *bufio.Writer
	mu        sync.Mutex
	corr      uint64
	max       int
	principal string
}

// NativeStreamDialOptions configures a BST1 client connection.
type NativeStreamDialOptions struct {
	Timeout   time.Duration
	Token     string
	TLSConfig *tls.Config
	AuthType  byte
}

// DialNativeStreamWithOptions connects to a BST1 server with explicit auth and TLS options.
func DialNativeStreamWithOptions(addr string, opts NativeStreamDialOptions) (*NativeStreamClient, error) {
	if opts.Timeout <= 0 {
		opts.Timeout = 10 * time.Second
	}
	var c net.Conn
	var err error
	if opts.TLSConfig != nil {
		dialer := &net.Dialer{Timeout: opts.Timeout}
		c, err = tls.DialWithDialer(dialer, "tcp", addr, opts.TLSConfig)
	} else {
		c, err = net.DialTimeout("tcp", addr, opts.Timeout)
	}
	if err != nil {
		return nil, err
	}

	r := bufio.NewReaderSize(c, 1<<20)
	w := bufio.NewWriterSize(c, 1<<20)

	authType := opts.AuthType
	if authType == 0 && opts.Token != "" {
		authType = NativeAuthToken
	} else if authType == 0 && opts.TLSConfig != nil && len(opts.TLSConfig.Certificates) > 0 {
		authType = NativeAuthMTLS
	}

	var reqH [8]byte
	binary.BigEndian.PutUint32(reqH[:4], NativeStreamMagic)
	reqH[4] = nativeVersion
	reqH[5] = authType
	binary.BigEndian.PutUint16(reqH[6:8], uint16(len(opts.Token)))
	if _, err := w.Write(reqH[:]); err != nil {
		c.Close()
		return nil, err
	}
	if len(opts.Token) > 0 {
		if _, err := io.WriteString(w, opts.Token); err != nil {
			c.Close()
			return nil, err
		}
	}
	if err := w.Flush(); err != nil {
		c.Close()
		return nil, err
	}

	var respH [8]byte
	if _, err := io.ReadFull(r, respH[:]); err != nil {
		c.Close()
		return nil, err
	}
	if binary.BigEndian.Uint32(respH[:4]) != NativeStreamMagic || respH[4] != nativeVersion {
		c.Close()
		return nil, errors.New("BST1 protocol error in handshake response")
	}
	status := respH[5]
	principalLen := int(binary.BigEndian.Uint16(respH[6:8]))
	principal := ""
	if principalLen > 0 {
		pBytes := make([]byte, principalLen)
		if _, err := io.ReadFull(r, pBytes); err != nil {
			c.Close()
			return nil, err
		}
		principal = string(pBytes)
	}

	if status != NativeStatusSuccess {
		c.Close()
		switch status {
		case NativeStatusUnauthorized:
			return nil, ErrUnauthorized
		case NativeStatusForbidden:
			return nil, ErrUnauthorized
		case NativeStatusThrottled:
			return nil, ErrThrottled
		default:
			return nil, fmt.Errorf("BST1 handshake failed with status code %d", status)
		}
	}

	return &NativeStreamClient{
		conn:      c,
		r:         r,
		w:         w,
		max:       128 << 20,
		principal: principal,
	}, nil
}

func DialNativeStream(addr string, timeout time.Duration) (*NativeStreamClient, error) {
	return DialNativeStreamWithOptions(addr, NativeStreamDialOptions{Timeout: timeout})
}

func (c *NativeStreamClient) Principal() string {
	if c == nil {
		return ""
	}
	return c.principal
}
func (c *NativeStreamClient) call(op byte, body []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.corr++
	if err := writeNativeFrame(c.w, op, c.corr, body); err != nil {
		return nil, err
	}
	if err := c.w.Flush(); err != nil {
		return nil, err
	}
	rop, corr, resp, err := readNativeFrame(c.r, c.max)
	if err != nil {
		return nil, err
	}
	if rop != op|nativeResponseFlag || corr != c.corr {
		return nil, errors.New("native response mismatch")
	}
	if len(resp) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	if resp[0] != 0 {
		d := nativeDecoder{b: resp[1:]}
		return nil, errors.New(d.str())
	}
	return resp[1:], nil
}
func (c *NativeStreamClient) Produce(stream string, partition int, payloads [][]byte) (uint64, error) {
	b := appendNativeString(nil, stream)
	b = binary.AppendUvarint(b, uint64(partition))
	b = binary.AppendUvarint(b, uint64(len(payloads)))
	for _, v := range payloads {
		b = binary.AppendUvarint(b, uint64(len(v)))
		b = append(b, v...)
	}
	r, err := c.call(nativeOpProduce, b)
	if err != nil {
		return 0, err
	}
	d := nativeDecoder{b: r}
	base := d.uvar()
	_ = d.uvar()
	return base, d.err
}
func (c *NativeStreamClient) Fetch(stream string, partition int, offset uint64, maxRecords, maxBytes int) ([][]byte, uint64, error) {
	b := appendNativeString(nil, stream)
	b = binary.AppendUvarint(b, uint64(partition))
	b = binary.AppendUvarint(b, offset)
	b = binary.AppendUvarint(b, uint64(maxRecords))
	b = binary.AppendUvarint(b, uint64(maxBytes))
	r, err := c.call(nativeOpFetch, b)
	if err != nil {
		return nil, offset, err
	}
	d := nativeDecoder{b: r}
	next := d.uvar()
	n := int(d.uvar())
	out := make([][]byte, n)
	for i := range out {
		out[i] = append([]byte(nil), d.bytes(int(d.uvar()))...)
	}
	return out, next, d.err
}

// FetchRaw retrieves a batch without creating one allocation per record. The
// returned buffer contains consecutive little-endian u32 lengths and payloads.
func (c *NativeStreamClient) FetchRaw(stream string, partition int, offset uint64, maxRecords, maxBytes int) ([]byte, int, uint64, error) {
	b := appendNativeString(nil, stream)
	b = binary.AppendUvarint(b, uint64(partition))
	b = binary.AppendUvarint(b, offset)
	b = binary.AppendUvarint(b, uint64(maxRecords))
	b = binary.AppendUvarint(b, uint64(maxBytes))
	r, err := c.call(nativeOpFetchRaw, b)
	if err != nil {
		return nil, 0, offset, err
	}
	d := nativeDecoder{b: r}
	next := d.uvar()
	count := int(d.uvar())
	if d.err != nil {
		return nil, 0, offset, d.err
	}
	raw := r[d.off:]
	pos := 0
	for i := 0; i < count; i++ {
		if pos+4 > len(raw) {
			return nil, 0, offset, io.ErrUnexpectedEOF
		}
		n := int(binary.LittleEndian.Uint32(raw[pos : pos+4]))
		pos += 4
		if n < 0 || pos+n > len(raw) {
			return nil, 0, offset, io.ErrUnexpectedEOF
		}
		pos += n
	}
	if pos != len(raw) {
		return nil, 0, offset, errors.New("native raw fetch trailing bytes")
	}
	return raw, count, next, nil
}

// VisitFetchRaw visits payload slices backed by raw without copying them.
func VisitFetchRaw(raw []byte, fn func([]byte) error) error {
	for pos := 0; pos < len(raw); {
		if pos+4 > len(raw) {
			return io.ErrUnexpectedEOF
		}
		n := int(binary.LittleEndian.Uint32(raw[pos : pos+4]))
		pos += 4
		if n < 0 || pos+n > len(raw) {
			return io.ErrUnexpectedEOF
		}
		if fn != nil {
			if err := fn(raw[pos : pos+n]); err != nil {
				return err
			}
		}
		pos += n
	}
	return nil
}

func (c *NativeStreamClient) Commit(stream, group string, partition int, offset uint64) error {
	b := appendNativeString(nil, stream)
	b = appendNativeString(b, group)
	b = binary.AppendUvarint(b, uint64(partition))
	b = binary.AppendUvarint(b, offset)
	_, err := c.call(nativeOpCommit, b)
	return err
}
func (c *NativeStreamClient) Offset(stream, group string, partition int) (uint64, error) {
	b := appendNativeString(nil, stream)
	b = appendNativeString(b, group)
	b = binary.AppendUvarint(b, uint64(partition))
	r, err := c.call(nativeOpOffset, b)
	if err != nil {
		return 0, err
	}
	d := nativeDecoder{b: r}
	return d.uvar(), d.err
}
func (c *NativeStreamClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
