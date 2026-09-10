package broker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
)

var (
	ErrInvalidRange = errors.New("zerocopy: invalid segment range")
)

// SendSegmentRange transfers count bytes from file starting at offset directly to conn.
// If conn supports io.ReaderFrom (such as *net.TCPConn), it will utilize kernel zero-copy
// mechanisms (sendfile(2) / splice(2)) automatically.
func SendSegmentRange(conn net.Conn, file *os.File, offset, count int64) (int64, error) {
	if count <= 0 {
		return 0, nil
	}
	if file == nil {
		return 0, errors.New("zerocopy: nil file")
	}

	section := io.NewSectionReader(file, offset, count)
	if rf, ok := conn.(io.ReaderFrom); ok {
		return rf.ReadFrom(section)
	}

	return io.CopyN(conn, section, count)
}

// WriteZeroCopyFetchFrame writes the BST1 response header and streams raw on-disk
// records directly from the partition segment file(s) into the connection with zero user-space record copies.
func (p *nativePartition) WriteZeroCopyFetch(conn net.Conn, corr uint64, offset uint64, maxRecords, maxBytes int) (uint64, int, error) {
	if maxRecords <= 0 {
		maxRecords = 1024
	}
	if maxBytes <= 0 {
		maxBytes = 4 << 20
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	if offset >= uint64(len(p.index)) {
		// Empty response frame
		header := make([]byte, nativeHeaderSize+1+binary.MaxVarintLen64*2)
		binary.BigEndian.PutUint32(header[0:4], NativeStreamMagic)
		header[4] = nativeVersion
		header[5] = nativeOpFetchRaw | nativeResponseFlag
		binary.BigEndian.PutUint16(header[6:8], 0)
		binary.BigEndian.PutUint64(header[8:16], corr)

		idx := nativeHeaderSize
		header[idx] = 0 // Success status
		idx++
		idx += binary.PutUvarint(header[idx:], offset)
		idx += binary.PutUvarint(header[idx:], 0)

		payloadLen := uint32(idx - nativeHeaderSize)
		binary.BigEndian.PutUint32(header[16:20], payloadLen)

		_, err := conn.Write(header[:idx])
		return offset, 0, err
	}

	start := int(offset)
	end := start
	wireBytes := 0
	for end < len(p.index) && end-start < maxRecords {
		rp := p.index[end]
		if wireBytes+int(rp.n)+4 > maxBytes && end > start {
			break
		}
		wireBytes += 4 + int(rp.n)
		end++
	}

	count := end - start
	var prefixBuf [1 + binary.MaxVarintLen64*2]byte
	prefixBuf[0] = 0 // Success
	nOff := binary.PutUvarint(prefixBuf[1:], uint64(end))
	nCount := binary.PutUvarint(prefixBuf[1+nOff:], uint64(count))
	prefixLen := 1 + nOff + nCount

	totalPayloadLen := uint32(prefixLen + wireBytes)

	// Write 20-byte native BST1 frame header:
	// [Magic:4][Version:1][Op:1][ChanID:2][Corr:8][Length:4]
	var frameHeader [nativeHeaderSize]byte
	binary.BigEndian.PutUint32(frameHeader[0:4], NativeStreamMagic)
	frameHeader[4] = nativeVersion
	frameHeader[5] = nativeOpFetchRaw | nativeResponseFlag
	binary.BigEndian.PutUint16(frameHeader[6:8], 0)
	binary.BigEndian.PutUint64(frameHeader[8:16], corr)
	binary.BigEndian.PutUint32(frameHeader[16:20], totalPayloadLen)

	if _, err := conn.Write(frameHeader[:]); err != nil {
		return uint64(end), count, fmt.Errorf("write frame header: %w", err)
	}
	if _, err := conn.Write(prefixBuf[:prefixLen]); err != nil {
		return uint64(end), count, fmt.Errorf("write prefix: %w", err)
	}

	// Stream segment file data directly to the connection
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
		segBytes := readEnd - readStart

		if _, err := SendSegmentRange(conn, p.segments[segID].file, readStart, segBytes); err != nil {
			return uint64(end), count, fmt.Errorf("stream segment: %w", err)
		}
		i = j
	}

	return uint64(end), count, nil
}
