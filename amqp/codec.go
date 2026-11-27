package amqp

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

// Codec handles encoding and decoding of AMQP frames.
type Codec struct {
	rw       io.ReadWriter
	frameMax uint32
	reader   *bytes.Reader
	buf      *bytes.Buffer
	writeMu  sync.Mutex
	readMu   sync.Mutex
}

// NewCodec creates a new AMQP codec for the given connection.
func NewCodec(rw io.ReadWriter) *Codec {
	return &Codec{
		rw:       rw,
		frameMax: FrameMaxSize,
		buf:      new(bytes.Buffer),
	}
}

// SetFrameMax sets the maximum frame size.
func (c *Codec) SetFrameMax(size uint32) {
	c.frameMax = size
}

// ReadFrame reads a single frame from the connection.
func (c *Codec) ReadFrame() (*Frame, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	// Read frame type (1 byte)
	var frameType uint8
	if err := binary.Read(c.rw, binary.BigEndian, &frameType); err != nil {
		return nil, err
	}

	// Read channel ID (2 bytes)
	var channelID uint16
	if err := binary.Read(c.rw, binary.BigEndian, &channelID); err != nil {
		return nil, err
	}

	// Read payload size (4 bytes)
	var payloadSize uint32
	if err := binary.Read(c.rw, binary.BigEndian, &payloadSize); err != nil {
		return nil, err
	}

	// Read payload
	if payloadSize > c.frameMax {
		return nil, fmt.Errorf("frame too large: %d > %d", payloadSize, c.frameMax)
	}
	payload := make([]byte, payloadSize)
	if _, err := io.ReadFull(c.rw, payload); err != nil {
		return nil, err
	}

	// Read frame end byte (1 byte)
	var frameEnd uint8
	if err := binary.Read(c.rw, binary.BigEndian, &frameEnd); err != nil {
		return nil, err
	}
	if frameEnd != FrameEnd {
		return nil, fmt.Errorf("invalid frame end: 0x%02X", frameEnd)
	}

	return &Frame{
		Type:      frameType,
		ChannelID: channelID,
		Payload:   payload,
	}, nil
}

// WriteFrame writes a frame to the connection.
func (c *Codec) WriteFrame(frame *Frame) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.buf.Reset()

	// Write frame type
	c.buf.WriteByte(frame.Type)

	// Write channel ID
	binary.Write(c.buf, binary.BigEndian, frame.ChannelID)

	// Write payload size
	binary.Write(c.buf, binary.BigEndian, uint32(len(frame.Payload)))

	// Write payload
	c.buf.Write(frame.Payload)

	// Write frame end
	c.buf.WriteByte(FrameEnd)

	_, err := c.rw.Write(c.buf.Bytes())
	if flusher, ok := c.rw.(interface{ Flush() error }); ok {
		if fErr := flusher.Flush(); fErr != nil && err == nil {
			err = fErr
		}
	}
	return err
}

// WriteMethodFrame writes a method frame for the given channel.
func (c *Codec) WriteMethodFrame(channelID uint16, classID, methodID uint16, args []byte) error {
	payload := make([]byte, 4+len(args))
	binary.BigEndian.PutUint16(payload[0:2], classID)
	binary.BigEndian.PutUint16(payload[2:4], methodID)
	copy(payload[4:], args)

	return c.WriteFrame(&Frame{
		Type:      FrameMethod,
		ChannelID: channelID,
		Payload:   payload,
	})
}

// WriteHeaderFrame writes a content header frame.
func (c *Codec) WriteHeaderFrame(channelID uint16, classID uint16, bodySize uint64, propertyFlags uint16, propertyBuf []byte) error {
	payload := make([]byte, 14+len(propertyBuf))
	binary.BigEndian.PutUint16(payload[0:2], classID)
	// Content-header frames include a two-byte weight field, which is zero in
	// AMQP 0-9-1.
	binary.BigEndian.PutUint16(payload[2:4], 0)
	binary.BigEndian.PutUint64(payload[4:12], bodySize)
	binary.BigEndian.PutUint16(payload[12:14], propertyFlags)
	copy(payload[14:], propertyBuf)

	return c.WriteFrame(&Frame{
		Type:      FrameHeader,
		ChannelID: channelID,
		Payload:   payload,
	})
}

// WriteBodyFrame writes a content body frame.
func (c *Codec) WriteBodyFrame(channelID uint16, body []byte) error {
	return c.WriteFrame(&Frame{
		Type:      FrameBody,
		ChannelID: channelID,
		Payload:   body,
	})
}

// WriteHeartbeat writes a heartbeat frame.
func (c *Codec) WriteHeartbeat() error {
	return c.WriteFrame(&Frame{
		Type:      FrameHeartbeat,
		ChannelID: 0,
		Payload:   nil,
	})
}

// ParseMethodFrame parses a method frame payload.
func ParseMethodFrame(payload []byte) (*MethodFrame, error) {
	if len(payload) < 4 {
		return nil, errors.New("method frame too short")
	}
	return &MethodFrame{
		ClassID:  binary.BigEndian.Uint16(payload[0:2]),
		MethodID: binary.BigEndian.Uint16(payload[2:4]),
		Args:     payload[4:],
	}, nil
}

// ParseHeaderFrame parses a content header frame payload.
func ParseHeaderFrame(payload []byte) (*HeaderFrame, error) {
	if len(payload) < 14 {
		return nil, errors.New("header frame too short")
	}
	return &HeaderFrame{
		ClassID:       binary.BigEndian.Uint16(payload[0:2]),
		BodySize:      binary.BigEndian.Uint64(payload[4:12]),
		PropertyFlags: binary.BigEndian.Uint16(payload[12:14]),
		PropertyBuf:   payload[14:],
	}, nil
}

// EncodeMethodArgs encodes method arguments using AMQP wire format.
func EncodeMethodArgs(args ...any) []byte {
	var buf bytes.Buffer
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if _, ok := arg.(bool); ok {
			var bits uint8
			for bit := uint(0); bit < 8 && i < len(args); bit++ {
				value, ok := args[i].(bool)
				if !ok {
					break
				}
				if value {
					bits |= 1 << bit
				}
				i++
			}
			buf.WriteByte(bits)
			i--
			continue
		}
		switch v := arg.(type) {
		case uint8:
			buf.WriteByte(v)
		case uint16:
			binary.Write(&buf, binary.BigEndian, v)
		case uint32:
			binary.Write(&buf, binary.BigEndian, v)
		case uint64:
			binary.Write(&buf, binary.BigEndian, v)
		case int8:
			buf.WriteByte(uint8(v))
		case int16:
			binary.Write(&buf, binary.BigEndian, uint16(v))
		case int32:
			binary.Write(&buf, binary.BigEndian, uint32(v))
		case string:
			buf.WriteByte(uint8(len(v)))
			buf.WriteString(v)
		case []byte:
			binary.Write(&buf, binary.BigEndian, uint32(len(v)))
			buf.Write(v)
		case table:
			encodeTable(&buf, v)
		}
	}
	return buf.Bytes()
}

// table is a map[string]any for AMQP field tables.
type table = map[string]any

const maxAMQPFieldTableDepth = 32

// encodeTable encodes an AMQP field table.
func encodeTable(buf *bytes.Buffer, t table) {
	// Reserve space for length
	start := buf.Len()
	binary.Write(buf, binary.BigEndian, uint32(0)) // placeholder

	for key, value := range t {
		buf.WriteByte(byte(len(key)))
		buf.WriteString(key)
		encodeFieldValue(buf, value)
	}

	// Update length
	end := buf.Len()
	length := uint32(end - start - 4)
	buf.Bytes()[start] = byte(length >> 24)
	buf.Bytes()[start+1] = byte(length >> 16)
	buf.Bytes()[start+2] = byte(length >> 8)
	buf.Bytes()[start+3] = byte(length)
}

// encodeFieldValue encodes a single field value.
func encodeFieldValue(buf *bytes.Buffer, v any) {
	switch val := v.(type) {
	case string:
		buf.WriteByte('S')
		binary.Write(buf, binary.BigEndian, uint32(len(val)))
		buf.WriteString(val)
	case int:
		buf.WriteByte('I')
		binary.Write(buf, binary.BigEndian, uint32(val))
	case int32:
		buf.WriteByte('I')
		binary.Write(buf, binary.BigEndian, uint32(val))
	case int64:
		buf.WriteByte('L')
		binary.Write(buf, binary.BigEndian, uint64(val))
	case uint8:
		buf.WriteByte('B')
		buf.WriteByte(val)
	case bool:
		buf.WriteByte('t')
		if val {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	case float64:
		buf.WriteByte('f')
		binary.Write(buf, binary.BigEndian, val)
	case []byte:
		buf.WriteByte('x')
		binary.Write(buf, binary.BigEndian, uint32(len(val)))
		buf.Write(val)
	case table:
		buf.WriteByte('F')
		encodeTable(buf, val)
	}
}

// DecodeTable decodes an AMQP field table from bytes.
func DecodeTable(data []byte) (table, error) {
	if len(data) < 4 {
		return nil, io.ErrUnexpectedEOF
	}

	length := binary.BigEndian.Uint32(data[0:4])
	available := uint64(len(data) - 4)
	if uint64(length) > available {
		return nil, io.ErrUnexpectedEOF
	}
	if uint64(length) < available {
		return nil, errors.New("AMQP field table has trailing bytes")
	}
	return decodeTableEntries(data[4:], 0)
}

func decodeTableEntries(data []byte, depth int) (table, error) {
	if depth > maxAMQPFieldTableDepth {
		return nil, errors.New("AMQP field table nesting exceeds limit")
	}
	t := make(table)
	r := bytes.NewReader(data)
	for r.Len() > 0 {
		// Read key length
		var keyLen uint8
		if err := binary.Read(r, binary.BigEndian, &keyLen); err != nil {
			return nil, err
		}

		// Read key
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(r, key); err != nil {
			return nil, err
		}

		// Read value
		value, err := decodeFieldValue(r, depth)
		if err != nil {
			return nil, err
		}

		t[string(key)] = value
	}

	return t, nil
}

// decodeFieldValue reads a single field value from the reader.
func decodeFieldValue(r *bytes.Reader, depth int) (any, error) {
	var valueType byte
	if err := binary.Read(r, binary.BigEndian, &valueType); err != nil {
		return nil, err
	}

	switch valueType {
	case 'V':
		return nil, nil
	case 'S':
		var length uint32
		if err := binary.Read(r, binary.BigEndian, &length); err != nil {
			return nil, err
		}
		buf, err := readFieldValueBytes(r, length)
		if err != nil {
			return nil, err
		}
		return string(buf), nil

	case 'I':
		var val uint32
		if err := binary.Read(r, binary.BigEndian, &val); err != nil {
			return nil, err
		}
		return int(val), nil

	case 'L':
		var val uint64
		if err := binary.Read(r, binary.BigEndian, &val); err != nil {
			return nil, err
		}
		return int64(val), nil

	case 'B':
		var val uint8
		if err := binary.Read(r, binary.BigEndian, &val); err != nil {
			return nil, err
		}
		return val, nil

	case 't':
		var val uint8
		if err := binary.Read(r, binary.BigEndian, &val); err != nil {
			return nil, err
		}
		return val != 0, nil

	case 'f':
		var val float64
		if err := binary.Read(r, binary.BigEndian, &val); err != nil {
			return nil, err
		}
		return val, nil

	case 'x':
		var length uint32
		if err := binary.Read(r, binary.BigEndian, &length); err != nil {
			return nil, err
		}
		buf, err := readFieldValueBytes(r, length)
		if err != nil {
			return nil, err
		}
		return buf, nil

	case 'F':
		var length uint32
		if err := binary.Read(r, binary.BigEndian, &length); err != nil {
			return nil, err
		}
		buf, err := readFieldValueBytes(r, length)
		if err != nil {
			return nil, err
		}
		return decodeTableEntries(buf, depth+1)
	}

	return nil, fmt.Errorf("unknown field type: %c", valueType)
}

func readFieldValueBytes(r *bytes.Reader, length uint32) ([]byte, error) {
	if uint64(length) > uint64(r.Len()) {
		return nil, io.ErrUnexpectedEOF
	}
	buf := make([]byte, int(length))
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
