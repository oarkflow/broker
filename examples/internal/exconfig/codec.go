package exconfig

import "github.com/oarkflow/fastworker/broker"

func FrameCodec(node *Node) *broker.BinaryMessageCodec {
	if node == nil {
		return broker.NewBinaryMessageCodec()
	}
	cb := node.Block("codec", "fwb1")
	if cb == nil {
		blocks := node.Blocks("codec")
		if len(blocks) > 0 {
			cb = blocks[0]
		}
	}
	if cb == nil {
		return broker.NewBinaryMessageCodec()
	}
	var opts []broker.BinaryMessageCodecOption
	if n := int(ParseBytes(cb.String("max_frame_size", "128MB"), 128<<20)); n > 0 {
		opts = append(opts, broker.WithFrameMaxSize(n))
	}
	if n := int(ParseBytes(cb.String("compress_threshold", "0"), 0)); n > 0 {
		opts = append(opts, broker.WithFrameCompressionThreshold(n))
	}
	if key := cb.String("encryption_key", ""); key != "" {
		opts = append(opts, broker.WithFrameEncryptionKey([]byte(key)))
	}
	if key := cb.String("signing_key", ""); key != "" {
		opts = append(opts, broker.WithFrameSigningKey([]byte(key)))
	}
	return broker.NewBinaryMessageCodec(opts...)
}

func FrameCodecOptions(node *Node) []broker.BinaryMessageCodecOption {
	if node == nil {
		return nil
	}
	cb := node.Block("codec", "fwb1")
	if cb == nil {
		blocks := node.Blocks("codec")
		if len(blocks) > 0 {
			cb = blocks[0]
		}
	}
	if cb == nil {
		return nil
	}
	var opts []broker.BinaryMessageCodecOption
	if n := int(ParseBytes(cb.String("max_frame_size", "128MB"), 128<<20)); n > 0 {
		opts = append(opts, broker.WithFrameMaxSize(n))
	}
	if n := int(ParseBytes(cb.String("compress_threshold", "0"), 0)); n > 0 {
		opts = append(opts, broker.WithFrameCompressionThreshold(n))
	}
	if key := cb.String("encryption_key", ""); key != "" {
		opts = append(opts, broker.WithFrameEncryptionKey([]byte(key)))
	}
	if key := cb.String("signing_key", ""); key != "" {
		opts = append(opts, broker.WithFrameSigningKey([]byte(key)))
	}
	return opts
}
