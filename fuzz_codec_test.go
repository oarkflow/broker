package broker

import "testing"

func FuzzWALRecordDecode(f *testing.F) {
	codec, err := NewWALRecordCodec(WALCodecConfig{MaxRecordSize: 1 << 20})
	if err != nil {
		f.Fatal(err)
	}
	seed, err := codec.encode(nil, walRecord{Op: "append_atomic", Entries: []AppendEntry{{Message: Message{ID: "seed", Payload: []byte("payload")}, Destinations: []Destination{{Kind: DestinationQueue, Name: "jobs"}}}}})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	transactionSeed, err := codec.encode(nil, walRecord{Op: "stream_transaction", Offset: OffsetCommit{Stream: "input", ConsumerGroup: "workers", Offset: 2}, ExpectedOffset: 1})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(transactionSeed)
	f.Add([]byte("BWL1"))
	f.Fuzz(func(t *testing.T, data []byte) {
		var record walRecord
		_ = codec.decode(data, &record)
	})
}

func FuzzBinaryMessageDecode(f *testing.F) {
	codec := NewBinaryMessageCodec(WithFrameMaxSize(1 << 20))
	seed, err := codec.EncodeMessage(nil, Message{ID: "seed", Payload: []byte("payload")})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte("FWB1"))
	f.Fuzz(func(t *testing.T, data []byte) {
		var message Message
		_ = codec.DecodeMessage(data, &message)
	})
}
