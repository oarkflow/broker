package client

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/oarkflow/fastworker/broker"
)

func TestRemoteFramePublishAndPoll(t *testing.T) {
	b := broker.MustNew(broker.Config{DefaultQueue: "q", FrameCodec: broker.NewBinaryMessageCodec(broker.WithFrameSigningKey([]byte("wire")))})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())
	if err := b.DeclareQueue(context.Background(), broker.QueueConfig{Name: "q", VisibilityTimeout: time.Second}); err != nil {
		t.Fatal(err)
	}
	admin := broker.AdminServer{Broker: b, InsecureDev: true}
	srv := httptest.NewServer(admin.Handler())
	defer srv.Close()
	c := New(Config{BrokerURL: srv.URL, ConsumerID: "c", Group: "g", FrameCodec: b.FrameCodec(), PollWait: 10 * time.Millisecond})
	_, err := c.PublishFrame(context.Background(), broker.Message{ID: "frame-msg", Topic: "", Queue: "q", Type: "raw", Payload: []byte("payload"), CreatedAt: time.Now(), Status: broker.MessageQueued})
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := c.PollFrames(context.Background(), "q", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].ID != "frame-msg" || string(msgs[0].Payload) != "payload" || msgs[0].LeaseToken == "" {
		t.Fatalf("bad frame poll: %#v", msgs)
	}
}
