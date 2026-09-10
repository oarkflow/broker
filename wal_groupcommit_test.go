package broker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestGroupCommitFencesAfterStorageFailure(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "wal-*.log")
	if err != nil {
		t.Fatal(err)
	}
	writer := newGroupCommitWriter(file)
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	first := writer.write([]byte("first"))
	if !errors.Is(first, ErrStorageUnavailable) {
		t.Fatalf("first write error=%v", first)
	}
	second := writer.write([]byte("second"))
	if !errors.Is(second, ErrStorageUnavailable) {
		t.Fatalf("writer did not remain fenced: %v", second)
	}
	if err := writer.close(); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("close did not report fenced error: %v", err)
	}
}

func TestReadinessFailsAfterWALIsFenced(t *testing.T) {
	store, err := NewBinaryFileWALStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	broker := MustNew(Config{Store: store})
	if err := broker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer broker.Stop(context.Background())
	if err := store.file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(context.Background(), Message{ID: "fault"}, []Destination{{Kind: DestinationQueue, Name: "jobs"}}); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("append error=%v", err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	AdminServer{Broker: broker}.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}

func TestDurableAppendFailureDoesNotExposeGhostMessage(t *testing.T) {
	tests := []struct {
		name string
		open func(string) (Store, *os.File, error)
	}{
		{
			name: "binary",
			open: func(dir string) (Store, *os.File, error) {
				store, err := NewBinaryFileWALStore(dir)
				if err != nil {
					return nil, nil, err
				}
				return store, store.file, nil
			},
		},
		{
			name: "json",
			open: func(dir string) (Store, *os.File, error) {
				store, err := NewFileWALStore(dir)
				if err != nil {
					return nil, nil, err
				}
				return store, store.file, nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, file, err := test.open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			_, err = store.Append(context.Background(), Message{ID: "ghost"}, []Destination{{Kind: DestinationQueue, Name: "jobs"}})
			if !errors.Is(err, ErrStorageUnavailable) {
				t.Fatalf("append error=%v, want storage unavailable", err)
			}
			if _, ok, getErr := store.Get(context.Background(), "ghost"); getErr != nil || ok {
				t.Fatalf("failed append became visible: ok=%t err=%v", ok, getErr)
			}
			_ = store.Close()
		})
	}
}
