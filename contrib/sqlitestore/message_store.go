package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/oarkflow/broker"
)

// MessageStore is a SQLite/database-sql backed broker.Store. It uses the
// broker's in-memory store as the hot execution index and commits an atomic
// JSON snapshot after every mutating operation. That gives embedded and
// single-node standalone deployments a simple durable SQL option while keeping
// the high-throughput file/binary WAL available for heavier production loads.
//
// The store is crash-recoverable: in-flight leased/processing messages are
// restored as queued on startup so they can be redelivered.
type MessageStore struct {
	mu        sync.Mutex
	db        *sql.DB
	mem       *broker.MemoryStore
	committed broker.MemoryStoreSnapshot
	own       bool
}

func (*MessageStore) ConfirmationLevel() broker.ConfirmLevel { return broker.ConfirmPersisted }

func (s *MessageStore) StoreHealth() error {
	if s == nil || s.db == nil {
		return broker.ErrClosed
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.db.PingContext(ctx); err != nil {
		return errors.Join(broker.ErrStorageUnavailable, err)
	}
	return nil
}

// OpenMessageStore opens a persistent message data-plane store using any
// registered SQLite driver name.
func OpenMessageStore(driver, dsn string) (*MessageStore, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	s, err := NewMessageStore(db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	s.own = true
	return s, nil
}

func NewMessageStore(db *sql.DB) (*MessageStore, error) {
	if db == nil {
		return nil, broker.ErrClosed
	}
	s := &MessageStore{db: db, mem: broker.NewMemoryStore()}
	if err := s.init(context.Background()); err != nil {
		return nil, err
	}
	if err := s.load(context.Background()); err != nil {
		return nil, err
	}
	s.committed = s.mem.Snapshot()
	return s, nil
}

func (s *MessageStore) init(ctx context.Context) error {
	// These pragmas are ignored/harmless for SQLite-compatible drivers that do
	// not support all of them; failures are ignored so custom test drivers can be
	// used too.
	_, _ = s.db.ExecContext(ctx, `PRAGMA journal_mode=WAL`)
	_, _ = s.db.ExecContext(ctx, `PRAGMA synchronous=FULL`)
	_, _ = s.db.ExecContext(ctx, `PRAGMA busy_timeout=5000`)
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS fw_message_store_snapshots (id TEXT PRIMARY KEY, updated_at INTEGER NOT NULL, data_json BLOB NOT NULL)`)
	return err
}

func (s *MessageStore) load(ctx context.Context) error {
	var data []byte
	err := s.db.QueryRowContext(ctx, `SELECT data_json FROM fw_message_store_snapshots WHERE id='default'`).Scan(&data)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	var snap broker.MemoryStoreSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	s.mem.Restore(snap)
	return nil
}

func (s *MessageStore) persistLocked(ctx context.Context) error {
	next := s.mem.Snapshot()
	b, err := json.Marshal(next)
	if err != nil {
		s.mem.Restore(s.committed)
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err == nil {
		_, err = tx.ExecContext(ctx, `INSERT INTO fw_message_store_snapshots(id,updated_at,data_json) VALUES('default',?,?) ON CONFLICT(id) DO UPDATE SET updated_at=excluded.updated_at, data_json=excluded.data_json`, time.Now().UnixNano(), b)
	}
	if err == nil {
		err = tx.Commit()
	} else if tx != nil {
		_ = tx.Rollback()
	}
	if err != nil {
		s.mem.Restore(s.committed)
		return err
	}
	s.committed = next
	return nil
}

func (s *MessageStore) DeclareQueue(ctx context.Context, cfg broker.QueueConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.DeclareQueue(ctx, cfg); err != nil {
		return err
	}
	return s.persistLocked(ctx)
}
func (s *MessageStore) DeclareTopic(ctx context.Context, cfg broker.TopicConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.DeclareTopic(ctx, cfg); err != nil {
		return err
	}
	return s.persistLocked(ctx)
}
func (s *MessageStore) DeclareStream(ctx context.Context, cfg broker.StreamConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.DeclareStream(ctx, cfg); err != nil {
		return err
	}
	return s.persistLocked(ctx)
}
func (s *MessageStore) Append(ctx context.Context, msg broker.Message, dests []broker.Destination) ([]broker.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out, err := s.mem.Append(ctx, msg, dests)
	if err != nil {
		s.mem.Restore(s.committed)
		return out, err
	}
	return out, s.persistLocked(ctx)
}
func (s *MessageStore) AppendAtomic(ctx context.Context, entries []broker.AppendEntry) ([]broker.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out, err := s.mem.AppendAtomic(ctx, entries)
	if err != nil {
		return out, err
	}
	return out, s.persistLocked(ctx)
}
func (s *MessageStore) Lease(ctx context.Context, req broker.LeaseRequest) ([]broker.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out, err := s.mem.Lease(ctx, req)
	if err != nil || len(out) == 0 {
		return out, err
	}
	return out, s.persistLocked(ctx)
}
func (s *MessageStore) Ack(ctx context.Context, req broker.AckRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.Ack(ctx, req); err != nil {
		return err
	}
	return s.persistLocked(ctx)
}
func (s *MessageStore) Nack(ctx context.Context, req broker.NackRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.Nack(ctx, req); err != nil {
		return err
	}
	return s.persistLocked(ctx)
}
func (s *MessageStore) ExtendLease(ctx context.Context, req broker.LeaseRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.ExtendLease(ctx, req); err != nil {
		return err
	}
	return s.persistLocked(ctx)
}
func (s *MessageStore) Update(ctx context.Context, msg broker.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.Update(ctx, msg); err != nil {
		return err
	}
	return s.persistLocked(ctx)
}
func (s *MessageStore) Get(ctx context.Context, id string) (broker.Message, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mem.Get(ctx, id)
}
func (s *MessageStore) History(ctx context.Context, id string) ([]broker.MessageHistory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mem.History(ctx, id)
}
func (s *MessageStore) ListQueue(ctx context.Context, name string, limit int) ([]broker.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mem.ListQueue(ctx, name, limit)
}
func (s *MessageStore) ListDLQ(ctx context.Context, name string, limit int) ([]broker.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mem.ListDLQ(ctx, name, limit)
}
func (s *MessageStore) ListTopic(ctx context.Context, name string, limit int) ([]broker.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mem.ListTopic(ctx, name, limit)
}
func (s *MessageStore) ReadTopic(ctx context.Context, name, afterID string, limit int) ([]broker.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mem.ReadTopic(ctx, name, afterID, limit)
}
func (s *MessageStore) Move(ctx context.Context, req broker.MoveRequest) (broker.MoveResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out, err := s.mem.Move(ctx, req)
	if err != nil {
		return out, err
	}
	return out, s.persistLocked(ctx)
}
func (s *MessageStore) PauseQueue(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.PauseQueue(ctx, name); err != nil {
		return err
	}
	return s.persistLocked(ctx)
}
func (s *MessageStore) ResumeQueue(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.ResumeQueue(ctx, name); err != nil {
		return err
	}
	return s.persistLocked(ctx)
}
func (s *MessageStore) PurgeQueue(ctx context.Context, name string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, err := s.mem.PurgeQueue(ctx, name)
	if err != nil {
		return n, err
	}
	return n, s.persistLocked(ctx)
}
func (s *MessageStore) ReadStream(ctx context.Context, req broker.StreamReadRequest) ([]broker.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mem.ReadStream(ctx, req)
}
func (s *MessageStore) CommitOffset(ctx context.Context, commit broker.OffsetCommit) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.CommitOffset(ctx, commit); err != nil {
		return err
	}
	return s.persistLocked(ctx)
}
func (s *MessageStore) ConsumerOffset(ctx context.Context, commit broker.OffsetCommit) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mem.ConsumerOffset(ctx, commit)
}
func (s *MessageStore) StreamLagContext(ctx context.Context, stream, group, tenant string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mem.StreamLagContext(ctx, stream, group, tenant)
}
func (s *MessageStore) StreamLagAllContext(ctx context.Context, stream, tenant string) (map[string]uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mem.StreamLagAllContext(ctx, stream, tenant)
}
func (s *MessageStore) StreamHeadOffsetContext(ctx context.Context, stream, tenant string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mem.StreamHeadOffsetContext(ctx, stream, tenant)
}
func (s *MessageStore) Stats(ctx context.Context) (broker.StoreStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mem.Stats(ctx)
}
func (s *MessageStore) SweepLeases(ctx context.Context, now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, err := s.mem.SweepLeases(ctx, now)
	if err != nil || n == 0 {
		return n, err
	}
	return n, s.persistLocked(ctx)
}
func (s *MessageStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.mem.Close()
	if s.own {
		return s.db.Close()
	}
	return nil
}

var _ broker.Store = (*MessageStore)(nil)
var _ broker.AtomicStore = (*MessageStore)(nil)
