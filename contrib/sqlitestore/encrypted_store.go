package sqlitestore

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/oarkflow/broker"
)

// EncryptedMessageStore wraps a MessageStore with AES-GCM encryption at rest.
// All message payloads and metadata are encrypted before being written to SQLite
// and decrypted on read. The encryption key must be 16, 24, or 32 bytes for
// AES-128, AES-192, or AES-256 respectively.
type EncryptedMessageStore struct {
	mu    sync.Mutex
	db    *sql.DB
	mem   *broker.MemoryStore
	own   bool
	key   []byte
	gcm   cipher.AEAD
	closed bool
}

// OpenEncryptedMessageStore opens a persistent encrypted message store.
func OpenEncryptedMessageStore(driver, dsn string, encryptionKey []byte) (*EncryptedMessageStore, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	s, err := NewEncryptedMessageStore(db, encryptionKey)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	s.own = true
	return s, nil
}

// NewEncryptedMessageStore creates a new encrypted message store.
func NewEncryptedMessageStore(db *sql.DB, encryptionKey []byte) (*EncryptedMessageStore, error) {
	if db == nil {
		return nil, broker.ErrClosed
	}
	if len(encryptionKey) == 0 {
		return nil, errors.New("encryption key required")
	}
	block, err := aes.NewCipher(encryptionKey)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	s := &EncryptedMessageStore{db: db, mem: broker.NewMemoryStore(), key: encryptionKey, gcm: gcm}
	if err := s.init(); err != nil {
		return nil, err
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *EncryptedMessageStore) init() error {
	_, _ = s.db.Exec(`PRAGMA journal_mode=WAL`)
	_, _ = s.db.Exec(`PRAGMA synchronous=NORMAL`)
	_, _ = s.db.Exec(`PRAGMA busy_timeout=5000`)
	_, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS fw_encrypted_message_store (id TEXT PRIMARY KEY, updated_at INTEGER NOT NULL, nonce BLOB NOT NULL, ciphertext BLOB NOT NULL)`)
	return err
}

func (s *EncryptedMessageStore) load() error {
	var nonce, ciphertext []byte
	err := s.db.QueryRow(`SELECT nonce, ciphertext FROM fw_encrypted_message_store WHERE id='default'`).Scan(&nonce, &ciphertext)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	plaintext, err := s.gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return err
	}
	var snap broker.MemoryStoreSnapshot
	if err := json.Unmarshal(plaintext, &snap); err != nil {
		return err
	}
	s.mem.Restore(snap)
	return nil
}

func (s *EncryptedMessageStore) persistLocked() error {
	b, err := json.Marshal(s.mem.Snapshot())
	if err != nil {
		return err
	}
	nonce := make([]byte, s.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return err
	}
	ciphertext := s.gcm.Seal(nil, nonce, b, nil)
	_, err = s.db.Exec(`INSERT INTO fw_encrypted_message_store(id,updated_at,nonce,ciphertext) VALUES('default',?, ?, ?) ON CONFLICT(id) DO UPDATE SET updated_at=excluded.updated_at, nonce=excluded.nonce, ciphertext=excluded.ciphertext`, time.Now().UnixNano(), nonce, ciphertext)
	return err
}

func (s *EncryptedMessageStore) DeclareQueue(ctx context.Context, cfg broker.QueueConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.DeclareQueue(ctx, cfg); err != nil {
		return err
	}
	return s.persistLocked()
}
func (s *EncryptedMessageStore) DeclareTopic(ctx context.Context, cfg broker.TopicConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.DeclareTopic(ctx, cfg); err != nil {
		return err
	}
	return s.persistLocked()
}
func (s *EncryptedMessageStore) DeclareStream(ctx context.Context, cfg broker.StreamConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.DeclareStream(ctx, cfg); err != nil {
		return err
	}
	return s.persistLocked()
}
func (s *EncryptedMessageStore) Append(ctx context.Context, msg broker.Message, dests []broker.Destination) ([]broker.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out, err := s.mem.Append(ctx, msg, dests)
	if err != nil {
		return out, err
	}
	return out, s.persistLocked()
}
func (s *EncryptedMessageStore) AppendAtomic(ctx context.Context, entries []broker.AppendEntry) ([]broker.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out, err := s.mem.AppendAtomic(ctx, entries)
	if err != nil {
		return out, err
	}
	return out, s.persistLocked()
}
func (s *EncryptedMessageStore) Lease(ctx context.Context, req broker.LeaseRequest) ([]broker.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out, err := s.mem.Lease(ctx, req)
	if err != nil || len(out) == 0 {
		return out, err
	}
	return out, s.persistLocked()
}
func (s *EncryptedMessageStore) Ack(ctx context.Context, req broker.AckRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.Ack(ctx, req); err != nil {
		return err
	}
	return s.persistLocked()
}
func (s *EncryptedMessageStore) Nack(ctx context.Context, req broker.NackRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.Nack(ctx, req); err != nil {
		return err
	}
	return s.persistLocked()
}
func (s *EncryptedMessageStore) ExtendLease(ctx context.Context, req broker.LeaseRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.ExtendLease(ctx, req); err != nil {
		return err
	}
	return s.persistLocked()
}
func (s *EncryptedMessageStore) Update(ctx context.Context, msg broker.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.Update(ctx, msg); err != nil {
		return err
	}
	return s.persistLocked()
}
func (s *EncryptedMessageStore) Get(ctx context.Context, id string) (broker.Message, bool, error) {
	return s.mem.Get(ctx, id)
}
func (s *EncryptedMessageStore) History(ctx context.Context, id string) ([]broker.MessageHistory, error) {
	return s.mem.History(ctx, id)
}
func (s *EncryptedMessageStore) ListQueue(ctx context.Context, name string, limit int) ([]broker.Message, error) {
	return s.mem.ListQueue(ctx, name, limit)
}
func (s *EncryptedMessageStore) ListDLQ(ctx context.Context, name string, limit int) ([]broker.Message, error) {
	return s.mem.ListDLQ(ctx, name, limit)
}
func (s *EncryptedMessageStore) Move(ctx context.Context, req broker.MoveRequest) (broker.MoveResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out, err := s.mem.Move(ctx, req)
	if err != nil {
		return out, err
	}
	return out, s.persistLocked()
}
func (s *EncryptedMessageStore) PauseQueue(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.PauseQueue(ctx, name); err != nil {
		return err
	}
	return s.persistLocked()
}
func (s *EncryptedMessageStore) ResumeQueue(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.ResumeQueue(ctx, name); err != nil {
		return err
	}
	return s.persistLocked()
}
func (s *EncryptedMessageStore) PurgeQueue(ctx context.Context, name string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, err := s.mem.PurgeQueue(ctx, name)
	if err != nil {
		return n, err
	}
	return n, s.persistLocked()
}
func (s *EncryptedMessageStore) ReadStream(ctx context.Context, req broker.StreamReadRequest) ([]broker.Message, error) {
	return s.mem.ReadStream(ctx, req)
}
func (s *EncryptedMessageStore) CommitOffset(ctx context.Context, commit broker.OffsetCommit) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.CommitOffset(ctx, commit); err != nil {
		return err
	}
	return s.persistLocked()
}
func (s *EncryptedMessageStore) ConsumerOffset(ctx context.Context, commit broker.OffsetCommit) (uint64, error) {
	return s.mem.ConsumerOffset(ctx, commit)
}
func (s *EncryptedMessageStore) Stats(ctx context.Context) (broker.StoreStats, error) {
	return s.mem.Stats(ctx)
}
func (s *EncryptedMessageStore) SweepLeases(ctx context.Context, now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, err := s.mem.SweepLeases(ctx, now)
	if err != nil || n == 0 {
		return n, err
	}
	return n, s.persistLocked()
}
func (s *EncryptedMessageStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.own {
		return s.db.Close()
	}
	return nil
}
