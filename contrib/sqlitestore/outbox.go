package sqlitestore

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/oarkflow/broker"
)

// Outbox stores pending messages in the application's SQLite database. EnqueueTx
// makes the application update and intent to publish one database transaction.
// Dispatch is at-least-once: a crash after broker append but before completion
// can retry delivery. Stable idempotency keys let a supporting broker store
// recognize that retry. The application owns db and its durability settings.
type Outbox struct {
	db *sql.DB
}

// OutboxDispatchConfig bounds a dispatch pass. A worker's claim expires after
// LeaseDuration so another worker can recover abandoned work. AppendTimeout must
// be shorter than the lease; stores must honor the supplied context.
type OutboxDispatchConfig struct {
	BatchSize     int
	LeaseDuration time.Duration
	AppendTimeout time.Duration
	// AllowVolatileTarget permits marking delivery complete after an in-memory
	// append. Default false: the target must advertise durable confirmation.
	AllowVolatileTarget bool
}

// NewOutbox creates the outbox schema without taking ownership of db. Call this
// before beginning application transactions. Use a durable SQLite database with
// synchronous=FULL when commits must survive power loss.
func NewOutbox(db *sql.DB) (*Outbox, error) {
	if db == nil {
		return nil, broker.ErrClosed
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS fw_transactional_outbox (
			id TEXT PRIMARY KEY,
			created_at INTEGER NOT NULL,
			data_json BLOB NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			last_error TEXT NOT NULL DEFAULT '',
			lease_token TEXT NOT NULL DEFAULT '',
			lease_until INTEGER NOT NULL DEFAULT 0,
			published_at INTEGER
		)`,
		`CREATE INDEX IF NOT EXISTS idx_fw_transactional_outbox_pending
			ON fw_transactional_outbox(lease_until, created_at, id) WHERE published_at IS NULL`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(context.Background(), stmt); err != nil {
			return nil, err
		}
	}
	return &Outbox{db: db}, nil
}

// EnqueueTx records a stable message ID in an application-owned transaction on
// the same database passed to NewOutbox. Rollback removes the intent along with
// the application update. Reusing an ID is an error, never an overwrite.
// Destinations and message routing must already be resolved; dispatch appends
// directly to the Store and does not execute broker publish hooks or routing.
func (o *Outbox) EnqueueTx(ctx context.Context, tx *sql.Tx, msg broker.Message, dests []broker.Destination) error {
	if tx == nil || msg.ID == "" || len(dests) == 0 {
		return fmt.Errorf("%w: outbox requires a transaction, message ID and destinations", broker.ErrInvalidMessage)
	}
	for _, dest := range dests {
		if dest.Name == "" {
			return fmt.Errorf("%w: empty outbox destination", broker.ErrInvalidMessage)
		}
		switch dest.Kind {
		case broker.DestinationQueue, broker.DestinationDLQ, broker.DestinationTopic, broker.DestinationStream, broker.DestinationArchive:
		default:
			return fmt.Errorf("%w: unsupported outbox destination %q", broker.ErrInvalidMessage, dest.Kind)
		}
	}
	now := time.Now()
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = now
	}
	if msg.IdempotencyKey == "" {
		msg.IdempotencyKey = "outbox:" + msg.ID
	}
	entry := broker.DurableOutboxEntry{ID: msg.ID, Message: msg, Destinations: dests, CreatedAt: now}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO fw_transactional_outbox(id,created_at,data_json) VALUES(?,?,?)`, msg.ID, now.UnixNano(), data)
	return err
}

type outboxClaim struct {
	id       string
	data     []byte
	token    string
	deadline time.Time
}

// Dispatch publishes at most BatchSize entries and returns the number whose
// completion was recorded. Each claim and completion is atomic and fenced by a
// unique token, so an expired worker cannot complete a newer worker's claim.
// Failures remain pending, with their error and attempt count persisted.
func (o *Outbox) Dispatch(ctx context.Context, target broker.Store, cfg OutboxDispatchConfig) (int, error) {
	if target == nil {
		return 0, fmt.Errorf("%w: outbox target store required", broker.ErrInvalidMessage)
	}
	if !cfg.AllowVolatileTarget {
		confirmation, ok := target.(broker.ConfirmationStore)
		if !ok {
			return 0, fmt.Errorf("%w: outbox target must advertise durable confirmation", broker.ErrUnsupportedMode)
		}
		switch confirmation.ConfirmationLevel() {
		case broker.ConfirmPersisted, broker.ConfirmReplicated, broker.ConfirmCommitted:
		default:
			return 0, fmt.Errorf("%w: outbox target does not guarantee durable append", broker.ErrUnsupportedMode)
		}
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.BatchSize > 10000 {
		return 0, fmt.Errorf("%w: outbox batch size exceeds 10000", broker.ErrInvalidMessage)
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}
	if cfg.AppendTimeout <= 0 {
		cfg.AppendTimeout = cfg.LeaseDuration / 2
	}
	if cfg.AppendTimeout <= 0 || cfg.AppendTimeout >= cfg.LeaseDuration {
		return 0, fmt.Errorf("%w: append timeout must be shorter than outbox lease", broker.ErrInvalidMessage)
	}
	completed := 0
	for i := 0; i < cfg.BatchSize; i++ {
		claim, ok, err := o.claim(ctx, cfg.LeaseDuration)
		if err != nil || !ok {
			return completed, err
		}
		var entry broker.DurableOutboxEntry
		err = json.Unmarshal(claim.data, &entry)
		if err == nil {
			appendCtx, cancel := context.WithTimeout(ctx, cfg.AppendTimeout)
			_, err = target.Append(appendCtx, entry.Message, entry.Destinations)
			cancel()
			if errors.Is(err, broker.ErrDuplicateMessage) {
				err = nil
			}
		}
		if err != nil {
			// Stop after failure so this pass cannot reclaim the same failed
			// record repeatedly and starve later work in a tight retry loop.
			releaseErr := o.finish(ctx, claim, err)
			return completed, errors.Join(fmt.Errorf("outbox %q: %w", claim.id, err), releaseErr)
		}
		if err := o.finish(ctx, claim, nil); err != nil {
			return completed, err
		}
		completed++
	}
	return completed, nil
}

func (o *Outbox) claim(ctx context.Context, lease time.Duration) (outboxClaim, bool, error) {
	now := time.Now()
	claim := outboxClaim{token: rand.Text(), deadline: now.Add(lease)}
	// A single UPDATE avoids a read-then-write race between independent workers.
	err := o.db.QueryRowContext(ctx, `UPDATE fw_transactional_outbox
		SET lease_token=?, lease_until=?, attempts=attempts+1
		WHERE id=(SELECT id FROM fw_transactional_outbox
			WHERE published_at IS NULL AND lease_until<=? ORDER BY created_at,id LIMIT 1)
		RETURNING id,data_json`, claim.token, claim.deadline.UnixNano(), now.UnixNano()).Scan(&claim.id, &claim.data)
	if errors.Is(err, sql.ErrNoRows) {
		return outboxClaim{}, false, nil
	}
	return claim, err == nil, err
}

func (o *Outbox) finish(ctx context.Context, claim outboxClaim, publishErr error) error {
	var published any
	lastError := ""
	if publishErr == nil {
		published = time.Now().UnixNano()
	} else {
		lastError = publishErr.Error()
	}
	res, err := o.db.ExecContext(ctx, `UPDATE fw_transactional_outbox
		SET published_at=?, last_error=?, lease_token='', lease_until=0
		WHERE id=? AND lease_token=? AND lease_until>? AND published_at IS NULL`,
		published, lastError, claim.id, claim.token, time.Now().UnixNano())
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err == nil && n != 1 {
		err = broker.ErrLeaseLost
	}
	return err
}

// GetEntry returns persisted dispatch status without transferring ownership of
// the application's database. Published entries remain available for auditing;
// the application may prune them according to its retry/audit retention policy.
func (o *Outbox) GetEntry(ctx context.Context, id string) (broker.DurableOutboxEntry, bool, error) {
	var entry broker.DurableOutboxEntry
	var data []byte
	var published sql.NullInt64
	var leaseUntil int64
	var attempts int
	var lastError string
	err := o.db.QueryRowContext(ctx, `SELECT data_json,attempts,last_error,lease_until,published_at
		FROM fw_transactional_outbox WHERE id=?`, id).Scan(&data, &attempts, &lastError, &leaseUntil, &published)
	if errors.Is(err, sql.ErrNoRows) {
		return entry, false, nil
	}
	if err != nil {
		return entry, false, err
	}
	if err := json.Unmarshal(data, &entry); err != nil {
		return entry, false, err
	}
	entry.Attempts = attempts
	entry.LastError = lastError
	entry.Status = broker.OutboxPending
	if published.Valid {
		at := time.Unix(0, published.Int64)
		entry.PublishedAt = &at
		entry.Status = broker.OutboxPublished
	} else if leaseUntil > time.Now().UnixNano() {
		entry.Status = broker.OutboxInFlight
	} else if entry.LastError != "" {
		entry.Status = broker.OutboxFailed
	}
	return entry, true, nil
}
