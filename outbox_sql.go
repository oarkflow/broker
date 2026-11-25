package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// SQLBindStyle selects the parameter placeholder syntax for the built-in SQL
// outbox transaction adapter.
type SQLBindStyle uint8

const (
	// SQLBindQuestionMark is used by SQLite and MySQL drivers.
	SQLBindQuestionMark SQLBindStyle = iota + 1
	// SQLBindDollarNumbered is used by PostgreSQL drivers.
	SQLBindDollarNumbered
)

// NewSQLOutboxEntryTransaction adapts a caller-owned database/sql transaction
// to OutboxEntryTransaction. The transaction must share a database with the
// configured OutboxStateStore, and its fw_outbox_entries table must have the
// columns used below. The helper never commits or rolls back tx.
//
// The portable table layout uses text-compatible columns for id, status,
// claim_token, and data_json; integer columns store timestamps in Unix
// nanoseconds. Use SQLBindQuestionMark for SQLite/MySQL or
// SQLBindDollarNumbered for PostgreSQL.
func NewSQLOutboxEntryTransaction(tx *sql.Tx, style SQLBindStyle) OutboxEntryTransaction {
	return sqlOutboxEntryTransaction{tx: tx, style: style}
}

type sqlOutboxEntryTransaction struct {
	tx    *sql.Tx
	style SQLBindStyle
}

func (t sqlOutboxEntryTransaction) InsertOutboxEntry(ctx context.Context, entry DurableOutboxEntry) error {
	if t.tx == nil || entry.ID == "" {
		return ErrInvalidMessage
	}
	if ctx == nil {
		return ErrInvalidMessage
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	placeholders := "(?,?,?,?,?,?,?,?)"
	switch t.style {
	case SQLBindQuestionMark:
	case SQLBindDollarNumbered:
		placeholders = "($1,$2,$3,$4,$5,$6,$7,$8)"
	default:
		return fmt.Errorf("durable outbox: unsupported SQL bind style %d", t.style)
	}
	query := `INSERT INTO fw_outbox_entries(id,status,claim_token,lease_until,next_attempt_at,created_at,updated_at,data_json) VALUES` + placeholders
	_, err = t.tx.ExecContext(ctx, query,
		entry.ID,
		string(entry.Status),
		entry.ClaimToken,
		outboxUnixNanoOrZero(entry.ClaimUntil),
		outboxUnixNanoOrZero(entry.NextAttemptAt),
		outboxUnixNanoOrZero(entry.CreatedAt),
		outboxUnixNanoOrZero(entry.UpdatedAt),
		string(data),
	)
	return err
}

func outboxUnixNanoOrZero(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixNano()
}
