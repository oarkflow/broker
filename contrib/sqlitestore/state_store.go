// Package sqlitestore provides a database/sql-backed persistent state store for
// broker/task/consumer/publisher control-plane state. It does not import a
// SQLite driver directly, so the core module remains dependency-light. Open a
// *sql.DB with your preferred SQLite driver and pass it to NewStateStore.
package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/oarkflow/broker"
)

type StateStore struct {
	db  *sql.DB
	own bool
}

func unixNanoOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func Open(driver, dsn string) (*StateStore, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	s, err := NewStateStore(db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	s.own = true
	return s, nil
}

func NewStateStore(db *sql.DB) (*StateStore, error) {
	if db == nil {
		return nil, broker.ErrClosed
	}
	s := &StateStore{db: db}
	if err := s.init(context.Background()); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *StateStore) init(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS fw_broker_states (name TEXT PRIMARY KEY, node_id TEXT, mode TEXT, state TEXT, updated_at INTEGER NOT NULL, metadata_json BLOB)`,
		`CREATE TABLE IF NOT EXISTS fw_tasks (id TEXT PRIMARY KEY, status TEXT, updated_at INTEGER NOT NULL, data_json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS idx_fw_tasks_status_updated ON fw_tasks(status, updated_at DESC)`,
		`CREATE TABLE IF NOT EXISTS fw_consumers (id TEXT PRIMARY KEY, state TEXT, updated_at INTEGER NOT NULL, data_json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS idx_fw_consumers_state ON fw_consumers(state)`,
		`CREATE TABLE IF NOT EXISTS fw_consumer_events (seq INTEGER PRIMARY KEY AUTOINCREMENT, consumer_id TEXT NOT NULL, event_id TEXT, created_at INTEGER NOT NULL, data_json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS idx_fw_consumer_events_consumer_seq ON fw_consumer_events(consumer_id, seq)`,
		`CREATE INDEX IF NOT EXISTS idx_fw_consumer_events_event ON fw_consumer_events(consumer_id, event_id)`,
		`CREATE TABLE IF NOT EXISTS fw_publisher_records (id TEXT PRIMARY KEY, operation TEXT, status TEXT, updated_at INTEGER NOT NULL, data_json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS idx_fw_publisher_records_updated ON fw_publisher_records(updated_at DESC)`,
		`CREATE TABLE IF NOT EXISTS fw_mqtt_sessions (session_key TEXT PRIMARY KEY, client_id TEXT NOT NULL, updated_at INTEGER NOT NULL, data_json BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS fw_mqtt_retained (tenant_id TEXT NOT NULL, topic TEXT NOT NULL, qos INTEGER NOT NULL, updated_at INTEGER NOT NULL, payload BLOB NOT NULL, PRIMARY KEY(tenant_id, topic))`,
		`CREATE INDEX IF NOT EXISTS idx_fw_mqtt_retained_tenant_topic ON fw_mqtt_retained(tenant_id, topic)`,
		`CREATE TABLE IF NOT EXISTS fw_outbox_entries (id TEXT PRIMARY KEY, status TEXT NOT NULL, claim_token TEXT NOT NULL DEFAULT '', lease_until INTEGER NOT NULL DEFAULT 0, next_attempt_at INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, data_json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS idx_fw_outbox_created ON fw_outbox_entries(created_at, id)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	if err := s.ensureOutboxStatusColumn(ctx); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_fw_outbox_dispatchable ON fw_outbox_entries(status, lease_until, next_attempt_at, created_at, id)`)
	return err
}

func (s *StateStore) PutMQTTSession(ctx context.Context, session broker.MQTTSession) error {
	if session.Key == "" || session.ClientID == "" {
		return broker.ErrInvalidMessage
	}
	data, err := json.Marshal(session)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO fw_mqtt_sessions(session_key,client_id,updated_at,data_json) VALUES(?,?,?,?) ON CONFLICT(session_key) DO UPDATE SET client_id=excluded.client_id, updated_at=excluded.updated_at, data_json=excluded.data_json`, session.Key, session.ClientID, unixNanoOrZero(session.UpdatedAt), data)
	return err
}

func (s *StateStore) GetMQTTSession(ctx context.Context, key string) (broker.MQTTSession, bool, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx, `SELECT data_json FROM fw_mqtt_sessions WHERE session_key=?`, key).Scan(&data)
	if err == sql.ErrNoRows {
		return broker.MQTTSession{}, false, nil
	}
	if err != nil {
		return broker.MQTTSession{}, false, err
	}
	var session broker.MQTTSession
	if err := json.Unmarshal(data, &session); err != nil {
		return broker.MQTTSession{}, false, err
	}
	return session, true, nil
}

func (s *StateStore) DeleteMQTTSession(ctx context.Context, key string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM fw_mqtt_sessions WHERE session_key=?`, key)
	return err
}

func (s *StateStore) PutMQTTRetained(ctx context.Context, message broker.MQTTRetainedMessage) error {
	if message.Topic == "" || message.QoS > 2 {
		return broker.ErrInvalidMessage
	}
	tenant, err := broker.ResolveTenant(ctx, message.TenantID)
	if err != nil {
		return err
	}
	if len(message.Payload) == 0 {
		return s.DeleteMQTTRetained(ctx, tenant, message.Topic)
	}
	if message.UpdatedAt.IsZero() {
		message.UpdatedAt = time.Now()
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO fw_mqtt_retained(tenant_id,topic,qos,updated_at,payload) VALUES(?,?,?,?,?) ON CONFLICT(tenant_id,topic) DO UPDATE SET qos=excluded.qos, updated_at=excluded.updated_at, payload=excluded.payload`, tenant, message.Topic, message.QoS, message.UpdatedAt.UnixNano(), message.Payload)
	return err
}

func (s *StateStore) GetMQTTRetained(ctx context.Context, tenant, topic string) (broker.MQTTRetainedMessage, bool, error) {
	resolved, err := broker.ResolveTenant(ctx, tenant)
	if err != nil {
		return broker.MQTTRetainedMessage{}, false, err
	}
	var message broker.MQTTRetainedMessage
	var updated int64
	err = s.db.QueryRowContext(ctx, `SELECT tenant_id,topic,qos,updated_at,payload FROM fw_mqtt_retained WHERE tenant_id=? AND topic=?`, resolved, topic).Scan(&message.TenantID, &message.Topic, &message.QoS, &updated, &message.Payload)
	if err == sql.ErrNoRows {
		return broker.MQTTRetainedMessage{}, false, nil
	}
	if err != nil {
		return broker.MQTTRetainedMessage{}, false, err
	}
	message.UpdatedAt = time.Unix(0, updated)
	if !broker.TenantAllowed(ctx, message.TenantID) {
		return broker.MQTTRetainedMessage{}, false, broker.ErrUnauthorized
	}
	return message, true, nil
}

func (s *StateStore) ListMQTTRetained(ctx context.Context, tenant string) ([]broker.MQTTRetainedMessage, error) {
	resolved, err := broker.ResolveTenant(ctx, tenant)
	if err != nil {
		return nil, err
	}
	query := `SELECT tenant_id,topic,qos,updated_at,payload FROM fw_mqtt_retained`
	args := []any{}
	if tenant != "" || broker.HasRestrictedTenantScope(ctx) {
		query += ` WHERE tenant_id=?`
		args = append(args, resolved)
	}
	query += ` ORDER BY tenant_id,topic`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]broker.MQTTRetainedMessage, 0)
	for rows.Next() {
		var message broker.MQTTRetainedMessage
		var updated int64
		if err := rows.Scan(&message.TenantID, &message.Topic, &message.QoS, &updated, &message.Payload); err != nil {
			return nil, err
		}
		if broker.TenantAllowed(ctx, message.TenantID) {
			message.UpdatedAt = time.Unix(0, updated)
			out = append(out, message)
		}
	}
	return out, rows.Err()
}

func (s *StateStore) ListMQTTRetainedPage(ctx context.Context, tenant, afterTenant, afterTopic string, limit int) ([]broker.MQTTRetainedMessage, error) {
	if limit <= 0 {
		return nil, broker.ErrInvalidMessage
	}
	resolved, err := broker.ResolveTenant(ctx, tenant)
	if err != nil {
		return nil, err
	}
	query := `SELECT tenant_id,topic,qos,updated_at,payload FROM fw_mqtt_retained`
	args := []any{}
	if tenant != "" || broker.HasRestrictedTenantScope(ctx) {
		query += ` WHERE tenant_id=?`
		args = append(args, resolved)
		if afterTopic != "" {
			query += ` AND topic>?`
			args = append(args, afterTopic)
		}
	} else if afterTenant != "" || afterTopic != "" {
		query += ` WHERE tenant_id>? OR (tenant_id=? AND topic>?)`
		args = append(args, afterTenant, afterTenant, afterTopic)
	}
	query += ` ORDER BY tenant_id,topic LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]broker.MQTTRetainedMessage, 0, limit)
	for rows.Next() {
		var message broker.MQTTRetainedMessage
		var updated int64
		if err := rows.Scan(&message.TenantID, &message.Topic, &message.QoS, &updated, &message.Payload); err != nil {
			return nil, err
		}
		if broker.TenantAllowed(ctx, message.TenantID) {
			message.UpdatedAt = time.Unix(0, updated)
			out = append(out, message)
		}
	}
	return out, rows.Err()
}

func (s *StateStore) DeleteMQTTRetained(ctx context.Context, tenant, topic string) error {
	resolved, err := broker.ResolveTenant(ctx, tenant)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `DELETE FROM fw_mqtt_retained WHERE tenant_id=? AND topic=?`, resolved, topic)
	return err
}

func (s *StateStore) ensureOutboxStatusColumn(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(fw_outbox_entries)`)
	if err != nil {
		return err
	}
	columns := map[string]bool{}
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, kind string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultValue, &primaryKey); err != nil {
			_ = rows.Close()
			return err
		}
		columns[name] = true
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if columns["status"] && columns["claim_token"] && columns["lease_until"] && columns["next_attempt_at"] {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if !columns["status"] {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE fw_outbox_entries ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'`); err != nil {
			return err
		}
	}
	if !columns["claim_token"] {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE fw_outbox_entries ADD COLUMN claim_token TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	if !columns["lease_until"] {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE fw_outbox_entries ADD COLUMN lease_until INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
	}
	if !columns["next_attempt_at"] {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE fw_outbox_entries ADD COLUMN next_attempt_at INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
	}
	if !columns["status"] {
		rows, err := tx.QueryContext(ctx, `SELECT id,data_json FROM fw_outbox_entries`)
		if err != nil {
			return err
		}
		type statusRecord struct {
			id     string
			status string
		}
		var updates []statusRecord
		for rows.Next() {
			var id string
			var data []byte
			if err := rows.Scan(&id, &data); err != nil {
				_ = rows.Close()
				return err
			}
			var entry broker.DurableOutboxEntry
			if err := json.Unmarshal(data, &entry); err != nil {
				_ = rows.Close()
				return err
			}
			updates = append(updates, statusRecord{id: id, status: string(entry.Status)})
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, update := range updates {
			if _, err := tx.ExecContext(ctx, `UPDATE fw_outbox_entries SET status=? WHERE id=?`, update.status, update.id); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// PutOutboxEntry durably stores an outbox entry. The primary key makes repeated
// state updates for the same message idempotent.
func (s *StateStore) PutOutboxEntry(ctx context.Context, entry broker.DurableOutboxEntry) error {
	if entry.ID == "" {
		return broker.ErrInvalidMessage
	}
	b, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO fw_outbox_entries(id,status,claim_token,lease_until,next_attempt_at,created_at,updated_at,data_json) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status, claim_token=excluded.claim_token, lease_until=excluded.lease_until, next_attempt_at=excluded.next_attempt_at, updated_at=excluded.updated_at, data_json=excluded.data_json`, entry.ID, string(entry.Status), entry.ClaimToken, unixNanoOrZero(entry.ClaimUntil), unixNanoOrZero(entry.NextAttemptAt), entry.CreatedAt.UnixNano(), entry.UpdatedAt.UnixNano(), b)
	return err
}

// OutboxTransaction adapts a caller-owned SQL transaction for atomic business
// data plus outbox inserts. The transaction must use the same database as this
// StateStore, and remains owned by the caller.
func (s *StateStore) OutboxTransaction(tx *sql.Tx) broker.OutboxEntryTransaction {
	return stateOutboxTx{tx: tx}
}

type stateOutboxTx struct{ tx *sql.Tx }

func (t stateOutboxTx) InsertOutboxEntry(ctx context.Context, entry broker.DurableOutboxEntry) error {
	if t.tx == nil || entry.ID == "" {
		return broker.ErrInvalidMessage
	}
	b, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = t.tx.ExecContext(ctx, `INSERT INTO fw_outbox_entries(id,status,claim_token,lease_until,next_attempt_at,created_at,updated_at,data_json) VALUES(?,?,?,?,?,?,?,?)`, entry.ID, string(entry.Status), entry.ClaimToken, unixNanoOrZero(entry.ClaimUntil), unixNanoOrZero(entry.NextAttemptAt), entry.CreatedAt.UnixNano(), entry.UpdatedAt.UnixNano(), b)
	return err
}

func (s *StateStore) GetOutboxEntry(ctx context.Context, id string) (broker.DurableOutboxEntry, bool, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx, `SELECT data_json FROM fw_outbox_entries WHERE id=?`, id).Scan(&data)
	if err == sql.ErrNoRows {
		return broker.DurableOutboxEntry{}, false, nil
	}
	if err != nil {
		return broker.DurableOutboxEntry{}, false, err
	}
	var entry broker.DurableOutboxEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return broker.DurableOutboxEntry{}, false, err
	}
	return entry, true, nil
}

func (s *StateStore) ListOutboxEntries(ctx context.Context, limit int) ([]broker.DurableOutboxEntry, error) {
	q := `SELECT data_json FROM fw_outbox_entries ORDER BY created_at, id`
	args := []any{}
	if limit > 0 {
		q += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []broker.DurableOutboxEntry
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var entry broker.DurableOutboxEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			return nil, err
		}
		out = append(out, entry)
	}
	return out, rows.Err()
}

func (s *StateStore) ListDispatchableOutboxEntries(ctx context.Context, limit int) ([]broker.DurableOutboxEntry, error) {
	q := `SELECT data_json FROM fw_outbox_entries WHERE status=? OR (status=? AND next_attempt_at<=?) ORDER BY created_at, id`
	args := []any{string(broker.OutboxPending), string(broker.OutboxFailed), time.Now().UnixNano()}
	if limit > 0 {
		q += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []broker.DurableOutboxEntry
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var entry broker.DurableOutboxEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			return nil, err
		}
		out = append(out, entry)
	}
	return out, rows.Err()
}

func (s *StateStore) ClaimOutboxEntries(ctx context.Context, token string, limit int, lease time.Duration) ([]broker.DurableOutboxEntry, error) {
	if token == "" || limit <= 0 || lease <= 0 {
		return nil, broker.ErrInvalidMessage
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	now := time.Now()
	rows, err := tx.QueryContext(ctx, `SELECT id,data_json FROM fw_outbox_entries WHERE status=? OR (status=? AND next_attempt_at<=?) OR (status=? AND lease_until<=? AND next_attempt_at<=?) ORDER BY created_at,id LIMIT ?`, string(broker.OutboxPending), string(broker.OutboxFailed), now.UnixNano(), string(broker.OutboxInFlight), now.UnixNano(), now.UnixNano(), limit)
	if err != nil {
		return nil, err
	}
	type candidate struct {
		id    string
		entry broker.DurableOutboxEntry
	}
	var candidates []candidate
	for rows.Next() {
		var id string
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			_ = rows.Close()
			return nil, err
		}
		var entry broker.DurableOutboxEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			_ = rows.Close()
			return nil, err
		}
		candidates = append(candidates, candidate{id: id, entry: entry})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	claimed := make([]broker.DurableOutboxEntry, 0, len(candidates))
	leaseUntil := now.Add(lease)
	for _, candidate := range candidates {
		entry := candidate.entry
		entry.Status = broker.OutboxInFlight
		entry.ClaimToken = token
		entry.ClaimUntil = leaseUntil
		entry.UpdatedAt = now
		data, err := json.Marshal(entry)
		if err != nil {
			return nil, err
		}
		res, err := tx.ExecContext(ctx, `UPDATE fw_outbox_entries SET status=?,claim_token=?,lease_until=?,updated_at=?,data_json=? WHERE id=? AND (status=? OR (status=? AND next_attempt_at<=?) OR (status=? AND lease_until<=? AND next_attempt_at<=?))`, string(entry.Status), token, leaseUntil.UnixNano(), now.UnixNano(), data, candidate.id, string(broker.OutboxPending), string(broker.OutboxFailed), now.UnixNano(), string(broker.OutboxInFlight), now.UnixNano(), now.UnixNano())
		if err != nil {
			return nil, err
		}
		updated, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		if updated == 1 {
			claimed = append(claimed, entry)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return claimed, nil
}

func (s *StateStore) UpdateClaimedOutboxEntry(ctx context.Context, token string, entry broker.DurableOutboxEntry) error {
	if token == "" || entry.ID == "" {
		return broker.ErrInvalidMessage
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `UPDATE fw_outbox_entries SET status=?,claim_token=?,lease_until=?,next_attempt_at=?,updated_at=?,data_json=? WHERE id=? AND status=? AND claim_token=?`, string(entry.Status), entry.ClaimToken, unixNanoOrZero(entry.ClaimUntil), unixNanoOrZero(entry.NextAttemptAt), entry.UpdatedAt.UnixNano(), data, entry.ID, string(broker.OutboxInFlight), token)
	if err != nil {
		return err
	}
	updated, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if updated != 1 {
		return broker.ErrOutboxClaimLost
	}
	return nil
}

func (s *StateStore) PruneCompletedOutboxEntries(ctx context.Context, before time.Time, limit int) ([]string, error) {
	if before.IsZero() || limit <= 0 {
		return nil, broker.ErrInvalidMessage
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id FROM fw_outbox_entries WHERE status IN (?,?) AND updated_at<? ORDER BY updated_at,id LIMIT ?`, string(broker.OutboxPublished), string(broker.OutboxDeadLetter), before.UnixNano(), limit)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, `DELETE FROM fw_outbox_entries WHERE id=? AND status IN (?,?)`, id, string(broker.OutboxPublished), string(broker.OutboxDeadLetter)); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return ids, nil
}

func (s *StateStore) PutBrokerState(ctx context.Context, rec broker.BrokerStateRecord) error {
	if rec.UpdatedAt.IsZero() {
		rec.UpdatedAt = time.Now()
	}
	meta, _ := json.Marshal(rec.Metadata)
	_, err := s.db.ExecContext(ctx, `INSERT INTO fw_broker_states(name,node_id,mode,state,updated_at,metadata_json) VALUES(?,?,?,?,?,?) ON CONFLICT(name) DO UPDATE SET node_id=excluded.node_id, mode=excluded.mode, state=excluded.state, updated_at=excluded.updated_at, metadata_json=excluded.metadata_json`, rec.Name, rec.NodeID, string(rec.Mode), string(rec.State), rec.UpdatedAt.UnixNano(), meta)
	return err
}
func (s *StateStore) GetBrokerState(ctx context.Context, name string) (broker.BrokerStateRecord, bool, error) {
	var rec broker.BrokerStateRecord
	var mode, state string
	var updated int64
	var meta []byte
	err := s.db.QueryRowContext(ctx, `SELECT name,node_id,mode,state,updated_at,metadata_json FROM fw_broker_states WHERE name=?`, name).Scan(&rec.Name, &rec.NodeID, &mode, &state, &updated, &meta)
	if err == sql.ErrNoRows {
		return rec, false, nil
	}
	if err != nil {
		return rec, false, err
	}
	rec.Mode = broker.Mode(mode)
	rec.State = broker.BrokerState(state)
	rec.UpdatedAt = time.Unix(0, updated)
	_ = json.Unmarshal(meta, &rec.Metadata)
	return rec, true, nil
}
func (s *StateStore) PutTask(ctx context.Context, t broker.TaskInfo) error {
	if t.UpdatedAt.IsZero() {
		t.UpdatedAt = time.Now()
	}
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO fw_tasks(id,status,updated_at,data_json) VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status, updated_at=excluded.updated_at, data_json=excluded.data_json`, t.ID, string(t.Status), t.UpdatedAt.UnixNano(), b)
	return err
}
func (s *StateStore) GetTask(ctx context.Context, id string) (broker.TaskInfo, bool, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx, `SELECT data_json FROM fw_tasks WHERE id=?`, id).Scan(&data)
	if err == sql.ErrNoRows {
		return broker.TaskInfo{}, false, nil
	}
	if err != nil {
		return broker.TaskInfo{}, false, err
	}
	var t broker.TaskInfo
	if err := json.Unmarshal(data, &t); err != nil {
		return t, false, err
	}
	return t, true, nil
}
func (s *StateStore) ListTasks(ctx context.Context, st broker.TaskStatus, limit int) ([]broker.TaskInfo, error) {
	q := `SELECT data_json FROM fw_tasks`
	args := []any{}
	if st != "" {
		q += ` WHERE status=?`
		args = append(args, string(st))
	}
	q += ` ORDER BY updated_at DESC`
	if limit > 0 {
		q += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []broker.TaskInfo
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var t broker.TaskInfo
		if err := json.Unmarshal(data, &t); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
func (s *StateStore) DeleteTask(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM fw_tasks WHERE id=?`, id)
	return err
}
func (s *StateStore) PutConsumer(ctx context.Context, c broker.ConsumerInfo) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	updated := c.LastHeartbeat
	if updated.IsZero() {
		updated = time.Now()
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO fw_consumers(id,state,updated_at,data_json) VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET state=excluded.state, updated_at=excluded.updated_at, data_json=excluded.data_json`, c.ConsumerID, string(c.State), updated.UnixNano(), b)
	return err
}
func (s *StateStore) GetConsumer(ctx context.Context, id string) (broker.ConsumerInfo, bool, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx, `SELECT data_json FROM fw_consumers WHERE id=?`, id).Scan(&data)
	if err == sql.ErrNoRows {
		return broker.ConsumerInfo{}, false, nil
	}
	if err != nil {
		return broker.ConsumerInfo{}, false, err
	}
	var c broker.ConsumerInfo
	if err := json.Unmarshal(data, &c); err != nil {
		return c, false, err
	}
	return c, true, nil
}
func (s *StateStore) ListConsumers(ctx context.Context) ([]broker.ConsumerInfo, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT data_json FROM fw_consumers ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []broker.ConsumerInfo
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var c broker.ConsumerInfo
		if err := json.Unmarshal(data, &c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
func (s *StateStore) DeleteConsumer(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM fw_consumers WHERE id=?`, id)
	return err
}
func (s *StateStore) AddConsumerEvent(ctx context.Context, id string, n broker.Notification) error {
	if n.Timestamp.IsZero() {
		n.Timestamp = time.Now()
	}
	b, err := json.Marshal(n)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO fw_consumer_events(consumer_id,event_id,created_at,data_json) VALUES(?,?,?,?)`, id, n.ID, n.Timestamp.UnixNano(), b)
	return err
}
func (s *StateStore) ConsumerEvents(ctx context.Context, id, after string, limit int) ([]broker.Notification, error) {
	q := `SELECT data_json FROM fw_consumer_events WHERE consumer_id=?`
	args := []any{id}
	if after != "" {
		q += ` AND seq > COALESCE((SELECT seq FROM fw_consumer_events WHERE consumer_id=? AND event_id=? LIMIT 1),0)`
		args = append(args, id, after)
	}
	q += ` ORDER BY seq DESC`
	if limit > 0 {
		q += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var rev []broker.Notification
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var n broker.Notification
		if err := json.Unmarshal(data, &n); err != nil {
			return nil, err
		}
		rev = append(rev, n)
	}
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	return rev, rows.Err()
}
func (s *StateStore) PutPublisherRecord(ctx context.Context, rec broker.PublisherRecord) error {
	if rec.UpdatedAt.IsZero() {
		rec.UpdatedAt = time.Now()
	}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = rec.UpdatedAt
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO fw_publisher_records(id,operation,status,updated_at,data_json) VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET operation=excluded.operation, status=excluded.status, updated_at=excluded.updated_at, data_json=excluded.data_json`, rec.ID, rec.Operation, rec.Status, rec.UpdatedAt.UnixNano(), b)
	return err
}
func (s *StateStore) GetPublisherRecord(ctx context.Context, id string) (broker.PublisherRecord, bool, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx, `SELECT data_json FROM fw_publisher_records WHERE id=?`, id).Scan(&data)
	if err == sql.ErrNoRows {
		return broker.PublisherRecord{}, false, nil
	}
	if err != nil {
		return broker.PublisherRecord{}, false, err
	}
	var rec broker.PublisherRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return rec, false, err
	}
	return rec, true, nil
}
func (s *StateStore) ListPublisherRecords(ctx context.Context, limit int) ([]broker.PublisherRecord, error) {
	q := `SELECT data_json FROM fw_publisher_records ORDER BY updated_at DESC`
	args := []any{}
	if limit > 0 {
		q += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []broker.PublisherRecord
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var rec broker.PublisherRecord
		if err := json.Unmarshal(data, &rec); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}
func (s *StateStore) Close() error {
	if s.own {
		return s.db.Close()
	}
	return nil
}
