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

	"github.com/oarkflow/fastworker/broker"
)

type StateStore struct {
	db  *sql.DB
	own bool
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
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
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
