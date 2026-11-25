package sqlitestore

import (
	"context"
	"database/sql"

	"github.com/oarkflow/broker"
)

func sqliteStoragePath(ctx context.Context, db *sql.DB) (string, error) {
	if db == nil {
		return "", broker.ErrClosed
	}
	rows, err := db.QueryContext(ctx, "PRAGMA database_list")
	if err != nil {
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		var sequence int
		var name, path string
		if err := rows.Scan(&sequence, &name, &path); err != nil {
			return "", err
		}
		if name == "main" {
			if path == "" {
				return "", broker.ErrStorageUsageUnsupported
			}
			return path, nil
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return "", broker.ErrStorageUsageUnsupported
}

func (s *MessageStore) StorageUsage(ctx context.Context) (broker.StorageUsage, error) {
	if s == nil {
		return broker.StorageUsage{}, broker.ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	path, err := sqliteStoragePath(ctx, s.db)
	if err != nil {
		return broker.StorageUsage{}, err
	}
	return broker.MeasurePathStorageUsage(ctx, path)
}

func (s *EncryptedMessageStore) StorageUsage(ctx context.Context) (broker.StorageUsage, error) {
	if s == nil {
		return broker.StorageUsage{}, broker.ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	path, err := sqliteStoragePath(ctx, s.db)
	if err != nil {
		return broker.StorageUsage{}, err
	}
	return broker.MeasurePathStorageUsage(ctx, path)
}
