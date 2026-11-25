package broker

import (
	"context"
	"errors"
)

var ErrStorageUsageUnsupported = errors.New("storage usage is unavailable for this store")

// StorageUsage reports a store's logical bytes and its underlying filesystem
// capacity. Location identifies the measured store path; VolumeID groups paths
// that share one filesystem so available capacity is not double-counted.
type StorageUsage struct {
	Location       string `json:"location"`
	VolumeID       string `json:"volume_id"`
	UsedBytes      uint64 `json:"used_bytes"`
	AvailableBytes uint64 `json:"available_bytes"`
	CapacityBytes  uint64 `json:"capacity_bytes"`
}

// StorageUsageReporter is an optional Store extension for local disk-aware
// placement. Stores that cannot provide a reliable filesystem measurement
// should omit this interface rather than report a guessed value.
type StorageUsageReporter interface {
	StorageUsage(context.Context) (StorageUsage, error)
}

// MeasurePathStorageUsage measures a file or directory and the capacity of
// the filesystem containing it. Directory measurements sum regular-file
// logical sizes without following symlinks.
func MeasurePathStorageUsage(ctx context.Context, path string) (StorageUsage, error) {
	return measurePathStorageUsage(ctx, path)
}

func saturatingAddUint64(a, b uint64) uint64 {
	if ^uint64(0)-a < b {
		return ^uint64(0)
	}
	return a + b
}
