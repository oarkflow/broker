//go:build !darwin && !linux

package broker

import "context"

func measurePathStorageUsage(context.Context, string) (StorageUsage, error) {
	return StorageUsage{}, ErrStorageUsageUnsupported
}
