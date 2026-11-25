//go:build darwin || linux

package broker

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
)

func measurePathStorageUsage(ctx context.Context, path string) (StorageUsage, error) {
	if err := ctx.Err(); err != nil {
		return StorageUsage{}, err
	}
	if path == "" {
		return StorageUsage{}, ErrStorageUsageUnsupported
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return StorageUsage{}, err
	}
	info, err := os.Stat(absolute)
	missing := errors.Is(err, os.ErrNotExist)
	if err != nil && !missing {
		return StorageUsage{}, err
	}
	fsPath := absolute
	used := uint64(0)
	if missing {
		fsPath = filepath.Dir(absolute)
		for {
			if _, statErr := os.Stat(fsPath); statErr == nil {
				break
			} else if !errors.Is(statErr, os.ErrNotExist) {
				return StorageUsage{}, statErr
			}
			parent := filepath.Dir(fsPath)
			if parent == fsPath {
				return StorageUsage{}, os.ErrNotExist
			}
			fsPath = parent
		}
	} else if info.IsDir() {
		err = filepath.WalkDir(absolute, func(item string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
				return nil
			}
			itemInfo, err := entry.Info()
			if err != nil {
				return err
			}
			if itemInfo.Size() > 0 {
				used = saturatingAddUint64(used, uint64(itemInfo.Size()))
			}
			return nil
		})
		if err != nil {
			return StorageUsage{}, err
		}
	} else if info.Mode().IsRegular() {
		if info.Size() > 0 {
			used = uint64(info.Size())
		}
		fsPath = filepath.Dir(absolute)
		// SQLite WAL files are part of the store's on-disk footprint while they
		// exist, even though PRAGMA database_list reports only the main file.
		for _, suffix := range []string{"-wal", "-shm"} {
			sidecar, statErr := os.Stat(absolute + suffix)
			if statErr == nil && sidecar.Mode().IsRegular() && sidecar.Size() > 0 {
				used = saturatingAddUint64(used, uint64(sidecar.Size()))
			}
		}
	} else {
		return StorageUsage{}, ErrStorageUsageUnsupported
	}

	var filesystem syscall.Statfs_t
	if err := syscall.Statfs(fsPath, &filesystem); err != nil {
		return StorageUsage{}, err
	}
	var availableBytes, capacityBytes uint64
	if filesystem.Bavail > 0 && filesystem.Bsize > 0 {
		availableBytes = uint64(filesystem.Bavail) * uint64(filesystem.Bsize)
	}
	if filesystem.Blocks > 0 && filesystem.Bsize > 0 {
		capacityBytes = uint64(filesystem.Blocks) * uint64(filesystem.Bsize)
	}
	volumeInfo := info
	if missing {
		volumeInfo, err = os.Stat(fsPath)
		if err != nil {
			return StorageUsage{}, err
		}
	}
	fileStat, ok := volumeInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return StorageUsage{}, fmt.Errorf("filesystem device identity is unavailable")
	}
	return StorageUsage{
		Location: absolute, VolumeID: strconv.FormatUint(uint64(fileStat.Dev), 10),
		UsedBytes: used, AvailableBytes: availableBytes, CapacityBytes: capacityBytes,
	}, nil
}
