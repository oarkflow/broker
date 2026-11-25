package broker

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"time"
)

type WALVerificationProblem struct {
	Offset int64  `json:"offset"`
	Tail   bool   `json:"tail"`
	Error  string `json:"error"`
}

type WALVerificationReport struct {
	Path            string                   `json:"path"`
	SizeBytes       int64                    `json:"size_bytes"`
	GoodPrefixBytes int64                    `json:"good_prefix_bytes"`
	Records         uint64                   `json:"records"`
	NativeRecords   uint64                   `json:"native_records"`
	LegacyRecords   uint64                   `json:"legacy_records"`
	Problems        []WALVerificationProblem `json:"problems,omitempty"`
	VerifiedAt      time.Time                `json:"verified_at"`
}

func (r WALVerificationReport) Valid() bool { return len(r.Problems) == 0 }

// VerifyBinaryWAL performs a read-only validation of every length prefix,
// checksum, signature, encryption envelope and operation payload.
func VerifyBinaryWAL(path string, walCodec *WALRecordCodec, legacyCodec FrameCodec) (WALVerificationReport, error) {
	report := WALVerificationReport{Path: path, VerifiedAt: time.Now().UTC()}
	info, err := os.Stat(path)
	if err != nil {
		return report, err
	}
	report.SizeBytes = info.Size()
	if walCodec == nil {
		walCodec, err = NewWALRecordCodec(WALCodecConfig{})
		if err != nil {
			return report, err
		}
	}
	if legacyCodec == nil {
		legacyCodec = NewBinaryMessageCodec()
	}
	file, err := os.Open(path)
	if err != nil {
		return report, err
	}
	defer file.Close()
	reader := bufio.NewReaderSize(file, 1<<20)
	offset := int64(0)
	for {
		start := offset
		length, readErr := binary.ReadUvarint(reader)
		if errors.Is(readErr, io.EOF) {
			report.GoodPrefixBytes = offset
			return report, nil
		}
		if readErr != nil {
			report.Problems = append(report.Problems, WALVerificationProblem{Offset: start, Tail: true, Error: "incomplete or invalid record length: " + readErr.Error()})
			report.GoodPrefixBytes = start
			return report, nil
		}
		offset += int64(uvarintLen(length))
		if length == 0 || length > uint64(walCodec.maxRecordSize) {
			report.Problems = append(report.Problems, WALVerificationProblem{Offset: start, Error: fmt.Sprintf("invalid record length %d", length)})
			report.GoodPrefixBytes = start
			return report, nil
		}
		payload := make([]byte, int(length))
		if _, readErr := io.ReadFull(reader, payload); readErr != nil {
			report.Problems = append(report.Problems, WALVerificationProblem{Offset: start, Tail: true, Error: "incomplete record payload: " + readErr.Error()})
			report.GoodPrefixBytes = start
			return report, nil
		}
		offset += int64(length)
		var record walRecord
		if isNativeBinaryWALRecord(payload) {
			err = walCodec.decode(payload, &record)
			report.NativeRecords++
		} else {
			var message Message
			if err = legacyCodec.DecodeMessage(payload, &message); err == nil {
				err = json.Unmarshal(message.Payload, &record)
			}
			report.LegacyRecords++
		}
		if err != nil {
			report.Problems = append(report.Problems, WALVerificationProblem{Offset: start, Error: err.Error()})
			// Length framing allows validation to continue and report every bad
			// middle record, but repair will refuse to discard valid successors.
			continue
		}
		report.Records++
		if len(report.Problems) == 0 {
			report.GoodPrefixBytes = offset
		}
	}
}

// RepairBinaryWALTail preserves the original as a timestamped backup and
// atomically installs a copy truncated to the last verified prefix. It refuses
// to modify WALs with corrupt middle records.
func RepairBinaryWALTail(path string, walCodec *WALRecordCodec, legacyCodec FrameCodec) (WALVerificationReport, string, error) {
	var empty WALVerificationReport
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return empty, "", err
	}
	if !pathInfo.Mode().IsRegular() {
		return empty, "", fmt.Errorf("BWL1 tail repair requires a regular file: %s", path)
	}
	report, err := VerifyBinaryWAL(path, walCodec, legacyCodec)
	if err != nil {
		return report, "", err
	}
	if report.Valid() {
		return report, "", nil
	}
	for _, problem := range report.Problems {
		if !problem.Tail {
			return report, "", fmt.Errorf("repair refused: non-tail corruption at offset %d", problem.Offset)
		}
	}
	source, err := os.Open(path)
	if err != nil {
		return report, "", err
	}
	defer source.Close()
	sourceInfo, err := source.Stat()
	if err != nil {
		return report, "", err
	}
	if !os.SameFile(pathInfo, sourceInfo) || sourceInfo.Size() != report.SizeBytes {
		return report, "", errors.New("source BWL1 WAL changed during tail repair preflight")
	}
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, ".broker-wal-repair-*")
	if err != nil {
		return report, "", err
	}
	tempPath := temp.Name()
	cleanup := true
	defer func() {
		_ = temp.Close()
		if cleanup {
			_ = os.Remove(tempPath)
		}
	}()
	_, err = source.Seek(0, io.SeekStart)
	if err == nil {
		_, err = io.CopyN(temp, source, report.GoodPrefixBytes)
	}
	if err == nil {
		err = temp.Chmod(0o600)
	}
	if err == nil {
		err = temp.Sync()
	}
	if closeErr := temp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return report, "", err
	}
	repairedReport, err := VerifyBinaryWAL(tempPath, walCodec, legacyCodec)
	if err != nil || !repairedReport.Valid() || repairedReport.Records != report.Records {
		if err == nil {
			err = errors.New("repaired BWL1 WAL failed verification or record-count comparison")
		}
		return report, "", err
	}
	if err := verifyWALSourceUnchanged(path, pathInfo, source, sourceInfo); err != nil {
		return report, "", err
	}
	backup := path + ".recovery-" + time.Now().UTC().Format("20060102T150405.000000000Z") + ".bak"
	if err := copyOpenFile(source, backup); err != nil {
		return report, "", fmt.Errorf("preserve original WAL: %w", err)
	}
	if err := syncDirectory(dir); err != nil {
		return report, backup, fmt.Errorf("sync BWL1 backup directory: %w", err)
	}
	if err := verifyWALSourceUnchanged(path, pathInfo, source, sourceInfo); err != nil {
		return report, backup, err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return report, backup, err
	}
	cleanup = false
	if err := syncDirectory(dir); err != nil {
		return repairedReport, backup, fmt.Errorf("sync repaired BWL1 WAL directory: %w", err)
	}
	repairedReport.Path = path
	return repairedReport, backup, nil
}

func verifyWALSourceUnchanged(path string, originalInfo os.FileInfo, source *os.File, sourceInfo os.FileInfo) error {
	currentPathInfo, err := os.Lstat(path)
	if err != nil {
		return err
	}
	currentSourceInfo, err := source.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(originalInfo, currentPathInfo) || !os.SameFile(originalInfo, currentSourceInfo) ||
		currentPathInfo.Size() != originalInfo.Size() || !currentPathInfo.ModTime().Equal(originalInfo.ModTime()) ||
		currentSourceInfo.Size() != sourceInfo.Size() || !currentSourceInfo.ModTime().Equal(sourceInfo.ModTime()) {
		return errors.New("source BWL1 WAL changed during tail repair")
	}
	return nil
}

// RotateBinaryWALEncryptionKey rewrites a verified BWL1 WAL with a new
// encryption key. It preserves the original as a synced backup and atomically
// replaces the source only after the rewritten file verifies with newCodec.
// The broker must be stopped while this offline operation runs.
func RotateBinaryWALEncryptionKey(path string, oldCodec, newCodec *WALRecordCodec, legacyCodec FrameCodec) (WALVerificationReport, string, error) {
	var report WALVerificationReport
	if newCodec == nil || newCodec.aead == nil {
		return report, "", errors.New("BWL1 key rotation requires a new encryption key")
	}
	if oldCodec == nil {
		var err error
		oldCodec, err = NewWALRecordCodec(WALCodecConfig{})
		if err != nil {
			return report, "", err
		}
	}
	if legacyCodec == nil {
		legacyCodec = NewBinaryMessageCodec()
	}
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return report, "", err
	}
	if !pathInfo.Mode().IsRegular() {
		return report, "", fmt.Errorf("BWL1 key rotation requires a regular file: %s", path)
	}
	originalReport, err := VerifyBinaryWAL(path, oldCodec, legacyCodec)
	if err != nil {
		return originalReport, "", err
	}
	if !originalReport.Valid() {
		return originalReport, "", fmt.Errorf("key rotation refused: source WAL verification failed")
	}
	source, err := os.Open(path)
	if err != nil {
		return originalReport, "", err
	}
	defer source.Close()
	sourceInfo, err := source.Stat()
	if err != nil {
		return originalReport, "", err
	}
	if !os.SameFile(pathInfo, sourceInfo) || sourceInfo.Size() != originalReport.SizeBytes {
		return originalReport, "", errors.New("source BWL1 WAL changed during key rotation preflight")
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".broker-wal-rotate-*")
	if err != nil {
		return originalReport, "", err
	}
	tempPath := temp.Name()
	cleanup := true
	defer func() {
		_ = temp.Close()
		if cleanup {
			_ = os.Remove(tempPath)
		}
	}()
	if err := temp.Chmod(pathInfo.Mode().Perm()); err != nil {
		return originalReport, "", err
	}
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return originalReport, "", err
	}
	reader := bufio.NewReaderSize(source, 1<<20)
	writer := bufio.NewWriterSize(temp, 1<<20)
	originalDigest := sha256.New()
	var rotatedRecords uint64
	for {
		length, readErr := binary.ReadUvarint(reader)
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil || length == 0 || length > uint64(oldCodec.maxRecordSize) {
			if readErr == nil {
				readErr = fmt.Errorf("invalid BWL1 record length %d", length)
			}
			return originalReport, "", fmt.Errorf("read source BWL1 WAL during key rotation: %w", readErr)
		}
		payload := make([]byte, int(length))
		if _, err := io.ReadFull(reader, payload); err != nil {
			return originalReport, "", fmt.Errorf("read source BWL1 record during key rotation: %w", err)
		}
		var record walRecord
		if isNativeBinaryWALRecord(payload) {
			err = oldCodec.decode(payload, &record)
		} else {
			var message Message
			if err = legacyCodec.DecodeMessage(payload, &message); err == nil {
				err = json.Unmarshal(message.Payload, &record)
			}
		}
		if err != nil {
			return originalReport, "", fmt.Errorf("decode BWL1 record during key rotation: %w", err)
		}
		if err := writeWALRecordFingerprint(originalDigest, record); err != nil {
			return originalReport, "", err
		}
		record.Checksum = ""
		rotated, err := newCodec.encode(nil, record)
		if err != nil {
			return originalReport, "", fmt.Errorf("encode BWL1 record with new key: %w", err)
		}
		var prefix [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(prefix[:], uint64(len(rotated)))
		if _, err := writer.Write(prefix[:n]); err != nil {
			return originalReport, "", err
		}
		if _, err := writer.Write(rotated); err != nil {
			return originalReport, "", err
		}
		rotatedRecords++
	}
	if err := writer.Flush(); err != nil {
		return originalReport, "", err
	}
	if err := temp.Sync(); err != nil {
		return originalReport, "", err
	}
	if err := temp.Close(); err != nil {
		return originalReport, "", err
	}
	rotatedReport, err := VerifyBinaryWAL(tempPath, newCodec, nil)
	if err != nil || !rotatedReport.Valid() || rotatedReport.Records != originalReport.Records || rotatedRecords != originalReport.Records {
		if err == nil {
			err = errors.New("rewritten BWL1 WAL failed verification or record-count comparison")
		}
		return originalReport, "", err
	}
	rotatedDigest, digestRecords, err := binaryWALRecordFingerprint(tempPath, newCodec, nil)
	if err != nil || digestRecords != originalReport.Records || !bytes.Equal(originalDigest.Sum(nil), rotatedDigest) {
		if err == nil {
			err = errors.New("rewritten BWL1 WAL does not match the source record content")
		}
		return originalReport, "", err
	}
	currentInfo, err := os.Lstat(path)
	if err != nil {
		return originalReport, "", err
	}
	sourceInfo, err = source.Stat()
	if err != nil {
		return originalReport, "", err
	}
	if !os.SameFile(pathInfo, currentInfo) || !os.SameFile(pathInfo, sourceInfo) ||
		currentInfo.Size() != pathInfo.Size() || !currentInfo.ModTime().Equal(pathInfo.ModTime()) {
		return originalReport, "", errors.New("source BWL1 WAL changed during key rotation")
	}
	backup := path + ".key-rotation-" + time.Now().UTC().Format("20060102T150405.000000000Z") + ".bak"
	if err := copyOpenFile(source, backup); err != nil {
		return originalReport, "", fmt.Errorf("preserve original BWL1 WAL: %w", err)
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return originalReport, backup, fmt.Errorf("sync BWL1 backup directory: %w", err)
	}
	currentInfo, err = os.Lstat(path)
	if err != nil {
		return originalReport, backup, err
	}
	if !os.SameFile(pathInfo, currentInfo) || currentInfo.Size() != pathInfo.Size() || !currentInfo.ModTime().Equal(pathInfo.ModTime()) {
		return originalReport, backup, errors.New("source BWL1 WAL changed before atomic replacement")
	}
	if err := os.Rename(tempPath, path); err != nil {
		return originalReport, backup, err
	}
	cleanup = false
	rotatedReport.Path = path
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return rotatedReport, backup, err
	}
	return rotatedReport, backup, nil
}

func binaryWALRecordFingerprint(path string, codec *WALRecordCodec, legacyCodec FrameCodec) ([]byte, uint64, error) {
	if codec == nil {
		var err error
		codec, err = NewWALRecordCodec(WALCodecConfig{})
		if err != nil {
			return nil, 0, err
		}
	}
	if legacyCodec == nil {
		legacyCodec = NewBinaryMessageCodec()
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()
	reader := bufio.NewReaderSize(file, 1<<20)
	digest := sha256.New()
	var records uint64
	for {
		length, err := binary.ReadUvarint(reader)
		if errors.Is(err, io.EOF) {
			return digest.Sum(nil), records, nil
		}
		if err != nil {
			return nil, records, err
		}
		if length == 0 || length > uint64(codec.maxRecordSize) {
			return nil, records, fmt.Errorf("invalid BWL1 record length %d during fingerprint", length)
		}
		payload := make([]byte, int(length))
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, records, err
		}
		var record walRecord
		if isNativeBinaryWALRecord(payload) {
			err = codec.decode(payload, &record)
		} else {
			var message Message
			if err = legacyCodec.DecodeMessage(payload, &message); err == nil {
				err = json.Unmarshal(message.Payload, &record)
			}
		}
		if err != nil {
			return nil, records, err
		}
		if err := writeWALRecordFingerprint(digest, record); err != nil {
			return nil, records, err
		}
		records++
	}
}

func writeWALRecordFingerprint(digest hash.Hash, record walRecord) error {
	record.Checksum = ""
	canonical, err := json.Marshal(record)
	if err != nil {
		return err
	}
	var size [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(size[:], uint64(len(canonical)))
	if _, err := digest.Write(size[:n]); err != nil {
		return err
	}
	_, err = digest.Write(canonical)
	return err
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return err
	}
	return directory.Close()
}

func copyOpenFile(source *os.File, destinationPath string) error {
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return err
	}
	destination, err := os.OpenFile(destinationPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	complete := false
	defer func() {
		_ = destination.Close()
		if !complete {
			_ = os.Remove(destinationPath)
		}
	}()
	if _, err := io.Copy(destination, source); err != nil {
		return err
	}
	if err := destination.Sync(); err != nil {
		return err
	}
	if err := destination.Close(); err != nil {
		return err
	}
	complete = true
	return nil
}

func copyFilePrefix(sourcePath, destinationPath string, bytes int64) error {
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()
	destination, err := os.OpenFile(destinationPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err = io.CopyN(destination, source, bytes); err == nil {
		err = destination.Sync()
	}
	if closeErr := destination.Close(); err == nil {
		err = closeErr
	}
	return err
}

// ExportBinaryWAL writes one decoded JSON operation per line. The source WAL
// remains read-only and all native checksums, signatures and encryption tags
// are validated before an operation is emitted.
func ExportBinaryWAL(path string, output io.Writer, walCodec *WALRecordCodec, legacyCodec FrameCodec) (WALVerificationReport, error) {
	report, err := VerifyBinaryWAL(path, walCodec, legacyCodec)
	if err != nil || !report.Valid() {
		if err == nil {
			err = fmt.Errorf("export refused: WAL verification failed")
		}
		return report, err
	}
	if walCodec == nil {
		walCodec, err = NewWALRecordCodec(WALCodecConfig{})
		if err != nil {
			return report, err
		}
	}
	if legacyCodec == nil {
		legacyCodec = NewBinaryMessageCodec()
	}
	file, err := os.Open(path)
	if err != nil {
		return report, err
	}
	defer file.Close()
	reader := bufio.NewReaderSize(file, 1<<20)
	encoder := json.NewEncoder(output)
	for {
		length, readErr := binary.ReadUvarint(reader)
		if errors.Is(readErr, io.EOF) {
			return report, nil
		}
		if readErr != nil {
			return report, readErr
		}
		payload := make([]byte, int(length))
		if _, err := io.ReadFull(reader, payload); err != nil {
			return report, err
		}
		var record walRecord
		if isNativeBinaryWALRecord(payload) {
			err = walCodec.decode(payload, &record)
		} else {
			var message Message
			if err = legacyCodec.DecodeMessage(payload, &message); err == nil {
				err = json.Unmarshal(message.Payload, &record)
			}
		}
		if err != nil {
			return report, err
		}
		if err := encoder.Encode(record); err != nil {
			return report, err
		}
	}
}

// ImportBinaryWAL creates a new BWL1 file from newline-delimited JSON
// operations. It refuses to overwrite any existing path and publishes the
// verified file atomically so readers cannot observe a partial import.
func ImportBinaryWAL(input io.Reader, path string, walCodec *WALRecordCodec) (report WALVerificationReport, err error) {
	if walCodec == nil {
		walCodec, err = NewWALRecordCodec(WALCodecConfig{})
		if err != nil {
			return report, err
		}
	}
	if err = os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return report, err
	}
	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, ".broker-wal-import-*")
	if err != nil {
		return report, err
	}
	tempPath := file.Name()
	defer func() {
		_ = file.Close()
		_ = os.Remove(tempPath)
	}()
	buffered := bufio.NewWriterSize(file, 1<<20)
	decoder := json.NewDecoder(input)
	decoder.DisallowUnknownFields()
	for {
		var record walRecord
		if decodeErr := decoder.Decode(&record); errors.Is(decodeErr, io.EOF) {
			break
		} else if decodeErr != nil {
			return report, fmt.Errorf("decode import record %d: %w", report.Records+1, decodeErr)
		}
		if _, ok := walOperationCode(record.Op); !ok {
			return report, fmt.Errorf("unknown WAL operation %q", record.Op)
		}
		record.Checksum = ""
		payload, encodeErr := walCodec.encode(nil, record)
		if encodeErr != nil {
			return report, encodeErr
		}
		var prefix [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(prefix[:], uint64(len(payload)))
		if _, err = buffered.Write(prefix[:n]); err == nil {
			_, err = buffered.Write(payload)
		}
		if err != nil {
			return report, err
		}
		report.Records++
	}
	if err = buffered.Flush(); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return report, err
	}
	verified, verifyErr := VerifyBinaryWAL(tempPath, walCodec, nil)
	if verifyErr == nil && (!verified.Valid() || verified.Records != report.Records) {
		verifyErr = errors.New("imported BWL1 WAL failed verification or record-count comparison")
	}
	if verifyErr != nil {
		return verified, verifyErr
	}
	if err := os.Link(tempPath, path); err != nil {
		return verified, err
	}
	if err := os.Remove(tempPath); err != nil {
		return verified, fmt.Errorf("remove staged BWL1 import: %w", err)
	}
	if err := syncDirectory(dir); err != nil {
		return verified, fmt.Errorf("sync imported BWL1 WAL directory: %w", err)
	}
	verified.Path = path
	return verified, nil
}

type SegmentedWALVerificationReport struct {
	Dir             string                   `json:"dir"`
	ManifestVersion int                      `json:"manifest_version"`
	ActiveSegment   uint64                   `json:"active_segment"`
	TotalSegments   int                      `json:"total_segments"`
	TotalRecords    uint64                   `json:"total_records"`
	TotalBytes      int64                    `json:"total_bytes"`
	CheckpointFile  string                   `json:"checkpoint_file,omitempty"`
	CheckpointValid bool                     `json:"checkpoint_valid,omitempty"`
	Warnings        []string                 `json:"warnings,omitempty"`
	SegmentReports  []WALVerificationReport  `json:"segment_reports"`
	Problems        []WALVerificationProblem `json:"problems,omitempty"`
	VerifiedAt      time.Time                `json:"verified_at"`
}

func (r SegmentedWALVerificationReport) Valid() bool {
	if len(r.Problems) > 0 {
		return false
	}
	for _, sr := range r.SegmentReports {
		if !sr.Valid() {
			return false
		}
	}
	return true
}

// VerifySegmentedWAL verifies all segments and manifest in a BWL2 segmented WAL directory.
func VerifySegmentedWAL(dir string, walCodec *WALRecordCodec, legacyCodec FrameCodec) (SegmentedWALVerificationReport, error) {
	report := SegmentedWALVerificationReport{
		Dir:        dir,
		VerifiedAt: time.Now().UTC(),
	}

	manifestPath := filepath.Join(dir, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return report, fmt.Errorf("read manifest: %w", err)
	}

	var manifest SegmentedWALManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		report.Problems = append(report.Problems, WALVerificationProblem{Error: "invalid manifest.json: " + err.Error()})
		return report, nil
	}
	if walCodec == nil {
		walCodec, err = NewWALRecordCodec(WALCodecConfig{})
		if err != nil {
			return report, err
		}
	}
	if legacyCodec == nil {
		legacyCodec = NewBinaryMessageCodec()
	}

	report.ManifestVersion = manifest.Version
	report.ActiveSegment = manifest.ActiveSegment
	report.TotalSegments = len(manifest.Segments)
	if manifest.CheckpointFile != "" {
		report.CheckpointFile = manifest.CheckpointFile
		if manifest.CheckpointFile != segmentedWALCheckpointFile || !filepath.IsLocal(manifest.CheckpointFile) {
			report.Problems = append(report.Problems, WALVerificationProblem{Error: fmt.Sprintf("checkpoint path %q is unexpected or not local to the WAL directory", manifest.CheckpointFile)})
		} else {
			checkpoint, readErr := os.ReadFile(filepath.Join(dir, manifest.CheckpointFile))
			if readErr != nil {
				report.Problems = append(report.Problems, WALVerificationProblem{Error: fmt.Sprintf("read checkpoint %s: %v", manifest.CheckpointFile, readErr)})
			} else {
				plain, encrypted, decodeErr := walCodec.decryptCheckpoint(checkpoint)
				if decodeErr != nil {
					report.Problems = append(report.Problems, WALVerificationProblem{Error: fmt.Sprintf("verify checkpoint %s: %v", manifest.CheckpointFile, decodeErr)})
				} else {
					var snapshot MemoryStoreSnapshot
					if jsonErr := json.Unmarshal(plain, &snapshot); jsonErr != nil {
						report.Problems = append(report.Problems, WALVerificationProblem{Error: fmt.Sprintf("decode checkpoint %s: %v", manifest.CheckpointFile, jsonErr)})
					} else if snapshotErr := validateMemoryStoreSnapshot(snapshot); snapshotErr != nil {
						report.Problems = append(report.Problems, WALVerificationProblem{Error: fmt.Sprintf("validate checkpoint %s: %v", manifest.CheckpointFile, snapshotErr)})
					} else {
						report.CheckpointValid = true
						if walCodec.encryptionEnabled() && !encrypted {
							report.Warnings = append(report.Warnings, "referenced checkpoint is legacy plaintext; opening the store will rewrite it with encryption")
						}
					}
				}
			}
		}
	}

	for _, seg := range manifest.Segments {
		if err := validateSegmentMetaPaths(seg); err != nil {
			report.Problems = append(report.Problems, WALVerificationProblem{Error: err.Error()})
			continue
		}
		segPath := filepath.Join(dir, seg.Filename)
		info, err := os.Stat(segPath)
		if err != nil {
			report.Problems = append(report.Problems, WALVerificationProblem{Error: fmt.Sprintf("segment file %s missing: %v", seg.Filename, err)})
			continue
		}
		report.TotalBytes += info.Size()

		// Verify segment file (skip 22 byte BWL2 header for standard record verification)
		segFile, err := os.Open(segPath)
		if err != nil {
			report.Problems = append(report.Problems, WALVerificationProblem{Error: fmt.Sprintf("open segment %s: %v", seg.Filename, err)})
			continue
		}

		var h [22]byte
		if _, err := io.ReadFull(segFile, h[:]); err != nil {
			segFile.Close()
			report.Problems = append(report.Problems, WALVerificationProblem{Error: fmt.Sprintf("read segment header %s: %v", seg.Filename, err)})
			continue
		}
		segFile.Close()

		if binary.BigEndian.Uint32(h[0:4]) != SegmentMagic {
			report.Problems = append(report.Problems, WALVerificationProblem{Error: fmt.Sprintf("invalid magic in %s", seg.Filename)})
			continue
		}

		// Verify records in segment
		segRep, err := verifySegmentFileRecords(segPath, 22, walCodec, legacyCodec)
		if err != nil {
			report.Problems = append(report.Problems, WALVerificationProblem{Error: fmt.Sprintf("verify segment %s: %v", seg.Filename, err)})
			continue
		}
		report.TotalRecords += segRep.Records
		report.SegmentReports = append(report.SegmentReports, segRep)
	}

	return report, nil
}

func verifySegmentFileRecords(path string, offset int64, walCodec *WALRecordCodec, legacyCodec FrameCodec) (WALVerificationReport, error) {
	report := WALVerificationReport{Path: path, VerifiedAt: time.Now().UTC()}
	info, err := os.Stat(path)
	if err != nil {
		return report, err
	}
	report.SizeBytes = info.Size()
	if walCodec == nil {
		walCodec, err = NewWALRecordCodec(WALCodecConfig{})
		if err != nil {
			return report, err
		}
	}
	if legacyCodec == nil {
		legacyCodec = NewBinaryMessageCodec()
	}
	file, err := os.Open(path)
	if err != nil {
		return report, err
	}
	defer file.Close()

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return report, err
	}

	reader := bufio.NewReaderSize(file, 1<<20)
	for {
		start := offset
		length, readErr := binary.ReadUvarint(reader)
		if errors.Is(readErr, io.EOF) {
			report.GoodPrefixBytes = offset
			return report, nil
		}
		if readErr != nil {
			report.Problems = append(report.Problems, WALVerificationProblem{Offset: start, Tail: true, Error: "incomplete or invalid record length: " + readErr.Error()})
			report.GoodPrefixBytes = start
			return report, nil
		}
		offset += int64(uvarintLen(length))
		if length == 0 || length > uint64(walCodec.maxRecordSize) {
			report.Problems = append(report.Problems, WALVerificationProblem{Offset: start, Error: fmt.Sprintf("invalid record length %d", length)})
			report.GoodPrefixBytes = start
			return report, nil
		}
		payload := make([]byte, int(length))
		if _, readErr := io.ReadFull(reader, payload); readErr != nil {
			report.Problems = append(report.Problems, WALVerificationProblem{Offset: start, Tail: true, Error: "incomplete record payload: " + readErr.Error()})
			report.GoodPrefixBytes = start
			return report, nil
		}
		offset += int64(length)
		report.Records++
		if isNativeBinaryWALRecord(payload) {
			report.NativeRecords++
		} else {
			report.LegacyRecords++
		}
	}
}
