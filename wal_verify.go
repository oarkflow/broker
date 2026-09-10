package broker

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
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
	backup := path + ".recovery-" + time.Now().UTC().Format("20060102T150405.000000000Z") + ".bak"
	if err := copyFilePrefix(path, backup, report.SizeBytes); err != nil {
		return report, "", fmt.Errorf("preserve original WAL: %w", err)
	}
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, ".broker-wal-repair-*")
	if err != nil {
		return report, backup, err
	}
	tempPath := temp.Name()
	cleanup := true
	defer func() {
		_ = temp.Close()
		if cleanup {
			_ = os.Remove(tempPath)
		}
	}()
	source, err := os.Open(path)
	if err == nil {
		_, err = io.CopyN(temp, source, report.GoodPrefixBytes)
		_ = source.Close()
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
	if err == nil {
		err = os.Rename(tempPath, path)
	}
	if err != nil {
		return report, backup, err
	}
	cleanup = false
	if directory, openErr := os.Open(dir); openErr == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	return report, backup, nil
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
// operations. It refuses to overwrite any existing path and removes a partial
// destination if validation or persistence fails.
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
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return report, err
	}
	complete := false
	defer func() {
		_ = file.Close()
		if !complete {
			_ = os.Remove(path)
		}
	}()
	buffered := bufio.NewWriterSize(file, 1<<20)
	decoder := json.NewDecoder(input)
	for {
		var record walRecord
		if decodeErr := decoder.Decode(&record); errors.Is(decodeErr, io.EOF) {
			break
		} else if decodeErr != nil {
			return report, fmt.Errorf("decode import record %d: %w", report.Records+1, decodeErr)
		}
		if !knownWALOperation(record.Op) {
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
	complete = true
	return VerifyBinaryWAL(path, walCodec, nil)
}

func knownWALOperation(op string) bool {
	switch op {
	case "declare_queue", "declare_topic", "declare_stream", "append", "append_atomic", "lease", "lease_batch", "update_batch", "ack", "ack_batch", "nack", "move", "pause_queue", "resume_queue", "purge_queue", "update", "commit_offset":
		return true
	default:
		return false
	}
}

type SegmentedWALVerificationReport struct {
	Dir             string                   `json:"dir"`
	ManifestVersion int                      `json:"manifest_version"`
	ActiveSegment   uint64                   `json:"active_segment"`
	TotalSegments   int                      `json:"total_segments"`
	TotalRecords    uint64                   `json:"total_records"`
	TotalBytes      int64                    `json:"total_bytes"`
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

	report.ManifestVersion = manifest.Version
	report.ActiveSegment = manifest.ActiveSegment
	report.TotalSegments = len(manifest.Segments)

	for _, seg := range manifest.Segments {
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

