package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/oarkflow/broker"
)

func main() {
	path := flag.String("path", "", "path to a BWL1 broker.wal.fwb file")
	dir := flag.String("dir", "", "path to a BWL2 segmented WAL directory (containing manifest.json); mutually exclusive with -path")
	repairTail := flag.Bool("repair-tail", false, "BWL1 only: preserve the original and atomically remove only an incomplete tail")
	exportJSON := flag.String("export-json", "", "BWL1 only: write decoded newline-delimited JSON operations to this file (or - for stdout)")
	importJSON := flag.String("import-json", "", "BWL1 only: create -path from newline-delimited JSON operations in this file (or - for stdin)")
	encryptionKey := flag.String("encryption-key", "", "hex AES key for encrypted BWL1/BWL2 records")
	decryptionKeys := flag.String("decryption-keys", "", "comma-separated hex previous AES keys accepted for WAL record/checkpoint recovery")
	rotateKey := flag.Bool("rotate-key", false, "rotate BWL1 or BWL2 encryption to -encryption-key offline, using -decryption-keys for existing data; stop brokerd first")
	recursive := flag.Bool("recursive", false, "with -rotate-key -dir, rotate each BWL2 store found beneath -dir")
	signingKey := flag.String("signing-key", "", "hex HMAC key for signed BWL1/BWL2 records")
	flag.Parse()
	oldKeys, keyErr := decodeKeyList(*decryptionKeys)
	if keyErr != nil {
		fmt.Fprintln(os.Stderr, keyErr)
		os.Exit(2)
	}
	if (*path == "") == (*dir == "") {
		fmt.Fprintln(os.Stderr, "exactly one of -path (BWL1 file) or -dir (BWL2 directory) is required")
		os.Exit(2)
	}
	if *recursive && (!*rotateKey || *dir == "") {
		fmt.Fprintln(os.Stderr, "-recursive requires -rotate-key with -dir")
		os.Exit(2)
	}
	if *rotateKey && (*repairTail || *exportJSON != "" || *importJSON != "") {
		fmt.Fprintln(os.Stderr, "-rotate-key is mutually exclusive with -repair-tail, -export-json and -import-json")
		os.Exit(2)
	}
	if *dir != "" {
		if *repairTail || *exportJSON != "" || *importJSON != "" {
			fmt.Fprintln(os.Stderr, "-repair-tail, -export-json and -import-json apply only to -path (BWL1); a BWL2 store self-heals a torn active-segment tail on open")
			os.Exit(2)
		}
		decode := func(value string) []byte {
			if value == "" {
				return nil
			}
			decoded, err := hex.DecodeString(value)
			if err != nil {
				fmt.Fprintln(os.Stderr, "key must be hexadecimal:", err)
				os.Exit(2)
			}
			return decoded
		}
		codec, err := broker.NewWALRecordCodec(broker.WALCodecConfig{EncryptionKey: decode(*encryptionKey), DecryptionKeys: oldKeys, SigningKey: decode(*signingKey)})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		if *rotateKey {
			if *encryptionKey == "" {
				fmt.Fprintln(os.Stderr, "-rotate-key requires the new primary -encryption-key")
				os.Exit(2)
			}
			newKeyCodec, err := broker.NewWALRecordCodec(broker.WALCodecConfig{
				EncryptionKey: decode(*encryptionKey),
				SigningKey:    decode(*signingKey),
			})
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
			storeDirs := []string{*dir}
			if *recursive {
				storeDirs, err = findSegmentedWALDirs(*dir)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
			}
			for _, storeDir := range storeDirs {
				store, openErr := broker.NewSegmentedWALStore(broker.SegmentedWALConfig{Dir: storeDir, WALCodec: codec, DisableCompactor: true})
				if openErr != nil {
					fmt.Fprintf(os.Stderr, "open BWL2 store %s: %v\n", storeDir, openErr)
					os.Exit(1)
				}
				rotateErr := store.RotateEncryptionKey()
				closeErr := store.Close()
				if rotateErr != nil || closeErr != nil {
					fmt.Fprintf(os.Stderr, "key rotation %s: %v\n", storeDir, errors.Join(rotateErr, closeErr))
					os.Exit(1)
				}
				report, verifyErr := broker.VerifySegmentedWAL(storeDir, newKeyCodec, nil)
				if verifyErr != nil || !report.Valid() {
					fmt.Fprintf(os.Stderr, "verify rotated BWL2 store %s with the new key: %v\n", storeDir, verifyErr)
					_ = json.NewEncoder(os.Stderr).Encode(report)
					os.Exit(1)
				}
				fmt.Fprintf(os.Stdout, "rotated BWL2 store %s\n", storeDir)
			}
			fmt.Fprintln(os.Stdout, "BWL2 encryption key rotation complete; retain old keys until every relevant store has been rotated")
			return
		}
		report, err := broker.VerifySegmentedWAL(*dir, codec, nil)
		_ = json.NewEncoder(os.Stdout).Encode(report)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if !report.Valid() {
			os.Exit(1)
		}
		return
	}
	decode := func(value string) []byte {
		if value == "" {
			return nil
		}
		decoded, err := hex.DecodeString(value)
		if err != nil {
			fmt.Fprintln(os.Stderr, "key must be hexadecimal:", err)
			os.Exit(2)
		}
		return decoded
	}
	codec, err := broker.NewWALRecordCodec(broker.WALCodecConfig{EncryptionKey: decode(*encryptionKey), DecryptionKeys: oldKeys, SigningKey: decode(*signingKey)})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if *rotateKey {
		if *encryptionKey == "" {
			fmt.Fprintln(os.Stderr, "-rotate-key requires the new primary -encryption-key")
			os.Exit(2)
		}
		oldCodecConfig := broker.WALCodecConfig{SigningKey: decode(*signingKey)}
		if len(oldKeys) > 0 {
			oldCodecConfig.EncryptionKey = oldKeys[0]
			oldCodecConfig.DecryptionKeys = oldKeys[1:]
		}
		oldCodec, err := broker.NewWALRecordCodec(oldCodecConfig)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		newCodec, err := broker.NewWALRecordCodec(broker.WALCodecConfig{EncryptionKey: decode(*encryptionKey), SigningKey: decode(*signingKey)})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		report, backup, rotateErr := broker.RotateBinaryWALEncryptionKey(*path, oldCodec, newCodec, nil)
		_ = json.NewEncoder(os.Stdout).Encode(struct {
			broker.WALVerificationReport
			Backup string `json:"backup,omitempty"`
		}{WALVerificationReport: report, Backup: backup})
		if rotateErr != nil {
			fmt.Fprintln(os.Stderr, rotateErr)
			os.Exit(1)
		}
		return
	}
	selected := 0
	for _, enabled := range []bool{*repairTail, *exportJSON != "", *importJSON != ""} {
		if enabled {
			selected++
		}
	}
	if selected > 1 {
		fmt.Fprintln(os.Stderr, "-repair-tail, -export-json and -import-json are mutually exclusive")
		os.Exit(2)
	}
	if *exportJSON != "" {
		output := os.Stdout
		if *exportJSON != "-" {
			output, err = os.OpenFile(*exportJSON, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			defer output.Close()
		}
		if _, err = broker.ExportBinaryWAL(*path, output, codec, nil); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *importJSON != "" {
		input := os.Stdin
		if *importJSON != "-" {
			input, err = os.Open(*importJSON)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			defer input.Close()
		}
		report, importErr := broker.ImportBinaryWAL(input, *path, codec)
		_ = json.NewEncoder(os.Stdout).Encode(report)
		if importErr != nil {
			fmt.Fprintln(os.Stderr, importErr)
			os.Exit(1)
		}
		return
	}
	var report broker.WALVerificationReport
	backup := ""
	if *repairTail {
		report, backup, err = broker.RepairBinaryWALTail(*path, codec, nil)
	} else {
		report, err = broker.VerifyBinaryWAL(*path, codec, nil)
	}
	output := struct {
		broker.WALVerificationReport
		Backup string `json:"backup,omitempty"`
	}{WALVerificationReport: report, Backup: backup}
	_ = json.NewEncoder(os.Stdout).Encode(output)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if !report.Valid() && !*repairTail {
		os.Exit(1)
	}
}

func decodeKeyList(raw string) ([][]byte, error) {
	var keys [][]byte
	for _, value := range strings.Split(raw, ",") {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key, err := hex.DecodeString(value)
		if err != nil {
			return nil, fmt.Errorf("decryption keys must be comma-separated hexadecimal AES keys: %w", err)
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func findSegmentedWALDirs(root string) ([]string, error) {
	var stores []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() {
			return nil
		}
		manifest := filepath.Join(path, "manifest.json")
		info, err := os.Lstat(manifest)
		if err == nil {
			if !info.Mode().IsRegular() {
				return fmt.Errorf("manifest is not a regular file: %s", manifest)
			}
			stores = append(stores, path)
			return filepath.SkipDir
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("discover BWL2 stores beneath %s: %w", root, err)
	}
	if len(stores) == 0 {
		return nil, fmt.Errorf("no BWL2 manifest.json stores found beneath %s", root)
	}
	sort.Strings(stores)
	return stores, nil
}
