package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/oarkflow/broker"
)

func main() {
	path := flag.String("path", "", "path to a BWL1 broker.wal.fwb file")
	dir := flag.String("dir", "", "path to a BWL2 segmented WAL directory (containing manifest.json); mutually exclusive with -path")
	repairTail := flag.Bool("repair-tail", false, "BWL1 only: preserve the original and atomically remove only an incomplete tail")
	exportJSON := flag.String("export-json", "", "BWL1 only: write decoded newline-delimited JSON operations to this file (or - for stdout)")
	importJSON := flag.String("import-json", "", "BWL1 only: create -path from newline-delimited JSON operations in this file (or - for stdin)")
	encryptionKey := flag.String("encryption-key", "", "hex AES key for encrypted BWL1/BWL2 records")
	signingKey := flag.String("signing-key", "", "hex HMAC key for signed BWL1/BWL2 records")
	flag.Parse()
	if (*path == "") == (*dir == "") {
		fmt.Fprintln(os.Stderr, "exactly one of -path (BWL1 file) or -dir (BWL2 directory) is required")
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
		codec, err := broker.NewWALRecordCodec(broker.WALCodecConfig{EncryptionKey: decode(*encryptionKey), SigningKey: decode(*signingKey)})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
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
	codec, err := broker.NewWALRecordCodec(broker.WALCodecConfig{EncryptionKey: decode(*encryptionKey), SigningKey: decode(*signingKey)})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
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
