package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

func decodeStrictBoundedJSON(reader io.Reader, maxBytes int64, destination any) error {
	if maxBytes <= 0 {
		return fmt.Errorf("invalid JSON response size limit")
	}
	body, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return err
	}
	if int64(len(body)) > maxBytes {
		return fmt.Errorf("JSON response exceeds %d bytes", maxBytes)
	}
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return fmt.Errorf("JSON response must be an object")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("JSON response contains multiple values")
		}
		return err
	}
	return nil
}
