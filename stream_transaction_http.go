package broker

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/oarkflow/fh"
)

// StreamTransactionHTTPPayload is the request body for
// POST /v1/streams/{input}/transactions. Messages use the normal publish payload
// and options, including JSON or base64 encoding for binary payloads.
type StreamTransactionHTTPPayload struct {
	ConsumerGroup  string               `json:"consumer_group"`
	TenantID       string               `json:"tenant_id,omitempty"`
	ExpectedOffset uint64               `json:"expected_offset"`
	Offset         uint64               `json:"offset"`
	Messages       []PublishHTTPPayload `json:"messages,omitempty"`
}

func (a AdminServer) streamTransaction(c fh.Ctx) error {
	stream, ok, err := streamParam(c)
	if !ok {
		return err
	}
	principal := principalFromContext(c.Context())
	acl := a.effectiveACL()
	if !acl.Authorize(principal, PermissionPublish) || !acl.Authorize(principal, PermissionStreamCommit) {
		return WriteBrokerError(c, ErrUnauthorized)
	}
	var req StreamTransactionHTTPPayload
	decoder := json.NewDecoder(bytes.NewReader(c.Body()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		return WriteBrokerError(c, errf(ErrInvalidMessage, "invalid stream transaction body"))
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return WriteBrokerError(c, errf(ErrInvalidMessage, "stream transaction requires one JSON object"))
	}
	tx, err := a.Broker.Publisher().BeginStreamTx(c.Context(), OffsetCommit{Stream: stream, ConsumerGroup: req.ConsumerGroup, Offset: req.Offset, TenantID: req.TenantID}, req.ExpectedOffset)
	if err != nil {
		return WriteBrokerError(c, err)
	}
	defer tx.Rollback(c.Context())
	for _, message := range req.Messages {
		payload, err := message.PayloadBytes()
		if err != nil {
			return WriteBrokerError(c, errf(ErrInvalidMessage, "invalid output payload: %v", err))
		}
		if err := tx.Publish(message.Topic, payload, message.PublishOptions()...); err != nil {
			return WriteBrokerError(c, err)
		}
	}
	results, err := tx.Commit(c.Context())
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, results)
}
