package broker

import (
	"encoding/json"
	"io"
	"net/http"
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

func (a AdminServer) streamTransaction(w http.ResponseWriter, r *http.Request, stream string) {
	if r.Method != http.MethodPost {
		methodBroker(w)
		return
	}
	principal := principalFromContext(r.Context())
	acl := a.effectiveACL()
	if !acl.Authorize(principal, PermissionPublish) || !acl.Authorize(principal, PermissionStreamCommit) {
		WriteBrokerError(w, ErrUnauthorized)
		return
	}
	var req StreamTransactionHTTPPayload
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		WriteBrokerError(w, errf(ErrInvalidMessage, "invalid stream transaction body"))
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		WriteBrokerError(w, errf(ErrInvalidMessage, "stream transaction requires one JSON object"))
		return
	}
	tx, err := a.Broker.Publisher().BeginStreamTx(r.Context(), OffsetCommit{Stream: stream, ConsumerGroup: req.ConsumerGroup, Offset: req.Offset, TenantID: req.TenantID}, req.ExpectedOffset)
	if err != nil {
		WriteBrokerError(w, err)
		return
	}
	defer tx.Rollback(r.Context())
	for _, message := range req.Messages {
		payload, err := message.PayloadBytes()
		if err != nil {
			WriteBrokerError(w, errf(ErrInvalidMessage, "invalid output payload: %v", err))
			return
		}
		if err := tx.Publish(message.Topic, payload, message.PublishOptions()...); err != nil {
			WriteBrokerError(w, err)
			return
		}
	}
	results, err := tx.Commit(r.Context())
	if err != nil {
		WriteBrokerError(w, err)
		return
	}
	writeBrokerJSON(w, results)
}
