package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/oarkflow/broker/cluster"
	"github.com/oarkflow/fh"
)

const internalPartitionProposalPath = "/cluster/internal/partition-propose"
const internalPartitionPreparePath = "/cluster/internal/partition-prepare"
const internalPartitionOutcomePath = "/cluster/internal/partition-outcome"
const internalPartitionStagedPath = "/cluster/internal/partition-staged"
const internalPartitionResultPath = "/cluster/internal/partition-result"
const internalPartitionPurgeResultPath = "/cluster/internal/partition-purge-result"

type partitionProposalRequest struct {
	Kind      cluster.PartitionResourceKind `json:"kind"`
	Name      string                        `json:"name"`
	Partition int                           `json:"partition"`
	Epoch     uint64                        `json:"epoch"`
	Data      []byte                        `json:"data"`
}

type partitionTransactionPrepareRequest struct {
	ID                 cluster.PartitionGroupID  `json:"id"`
	TransactionID      string                    `json:"transaction_id"`
	Deadline           time.Time                 `json:"deadline"`
	Operation          cluster.BrokerCommandType `json:"operation"`
	RequestFingerprint string                    `json:"request_fingerprint"`
	Payload            json.RawMessage           `json:"payload"`
}

type partitionTransactionPrepareResponse struct {
	Messages []Message `json:"messages,omitempty"`
}

type partitionTransactionOutcomeRequest struct {
	Commit      OffsetCommit `json:"commit"`
	Fingerprint string       `json:"fingerprint"`
}

type partitionTransactionOutcomeResponse struct {
	Found       bool                    `json:"found"`
	Transaction OffsetAppendTransaction `json:"transaction,omitempty"`
}

type partitionStagedRequest struct {
	ID            cluster.PartitionGroupID `json:"id"`
	TransactionID string                   `json:"transaction_id"`
	ParticipantID string                   `json:"participant_id"`
}

type partitionStagedResponse struct {
	Found  bool                    `json:"found"`
	Staged stagedBrokerTransaction `json:"staged,omitempty"`
}

type partitionResultRequest struct {
	ID            cluster.PartitionGroupID `json:"id"`
	TransactionID string                   `json:"transaction_id"`
	ParticipantID string                   `json:"participant_id"`
}

type partitionResultResponse struct {
	Found    bool      `json:"found"`
	Messages []Message `json:"messages,omitempty"`
}

type partitionPurgeResultResponse struct {
	Found  bool `json:"found"`
	Purged int  `json:"purged,omitempty"`
}

func (db *DistributedBroker) forwardPartitionPurgeResultLookup(ctx context.Context, id cluster.PartitionGroupID, txID, participantID, address string) (int, bool, error) {
	if db.partitionForwardClient == nil || !strings.HasPrefix(strings.ToLower(address), "https://") {
		return 0, false, fmt.Errorf("partition purge result lookup requires a configured HTTPS mTLS client")
	}
	body, err := json.Marshal(partitionStagedRequest{ID: id.Canonical(), TransactionID: txID, ParticipantID: participantID})
	if err != nil {
		return 0, false, err
	}
	requestCtx := ctx
	if requestCtx == nil {
		requestCtx = context.Background()
	}
	response, err := SendHTTP(requestCtx, db.partitionForwardClient, fh.MethodPOST, strings.TrimRight(address, "/")+internalPartitionPurgeResultPath, body, jsonRequestHeaders)
	if err != nil {
		return 0, false, fmt.Errorf("lookup committed partition purge result: %w", err)
	}
	defer response.Close()
	if response.StatusCode() != fh.StatusOK {
		data, readErr := io.ReadAll(io.LimitReader(response.Reader(), 4096))
		if readErr != nil {
			return 0, false, fmt.Errorf("partition leader returned %s (read error: %v)", response.Raw.Status, readErr)
		}
		return 0, false, fmt.Errorf("partition leader returned %s: %s", response.Raw.Status, strings.TrimSpace(string(data)))
	}
	var result partitionPurgeResultResponse
	if err := json.NewDecoder(io.LimitReader(response.Reader(), 4096)).Decode(&result); err != nil {
		return 0, false, fmt.Errorf("decode committed partition purge result: %w", err)
	}
	return result.Purged, result.Found, nil
}

func (db *DistributedBroker) forwardPartitionStagedLookup(ctx context.Context, id cluster.PartitionGroupID, txID, participantID string, address string) (stagedBrokerTransaction, bool, error) {
	if db.partitionForwardClient == nil || !strings.HasPrefix(strings.ToLower(address), "https://") {
		return stagedBrokerTransaction{}, false, fmt.Errorf("partition transaction lookup requires a configured HTTPS mTLS client")
	}
	body, err := json.Marshal(partitionStagedRequest{ID: id.Canonical(), TransactionID: txID, ParticipantID: participantID})
	if err != nil {
		return stagedBrokerTransaction{}, false, err
	}
	requestCtx := ctx
	if requestCtx == nil {
		requestCtx = context.Background()
	}
	response, err := SendHTTP(requestCtx, db.partitionForwardClient, fh.MethodPOST, strings.TrimRight(address, "/")+internalPartitionStagedPath, body, jsonRequestHeaders)
	if err != nil {
		return stagedBrokerTransaction{}, false, fmt.Errorf("lookup staged partition transaction: %w", err)
	}
	defer response.Close()
	if response.StatusCode() != fh.StatusOK {
		data, readErr := io.ReadAll(io.LimitReader(response.Reader(), 4096))
		if readErr != nil {
			return stagedBrokerTransaction{}, false, fmt.Errorf("partition leader returned %s (read error: %v)", response.Raw.Status, readErr)
		}
		return stagedBrokerTransaction{}, false, fmt.Errorf("partition leader returned %s: %s", response.Raw.Status, strings.TrimSpace(string(data)))
	}
	var result partitionStagedResponse
	if err := json.NewDecoder(io.LimitReader(response.Reader(), 64<<20)).Decode(&result); err != nil {
		return stagedBrokerTransaction{}, false, fmt.Errorf("decode staged partition transaction: %w", err)
	}
	return result.Staged, result.Found, nil
}

func (db *DistributedBroker) forwardPartitionResultLookup(ctx context.Context, id cluster.PartitionGroupID, txID, participantID, address string) ([]Message, bool, error) {
	if db.partitionForwardClient == nil || !strings.HasPrefix(strings.ToLower(address), "https://") {
		return nil, false, fmt.Errorf("partition transaction result lookup requires a configured HTTPS mTLS client")
	}
	body, err := json.Marshal(partitionResultRequest{ID: id.Canonical(), TransactionID: txID, ParticipantID: participantID})
	if err != nil {
		return nil, false, err
	}
	requestCtx := ctx
	if requestCtx == nil {
		requestCtx = context.Background()
	}
	response, err := SendHTTP(requestCtx, db.partitionForwardClient, fh.MethodPOST, strings.TrimRight(address, "/")+internalPartitionResultPath, body, jsonRequestHeaders)
	if err != nil {
		return nil, false, fmt.Errorf("lookup committed partition transaction result: %w", err)
	}
	defer response.Close()
	if response.StatusCode() != fh.StatusOK {
		data, readErr := io.ReadAll(io.LimitReader(response.Reader(), 4096))
		if readErr != nil {
			return nil, false, fmt.Errorf("partition leader returned %s (read error: %v)", response.Raw.Status, readErr)
		}
		return nil, false, fmt.Errorf("partition leader returned %s: %s", response.Raw.Status, strings.TrimSpace(string(data)))
	}
	var result partitionResultResponse
	if err := json.NewDecoder(io.LimitReader(response.Reader(), 64<<20)).Decode(&result); err != nil {
		return nil, false, fmt.Errorf("decode committed partition transaction result: %w", err)
	}
	return result.Messages, result.Found, nil
}

func (db *DistributedBroker) forwardPartitionProposal(ctx context.Context, address string, kind cluster.PartitionResourceKind, name string, partition int, epoch uint64, data []byte) error {
	if db.partitionForwardClient == nil {
		return fmt.Errorf("partition proposal forwarding requires a configured mTLS client")
	}
	if !strings.HasPrefix(strings.ToLower(address), "https://") {
		return fmt.Errorf("partition proposal forwarding requires an HTTPS client address")
	}
	if strings.TrimSpace(address) == "" {
		return fmt.Errorf("partition leader has no client address")
	}
	encoded, err := json.Marshal(partitionProposalRequest{Kind: kind, Name: name, Partition: partition, Epoch: epoch, Data: data})
	if err != nil {
		return err
	}
	requestCtx := ctx
	if requestCtx == nil {
		requestCtx = context.Background()
	}
	response, err := SendHTTP(requestCtx, db.partitionForwardClient, fh.MethodPOST, strings.TrimRight(address, "/")+internalPartitionProposalPath, encoded, jsonRequestHeaders)
	if err != nil {
		return fmt.Errorf("forward partition proposal to leader: %w", err)
	}
	defer response.Close()
	if response.StatusCode() != fh.StatusNoContent {
		body, readErr := io.ReadAll(io.LimitReader(response.Reader(), 4096))
		if readErr != nil {
			return fmt.Errorf("partition leader returned %s (read error: %v)", response.Raw.Status, readErr)
		}
		return fmt.Errorf("partition leader returned %s: %s", response.Raw.Status, strings.TrimSpace(string(body)))
	}
	return nil
}

func (db *DistributedBroker) forwardPartitionTransactionPrepare(ctx context.Context, id cluster.PartitionGroupID, txID string, deadline time.Time, operation cluster.BrokerCommandType, payload []byte, requestFingerprint, participantID string) ([]Message, error) {
	route, err := db.PartitionResourceRoute(ctx, id.Kind, id.Topic, id.Partition)
	if err != nil {
		return nil, err
	}
	if route.LocalLeader {
		expectedID := PartitionTransactionParticipantID(id)
		if operation == cluster.BrokerCmdCommitOffsetAppend {
			expectedID = PartitionOffsetTransactionParticipantID(id)
		}
		if participantID != expectedID {
			return nil, cluster.ErrInvalidTransaction
		}
		return db.preparePartitionTransactionFromPeer(ctx, id, txID, deadline, operation, payload, requestFingerprint)
	}
	if db.partitionForwardClient == nil {
		return nil, fmt.Errorf("partition transaction prepare forwarding requires a configured mTLS client")
	}
	if !strings.HasPrefix(strings.ToLower(route.LeaderClientAddress), "https://") {
		return nil, fmt.Errorf("partition transaction prepare forwarding requires an HTTPS client address")
	}
	if len(payload) == 0 || len(payload) > 64<<20 {
		return nil, fmt.Errorf("partition transaction prepare payload size is invalid")
	}
	expectedID := PartitionTransactionParticipantID(id)
	if operation == cluster.BrokerCmdCommitOffsetAppend {
		expectedID = PartitionOffsetTransactionParticipantID(id)
	}
	if participantID != expectedID {
		return nil, cluster.ErrInvalidTransaction
	}
	encoded, err := json.Marshal(partitionTransactionPrepareRequest{
		ID: id.Canonical(), TransactionID: txID, Deadline: deadline.UTC(),
		Operation: operation, RequestFingerprint: requestFingerprint,
		Payload: append(json.RawMessage(nil), payload...),
	})
	if err != nil {
		return nil, err
	}
	requestCtx := ctx
	if requestCtx == nil {
		requestCtx = context.Background()
	}
	response, err := SendHTTP(requestCtx, db.partitionForwardClient, fh.MethodPOST, strings.TrimRight(route.LeaderClientAddress, "/")+internalPartitionPreparePath, encoded, jsonRequestHeaders)
	if err != nil {
		return nil, fmt.Errorf("forward partition transaction prepare to leader: %w", err)
	}
	defer response.Close()
	if response.StatusCode() != fh.StatusOK {
		body, readErr := io.ReadAll(io.LimitReader(response.Reader(), 4096))
		if readErr != nil {
			return nil, fmt.Errorf("partition leader returned %s (read error: %v)", response.Raw.Status, readErr)
		}
		return nil, fmt.Errorf("partition leader returned %s: %s", response.Raw.Status, strings.TrimSpace(string(body)))
	}
	var result partitionTransactionPrepareResponse
	if err := json.NewDecoder(io.LimitReader(response.Reader(), 64<<20)).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode partition transaction prepare result: %w", err)
	}
	return result.Messages, nil
}

func (db *DistributedBroker) lookupPartitionOffsetAppendOutcome(ctx context.Context, commit OffsetCommit, fingerprint string) (OffsetAppendTransaction, bool, error) {
	id := cluster.PartitionGroupID{Kind: cluster.PartitionResourceStream, Topic: commit.Stream, Partition: commit.Partition}.Canonical()
	route, err := db.PartitionResourceRoute(ctx, id.Kind, id.Topic, id.Partition)
	if err != nil {
		return OffsetAppendTransaction{}, false, err
	}
	if route.LocalLeader {
		store, _, routed, err := db.partitionStoreForResourceWrite(ctx, id.Kind, id.Topic, id.Partition)
		if err != nil {
			return OffsetAppendTransaction{}, false, err
		}
		if !routed || store == nil {
			return OffsetAppendTransaction{}, false, ErrBrokerUnavailable
		}
		lookup, ok := store.(interface {
			lookupOffsetAppendTransaction(OffsetCommit, string) (OffsetAppendTransaction, bool, error)
		})
		if !ok {
			return OffsetAppendTransaction{}, false, fmt.Errorf("%w: partition store cannot query offset-append outcomes", ErrUnsupportedMode)
		}
		return lookup.lookupOffsetAppendTransaction(commit, fingerprint)
	}
	if db.partitionForwardClient == nil || !strings.HasPrefix(strings.ToLower(route.LeaderClientAddress), "https://") {
		return OffsetAppendTransaction{}, false, fmt.Errorf("partition outcome lookup requires a configured HTTPS mTLS client")
	}
	encoded, err := json.Marshal(partitionTransactionOutcomeRequest{Commit: commit, Fingerprint: fingerprint})
	if err != nil {
		return OffsetAppendTransaction{}, false, err
	}
	requestCtx := ctx
	if requestCtx == nil {
		requestCtx = context.Background()
	}
	response, err := SendHTTP(requestCtx, db.partitionForwardClient, fh.MethodPOST, strings.TrimRight(route.LeaderClientAddress, "/")+internalPartitionOutcomePath, encoded, jsonRequestHeaders)
	if err != nil {
		return OffsetAppendTransaction{}, false, fmt.Errorf("lookup partition transaction outcome: %w", err)
	}
	defer response.Close()
	if response.StatusCode() != fh.StatusOK {
		body, readErr := io.ReadAll(io.LimitReader(response.Reader(), 4096))
		if readErr != nil {
			return OffsetAppendTransaction{}, false, fmt.Errorf("partition leader returned %s (read error: %v)", response.Raw.Status, readErr)
		}
		return OffsetAppendTransaction{}, false, fmt.Errorf("partition leader returned %s: %s", response.Raw.Status, strings.TrimSpace(string(body)))
	}
	var result partitionTransactionOutcomeResponse
	if err := json.NewDecoder(io.LimitReader(response.Reader(), 64<<20)).Decode(&result); err != nil {
		return OffsetAppendTransaction{}, false, fmt.Errorf("decode partition transaction outcome: %w", err)
	}
	return result.Transaction, result.Found, nil
}

func (a AdminServer) internalPartitionPropose(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "internal partition proposal") {
		return nil
	}
	if a.Distributed == nil {
		return WriteBrokerError(c, ErrUnsupportedMode)
	}
	var request partitionProposalRequest
	decoder := json.NewDecoder(bytes.NewReader(c.Body()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return WriteBrokerError(c, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return WriteBrokerError(c, ErrInvalidMessage)
	}
	kind := request.Kind
	if kind == "" {
		kind = cluster.PartitionResourceTopic
	}
	if (kind != cluster.PartitionResourceTopic && kind != cluster.PartitionResourceStream && kind != cluster.PartitionResourceQueue) ||
		request.Name == "" || request.Partition < 0 || request.Epoch == 0 || len(request.Data) == 0 || len(request.Data) > 64<<20 {
		return WriteBrokerError(c, ErrInvalidMessage)
	}
	if err := a.Distributed.proposeResourcePartitionFromPeer(c.Context(), kind, request.Name, request.Partition, request.Epoch, request.Data); err != nil {
		return a.writeControllerError(c, err)
	}
	c.Status(fh.StatusNoContent)
	return nil
}

func (a AdminServer) internalPartitionPrepare(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "internal partition transaction prepare") {
		return nil
	}
	if a.Distributed == nil {
		return WriteBrokerError(c, ErrUnsupportedMode)
	}
	var request partitionTransactionPrepareRequest
	decoder := json.NewDecoder(bytes.NewReader(c.Body()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return WriteBrokerError(c, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return WriteBrokerError(c, ErrInvalidMessage)
	}
	request.ID = request.ID.Canonical()
	participantID := PartitionTransactionParticipantID(request.ID)
	if request.Operation == cluster.BrokerCmdCommitOffsetAppend {
		participantID = PartitionOffsetTransactionParticipantID(request.ID)
	}
	if !validBrokerTransactionIdentity(request.TransactionID, participantID) ||
		request.ID.Topic == "" || request.ID.Partition < 0 || request.Deadline.IsZero() || !json.Valid(request.Payload) || len(request.Payload) > 64<<20 ||
		request.RequestFingerprint != transactionPayloadFingerprint(request.Payload) {
		return WriteBrokerError(c, cluster.ErrInvalidTransaction)
	}
	messages, err := a.Distributed.preparePartitionTransactionFromPeer(c.Context(), request.ID, request.TransactionID, request.Deadline, request.Operation, request.Payload, request.RequestFingerprint)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, partitionTransactionPrepareResponse{Messages: messages})
}

func (a AdminServer) internalPartitionOutcome(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "internal partition transaction outcome") {
		return nil
	}
	if a.Distributed == nil {
		return WriteBrokerError(c, ErrUnsupportedMode)
	}
	var request partitionTransactionOutcomeRequest
	decoder := json.NewDecoder(bytes.NewReader(c.Body()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return WriteBrokerError(c, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return WriteBrokerError(c, ErrInvalidMessage)
	}
	if !request.Commit.Partitioned || request.Commit.Partition < 0 || request.Commit.Stream == "" ||
		request.Commit.TransactionID == "" || !validTransactionPayloadFingerprint(request.Fingerprint) {
		return WriteBrokerError(c, cluster.ErrInvalidTransaction)
	}
	id := cluster.PartitionGroupID{Kind: cluster.PartitionResourceStream, Topic: request.Commit.Stream, Partition: request.Commit.Partition}
	route, err := a.Distributed.PartitionResourceRoute(c.Context(), id.Kind, id.Topic, id.Partition)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	if !route.LocalLeader {
		return a.writeControllerError(c, NotLeaderError(route.LeaderClientAddress, route.RaftTerm))
	}
	store, _, routed, err := a.Distributed.partitionStoreForResourceWrite(c.Context(), id.Kind, id.Topic, id.Partition)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	if !routed || store == nil {
		return WriteBrokerError(c, ErrBrokerUnavailable)
	}
	lookup, ok := store.(interface {
		lookupOffsetAppendTransaction(OffsetCommit, string) (OffsetAppendTransaction, bool, error)
	})
	if !ok {
		return WriteBrokerError(c, fmt.Errorf("%w: partition store cannot query offset-append outcomes", ErrUnsupportedMode))
	}
	transaction, found, err := lookup.lookupOffsetAppendTransaction(request.Commit, request.Fingerprint)
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, partitionTransactionOutcomeResponse{Found: found, Transaction: transaction})
}

func (a AdminServer) internalPartitionStaged(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "internal partition transaction lookup") {
		return nil
	}
	if a.Distributed == nil {
		return WriteBrokerError(c, ErrUnsupportedMode)
	}
	var request partitionStagedRequest
	decoder := json.NewDecoder(bytes.NewReader(c.Body()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return WriteBrokerError(c, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return WriteBrokerError(c, ErrInvalidMessage)
	}
	id := request.ID.Canonical()
	participantID, err := decodePartitionTransactionParticipantID(request.ParticipantID)
	if err != nil || participantID != id || !validBrokerTransactionIdentity(request.TransactionID, request.ParticipantID) {
		return WriteBrokerError(c, cluster.ErrInvalidTransaction)
	}
	route, err := a.Distributed.PartitionResourceRoute(c.Context(), id.Kind, id.Topic, id.Partition)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	if !route.LocalLeader {
		return a.writeControllerError(c, NotLeaderError(route.LeaderClientAddress, route.RaftTerm))
	}
	staged, found, err := a.Distributed.lookupLocalPartitionStagedTransaction(c.Context(), id, request.TransactionID, request.ParticipantID)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, partitionStagedResponse{Found: found, Staged: staged})
}

func (a AdminServer) internalPartitionResult(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "internal partition transaction result") {
		return nil
	}
	if a.Distributed == nil {
		return WriteBrokerError(c, ErrUnsupportedMode)
	}
	var request partitionResultRequest
	decoder := json.NewDecoder(bytes.NewReader(c.Body()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return WriteBrokerError(c, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return WriteBrokerError(c, ErrInvalidMessage)
	}
	id := request.ID.Canonical()
	participantID, err := decodePartitionTransactionParticipantID(request.ParticipantID)
	if err != nil || participantID != id || id.Kind != cluster.PartitionResourceStream ||
		request.ParticipantID != PartitionTransactionParticipantID(id) || !validBrokerTransactionIdentity(request.TransactionID, request.ParticipantID) {
		return WriteBrokerError(c, cluster.ErrInvalidTransaction)
	}
	route, err := a.Distributed.PartitionResourceRoute(c.Context(), id.Kind, id.Topic, id.Partition)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	if !route.LocalLeader {
		return a.writeControllerError(c, NotLeaderError(route.LeaderClientAddress, route.RaftTerm))
	}
	messages, found, err := a.Distributed.lookupLocalPartitionTransactionResult(c.Context(), id, request.TransactionID, request.ParticipantID)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, partitionResultResponse{Found: found, Messages: messages})
}

func (a AdminServer) internalPartitionPurgeResult(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "internal partition purge result") {
		return nil
	}
	if a.Distributed == nil {
		return WriteBrokerError(c, ErrUnsupportedMode)
	}
	var request partitionStagedRequest
	decoder := json.NewDecoder(bytes.NewReader(c.Body()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return WriteBrokerError(c, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return WriteBrokerError(c, ErrInvalidMessage)
	}
	id := request.ID.Canonical()
	decodedID, err := decodePartitionTransactionParticipantID(request.ParticipantID)
	if err != nil || decodedID != id || id.Kind != cluster.PartitionResourceQueue ||
		request.ParticipantID != PartitionTransactionParticipantID(id) || !validBrokerTransactionIdentity(request.TransactionID, request.ParticipantID) {
		return WriteBrokerError(c, cluster.ErrInvalidTransaction)
	}
	route, err := a.Distributed.PartitionResourceRoute(c.Context(), id.Kind, id.Topic, id.Partition)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	if !route.LocalLeader {
		return a.writeControllerError(c, NotLeaderError(route.LeaderClientAddress, route.RaftTerm))
	}
	count, found, err := a.Distributed.lookupLocalPartitionTransactionPurgeResult(c.Context(), id, request.TransactionID, request.ParticipantID)
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, partitionPurgeResultResponse{Found: found, Purged: count})
}

func (a AdminServer) internalPartitionLag(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "internal partition stream lag") {
		return nil
	}
	if a.Distributed == nil {
		return WriteBrokerError(c, ErrUnsupportedMode)
	}
	var request partitionLagRequest
	decoder := json.NewDecoder(bytes.NewReader(c.Body()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return WriteBrokerError(c, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return WriteBrokerError(c, ErrInvalidMessage)
	}
	stream, validationErr := sanitizeIdentifier(request.Stream)
	if validationErr != nil || stream != request.Stream || request.Partition < 0 {
		return WriteBrokerError(c, ErrInvalidMessage)
	}
	report, err := a.Distributed.PartitionStreamLag(c.Context(), request.Stream, request.Partition, request.ConsumerGroup)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, report)
}
