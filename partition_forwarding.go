package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/oarkflow/broker/cluster"
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
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, strings.TrimRight(address, "/")+internalPartitionPurgeResultPath, bytes.NewReader(body))
	if err != nil {
		return 0, false, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := db.partitionForwardClient.Do(request)
	if err != nil {
		return 0, false, fmt.Errorf("lookup committed partition purge result: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		data, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
		if readErr != nil {
			return 0, false, fmt.Errorf("partition leader returned %s (read error: %v)", response.Status, readErr)
		}
		return 0, false, fmt.Errorf("partition leader returned %s: %s", response.Status, strings.TrimSpace(string(data)))
	}
	var result partitionPurgeResultResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&result); err != nil {
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
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, strings.TrimRight(address, "/")+internalPartitionStagedPath, bytes.NewReader(body))
	if err != nil {
		return stagedBrokerTransaction{}, false, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := db.partitionForwardClient.Do(request)
	if err != nil {
		return stagedBrokerTransaction{}, false, fmt.Errorf("lookup staged partition transaction: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		data, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
		if readErr != nil {
			return stagedBrokerTransaction{}, false, fmt.Errorf("partition leader returned %s (read error: %v)", response.Status, readErr)
		}
		return stagedBrokerTransaction{}, false, fmt.Errorf("partition leader returned %s: %s", response.Status, strings.TrimSpace(string(data)))
	}
	var result partitionStagedResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, 64<<20)).Decode(&result); err != nil {
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
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, strings.TrimRight(address, "/")+internalPartitionResultPath, bytes.NewReader(body))
	if err != nil {
		return nil, false, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := db.partitionForwardClient.Do(request)
	if err != nil {
		return nil, false, fmt.Errorf("lookup committed partition transaction result: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		data, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
		if readErr != nil {
			return nil, false, fmt.Errorf("partition leader returned %s (read error: %v)", response.Status, readErr)
		}
		return nil, false, fmt.Errorf("partition leader returned %s: %s", response.Status, strings.TrimSpace(string(data)))
	}
	var result partitionResultResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, 64<<20)).Decode(&result); err != nil {
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
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, strings.TrimRight(address, "/")+internalPartitionProposalPath, bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := db.partitionForwardClient.Do(request)
	if err != nil {
		return fmt.Errorf("forward partition proposal to leader: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
		if readErr != nil {
			return fmt.Errorf("partition leader returned %s (read error: %v)", response.Status, readErr)
		}
		return fmt.Errorf("partition leader returned %s: %s", response.Status, strings.TrimSpace(string(body)))
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
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, strings.TrimRight(route.LeaderClientAddress, "/")+internalPartitionPreparePath, bytes.NewReader(encoded))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := db.partitionForwardClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("forward partition transaction prepare to leader: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
		if readErr != nil {
			return nil, fmt.Errorf("partition leader returned %s (read error: %v)", response.Status, readErr)
		}
		return nil, fmt.Errorf("partition leader returned %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	var result partitionTransactionPrepareResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, 64<<20)).Decode(&result); err != nil {
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
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, strings.TrimRight(route.LeaderClientAddress, "/")+internalPartitionOutcomePath, bytes.NewReader(encoded))
	if err != nil {
		return OffsetAppendTransaction{}, false, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := db.partitionForwardClient.Do(request)
	if err != nil {
		return OffsetAppendTransaction{}, false, fmt.Errorf("lookup partition transaction outcome: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
		if readErr != nil {
			return OffsetAppendTransaction{}, false, fmt.Errorf("partition leader returned %s (read error: %v)", response.Status, readErr)
		}
		return OffsetAppendTransaction{}, false, fmt.Errorf("partition leader returned %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	var result partitionTransactionOutcomeResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, 64<<20)).Decode(&result); err != nil {
		return OffsetAppendTransaction{}, false, fmt.Errorf("decode partition transaction outcome: %w", err)
	}
	return result.Transaction, result.Found, nil
}

func (a AdminServer) internalPartitionPropose(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "internal partition proposal") {
		return
	}
	if a.Distributed == nil {
		WriteBrokerError(w, ErrUnsupportedMode)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var request partitionProposalRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		WriteBrokerError(w, err)
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		WriteBrokerError(w, ErrInvalidMessage)
		return
	}
	kind := request.Kind
	if kind == "" {
		kind = cluster.PartitionResourceTopic
	}
	if (kind != cluster.PartitionResourceTopic && kind != cluster.PartitionResourceStream && kind != cluster.PartitionResourceQueue) ||
		request.Name == "" || request.Partition < 0 || request.Epoch == 0 || len(request.Data) == 0 || len(request.Data) > 64<<20 {
		WriteBrokerError(w, ErrInvalidMessage)
		return
	}
	if err := a.Distributed.proposeResourcePartitionFromPeer(r.Context(), kind, request.Name, request.Partition, request.Epoch, request.Data); err != nil {
		a.writeControllerError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a AdminServer) internalPartitionPrepare(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "internal partition transaction prepare") {
		return
	}
	if a.Distributed == nil {
		WriteBrokerError(w, ErrUnsupportedMode)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var request partitionTransactionPrepareRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		WriteBrokerError(w, err)
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		WriteBrokerError(w, ErrInvalidMessage)
		return
	}
	request.ID = request.ID.Canonical()
	participantID := PartitionTransactionParticipantID(request.ID)
	if request.Operation == cluster.BrokerCmdCommitOffsetAppend {
		participantID = PartitionOffsetTransactionParticipantID(request.ID)
	}
	if !validBrokerTransactionIdentity(request.TransactionID, participantID) ||
		request.ID.Topic == "" || request.ID.Partition < 0 || request.Deadline.IsZero() || !json.Valid(request.Payload) || len(request.Payload) > 64<<20 ||
		request.RequestFingerprint != transactionPayloadFingerprint(request.Payload) {
		WriteBrokerError(w, cluster.ErrInvalidTransaction)
		return
	}
	messages, err := a.Distributed.preparePartitionTransactionFromPeer(r.Context(), request.ID, request.TransactionID, request.Deadline, request.Operation, request.Payload, request.RequestFingerprint)
	if err != nil {
		a.writeControllerError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(partitionTransactionPrepareResponse{Messages: messages})
}

func (a AdminServer) internalPartitionOutcome(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "internal partition transaction outcome") {
		return
	}
	if a.Distributed == nil {
		WriteBrokerError(w, ErrUnsupportedMode)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var request partitionTransactionOutcomeRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		WriteBrokerError(w, err)
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		WriteBrokerError(w, ErrInvalidMessage)
		return
	}
	if !request.Commit.Partitioned || request.Commit.Partition < 0 || request.Commit.Stream == "" ||
		request.Commit.TransactionID == "" || !validTransactionPayloadFingerprint(request.Fingerprint) {
		WriteBrokerError(w, cluster.ErrInvalidTransaction)
		return
	}
	id := cluster.PartitionGroupID{Kind: cluster.PartitionResourceStream, Topic: request.Commit.Stream, Partition: request.Commit.Partition}
	route, err := a.Distributed.PartitionResourceRoute(r.Context(), id.Kind, id.Topic, id.Partition)
	if err != nil {
		a.writeControllerError(w, err)
		return
	}
	if !route.LocalLeader {
		a.writeControllerError(w, NotLeaderError(route.LeaderClientAddress, route.RaftTerm))
		return
	}
	store, _, routed, err := a.Distributed.partitionStoreForResourceWrite(r.Context(), id.Kind, id.Topic, id.Partition)
	if err != nil {
		a.writeControllerError(w, err)
		return
	}
	if !routed || store == nil {
		WriteBrokerError(w, ErrBrokerUnavailable)
		return
	}
	lookup, ok := store.(interface {
		lookupOffsetAppendTransaction(OffsetCommit, string) (OffsetAppendTransaction, bool, error)
	})
	if !ok {
		WriteBrokerError(w, fmt.Errorf("%w: partition store cannot query offset-append outcomes", ErrUnsupportedMode))
		return
	}
	transaction, found, err := lookup.lookupOffsetAppendTransaction(request.Commit, request.Fingerprint)
	if err != nil {
		WriteBrokerError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(partitionTransactionOutcomeResponse{Found: found, Transaction: transaction})
}

func (a AdminServer) internalPartitionStaged(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "internal partition transaction lookup") {
		return
	}
	if a.Distributed == nil {
		WriteBrokerError(w, ErrUnsupportedMode)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var request partitionStagedRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		WriteBrokerError(w, err)
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		WriteBrokerError(w, ErrInvalidMessage)
		return
	}
	id := request.ID.Canonical()
	participantID, err := decodePartitionTransactionParticipantID(request.ParticipantID)
	if err != nil || participantID != id || !validBrokerTransactionIdentity(request.TransactionID, request.ParticipantID) {
		WriteBrokerError(w, cluster.ErrInvalidTransaction)
		return
	}
	route, err := a.Distributed.PartitionResourceRoute(r.Context(), id.Kind, id.Topic, id.Partition)
	if err != nil {
		a.writeControllerError(w, err)
		return
	}
	if !route.LocalLeader {
		a.writeControllerError(w, NotLeaderError(route.LeaderClientAddress, route.RaftTerm))
		return
	}
	staged, found, err := a.Distributed.lookupLocalPartitionStagedTransaction(r.Context(), id, request.TransactionID, request.ParticipantID)
	if err != nil {
		a.writeControllerError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(partitionStagedResponse{Found: found, Staged: staged})
}

func (a AdminServer) internalPartitionResult(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "internal partition transaction result") {
		return
	}
	if a.Distributed == nil {
		WriteBrokerError(w, ErrUnsupportedMode)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var request partitionResultRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		WriteBrokerError(w, err)
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		WriteBrokerError(w, ErrInvalidMessage)
		return
	}
	id := request.ID.Canonical()
	participantID, err := decodePartitionTransactionParticipantID(request.ParticipantID)
	if err != nil || participantID != id || id.Kind != cluster.PartitionResourceStream ||
		request.ParticipantID != PartitionTransactionParticipantID(id) || !validBrokerTransactionIdentity(request.TransactionID, request.ParticipantID) {
		WriteBrokerError(w, cluster.ErrInvalidTransaction)
		return
	}
	route, err := a.Distributed.PartitionResourceRoute(r.Context(), id.Kind, id.Topic, id.Partition)
	if err != nil {
		a.writeControllerError(w, err)
		return
	}
	if !route.LocalLeader {
		a.writeControllerError(w, NotLeaderError(route.LeaderClientAddress, route.RaftTerm))
		return
	}
	messages, found, err := a.Distributed.lookupLocalPartitionTransactionResult(r.Context(), id, request.TransactionID, request.ParticipantID)
	if err != nil {
		a.writeControllerError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(partitionResultResponse{Found: found, Messages: messages})
}

func (a AdminServer) internalPartitionPurgeResult(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "internal partition purge result") {
		return
	}
	if a.Distributed == nil {
		WriteBrokerError(w, ErrUnsupportedMode)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var request partitionStagedRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		WriteBrokerError(w, err)
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		WriteBrokerError(w, ErrInvalidMessage)
		return
	}
	id := request.ID.Canonical()
	decodedID, err := decodePartitionTransactionParticipantID(request.ParticipantID)
	if err != nil || decodedID != id || id.Kind != cluster.PartitionResourceQueue ||
		request.ParticipantID != PartitionTransactionParticipantID(id) || !validBrokerTransactionIdentity(request.TransactionID, request.ParticipantID) {
		WriteBrokerError(w, cluster.ErrInvalidTransaction)
		return
	}
	route, err := a.Distributed.PartitionResourceRoute(r.Context(), id.Kind, id.Topic, id.Partition)
	if err != nil {
		a.writeControllerError(w, err)
		return
	}
	if !route.LocalLeader {
		a.writeControllerError(w, NotLeaderError(route.LeaderClientAddress, route.RaftTerm))
		return
	}
	count, found, err := a.Distributed.lookupLocalPartitionTransactionPurgeResult(r.Context(), id, request.TransactionID, request.ParticipantID)
	if err != nil {
		WriteBrokerError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(partitionPurgeResultResponse{Found: found, Purged: count})
}

func (a AdminServer) internalPartitionLag(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "internal partition stream lag") {
		return
	}
	if a.Distributed == nil {
		WriteBrokerError(w, ErrUnsupportedMode)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var request partitionLagRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		WriteBrokerError(w, err)
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		WriteBrokerError(w, ErrInvalidMessage)
		return
	}
	stream, validationErr := sanitizeIdentifier(request.Stream)
	if validationErr != nil || stream != request.Stream || request.Partition < 0 {
		WriteBrokerError(w, ErrInvalidMessage)
		return
	}
	report, err := a.Distributed.PartitionStreamLag(r.Context(), request.Stream, request.Partition, request.ConsumerGroup)
	if err != nil {
		a.writeControllerError(w, err)
		return
	}
	writeBrokerJSON(w, report)
}
