package broker

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/broker/cluster"
	"github.com/oarkflow/fh"
)

const internalPartitionQueueListPath = "/cluster/internal/partition-queue-list"
const internalPartitionQueueControlPath = "/cluster/internal/partition-queue-control"
const maxAssignedQueueListPartitions = 10000
const maxAssignedQueueListMessages = 10000
const maxAssignedQueuePartitionResponseBytes = 24 << 20
const maxAssignedQueueMessageBytes = 48 << 20
const maxAssignedQueueFailureBytes = 512

// AssignedQueueMessages is a bounded first-page sample across assigned queue
// partitions. A successful partition can be returned alongside failures from
// other partitions; this is not a global FIFO cursor page.
type AssignedQueueMessages struct {
	Queue      string         `json:"queue"`
	Partitions []int          `json:"partitions"`
	Completed  []int          `json:"completed"`
	Failures   map[int]string `json:"failures,omitempty"`
	Messages   []Message      `json:"messages"`
}

// QueuePartitionOperationResult reports completion across assigned partitions.
// Pause and resume include a durable transaction ID; purge reports each
// partition's exact apply count.
type QueuePartitionOperationResult struct {
	Partitions    []int          `json:"partitions"`
	Completed     []int          `json:"completed"`
	Failures      map[int]string `json:"failures,omitempty"`
	Purged        map[int]int    `json:"purged,omitempty"`
	TransactionID string         `json:"transaction_id,omitempty"`
}

type partitionQueueListRequest struct {
	Queue     string `json:"queue"`
	Partition int    `json:"partition"`
	Limit     int    `json:"limit"`
}

type partitionQueueListResponse struct {
	Messages []Message `json:"messages"`
}

type partitionQueueControlRequest struct {
	Queue     string `json:"queue"`
	Partition int    `json:"partition"`
	Action    string `json:"action"`
}

type partitionQueueControlResponse struct {
	Partition int `json:"partition"`
	Purged    int `json:"purged,omitempty"`
}

// ListAssignedQueue returns a bounded, best-effort sample from assigned queue
// partitions. Each partition receives a rotating share of the requested
// limit, and failures are reported per partition without discarding successes.
func (db *DistributedBroker) ListAssignedQueue(ctx context.Context, queue string, limit int) (*AssignedQueueMessages, error) {
	if db == nil || db.controllerQuorum == nil {
		return nil, ErrUnsupportedMode
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if validated, err := sanitizeIdentifier(queue); err != nil || validated != queue {
		return nil, fmt.Errorf("%w: invalid queue name", ErrInvalidMessage)
	}
	if limit <= 0 {
		limit = 100
	} else if limit > maxAssignedQueueListMessages {
		limit = maxAssignedQueueListMessages
	}
	assignments, err := db.controllerQuorum.ListPartitionsLinearizable(ctx)
	if err != nil {
		return nil, err
	}
	partitions := make([]int, 0)
	for _, assignment := range assignments {
		if assignment != nil && assignment.ID.Kind == cluster.PartitionResourceQueue && assignment.ID.Topic == queue {
			partitions = append(partitions, assignment.ID.Partition)
		}
	}
	sort.Ints(partitions)
	if len(partitions) == 0 {
		return nil, fmt.Errorf("%w: queue %q has no assigned partitions", ErrUnsupportedMode, queue)
	}
	if len(partitions) > maxAssignedQueueListPartitions {
		return nil, fmt.Errorf("%w: queue %q has %d partitions; server fan-out limit is %d", ErrUnsupportedMode, queue, len(partitions), maxAssignedQueueListPartitions)
	}
	start := int(db.queueListSequence.Add(1) % uint64(len(partitions)))
	quotient, remainder := limit/len(partitions), limit%len(partitions)
	type result struct {
		partition int
		queried   bool
		messages  []Message
		err       error
	}
	results := make([]result, len(partitions))
	type job struct {
		index     int
		partition int
		budget    int
	}
	jobs := make(chan job)
	workerCount := 4
	if len(partitions) < workerCount {
		workerCount = len(partitions)
	}
	var wait sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for job := range jobs {
				select {
				case <-ctx.Done():
					results[job.index].err = ctx.Err()
				default:
					results[job.index].messages, results[job.index].err = db.listQueuePartitionFromLeader(ctx, queue, job.partition, job.budget)
				}
			}
		}()
	}
	for i, partition := range partitions {
		budget := quotient
		if (i-start+len(partitions))%len(partitions) < remainder {
			budget++
		}
		results[i].partition = partition
		if budget == 0 {
			continue
		}
		results[i].queried = true
		jobs <- job{index: i, partition: partition, budget: budget}
	}
	close(jobs)
	wait.Wait()
	report := &AssignedQueueMessages{
		Queue: queue, Partitions: append([]int(nil), partitions...),
		Failures: make(map[int]string), Messages: make([]Message, 0, limit),
	}
	var failures []error
	messageBytes := 0
	for _, result := range results {
		if !result.queried {
			continue
		}
		if result.err != nil {
			report.Failures[result.partition] = boundedQueueFailure(result.err.Error())
			failures = append(failures, fmt.Errorf("queue %s partition %d: %w", queue, result.partition, result.err))
			continue
		}
		encoded, encodeErr := json.Marshal(result.messages)
		if encodeErr != nil || len(encoded) > maxAssignedQueuePartitionResponseBytes || len(encoded) > maxAssignedQueueMessageBytes-messageBytes {
			failure := fmt.Errorf("partition response exceeds assigned queue listing size limit")
			report.Failures[result.partition] = failure.Error()
			failures = append(failures, fmt.Errorf("queue %s partition %d: %w", queue, result.partition, failure))
			continue
		}
		messageBytes += len(encoded)
		report.Completed = append(report.Completed, result.partition)
		report.Messages = append(report.Messages, result.messages...)
	}
	sort.SliceStable(report.Messages, func(i, j int) bool {
		if report.Messages[i].CreatedAt.Equal(report.Messages[j].CreatedAt) {
			if report.Messages[i].Partition == report.Messages[j].Partition {
				return report.Messages[i].ID < report.Messages[j].ID
			}
			return report.Messages[i].Partition < report.Messages[j].Partition
		}
		return report.Messages[i].CreatedAt.Before(report.Messages[j].CreatedAt)
	})
	return report, errors.Join(failures...)
}

// ApplyAssignedQueueControl applies pause, resume, and purge through one durable
// coordinator decision across every assigned partition.
func (db *DistributedBroker) ApplyAssignedQueueControl(ctx context.Context, queue, action string) (*QueuePartitionOperationResult, error) {
	if db == nil || db.controllerQuorum == nil {
		return nil, ErrUnsupportedMode
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if validated, err := sanitizeIdentifier(queue); err != nil || validated != queue {
		return nil, fmt.Errorf("%w: invalid queue name", ErrInvalidMessage)
	}
	if action != "pause" && action != "resume" && action != "purge" {
		return nil, fmt.Errorf("%w: queue action must be pause, resume, or purge", ErrInvalidMessage)
	}
	return db.applyAssignedQueueControlTransaction(ctx, queue, action)
}

func (db *DistributedBroker) applyAssignedQueueControlTransaction(ctx context.Context, queue, action string) (*QueuePartitionOperationResult, error) {
	assignments, err := db.controllerQuorum.ListPartitionsLinearizable(ctx)
	if err != nil {
		return nil, err
	}
	partitions := make([]int, 0)
	for _, assignment := range assignments {
		if assignment != nil && assignment.ID.Kind == cluster.PartitionResourceQueue && assignment.ID.Topic == queue {
			partitions = append(partitions, assignment.ID.Partition)
		}
	}
	sort.Ints(partitions)
	if len(partitions) == 0 {
		return nil, fmt.Errorf("%w: queue %q has no assigned partitions", ErrUnsupportedMode, queue)
	}
	if len(partitions) > maxAssignedQueueListPartitions {
		return nil, fmt.Errorf("%w: queue %q has %d partitions; server fan-out limit is %d", ErrUnsupportedMode, queue, len(partitions), maxAssignedQueueListPartitions)
	}
	result := &QueuePartitionOperationResult{Partitions: append([]int(nil), partitions...), Failures: make(map[int]string)}
	identity := make([]byte, 16)
	if _, err := rand.Read(identity); err != nil {
		return nil, fmt.Errorf("generate queue control transaction ID: %w", err)
	}
	result.TransactionID = "broker-queue-control:" + hex.EncodeToString(identity)
	if action == "purge" {
		result.Purged = make(map[int]int, len(partitions))
	}
	fingerprintPayload, err := json.Marshal(struct {
		Queue      string `json:"queue"`
		Action     string `json:"action"`
		Partitions []int  `json:"partitions"`
	}{Queue: queue, Action: action, Partitions: partitions})
	if err != nil {
		return nil, err
	}
	fingerprint := transactionPayloadFingerprint(fingerprintPayload)
	participantIDs := make([]string, 0, len(partitions))
	participants := make(map[string]cluster.TransactionParticipant, len(partitions))
	for _, partition := range partitions {
		id := cluster.PartitionGroupID{Kind: cluster.PartitionResourceQueue, Topic: queue, Partition: partition}.Canonical()
		participant, err := db.newPartitionQueueControlTransactionParticipant(ctx, id, action)
		if err != nil {
			return nil, err
		}
		participantID := PartitionTransactionParticipantID(id)
		participants[participantID] = participant
		participantIDs = append(participantIDs, participantID)
	}
	sort.Strings(participantIDs)
	transactionTimeout := db.distCfg.TransactionTimeout
	if transactionTimeout <= 0 {
		transactionTimeout = 30 * time.Second
	}
	deadline := time.Now().Add(transactionTimeout)
	if err := db.controllerQuorum.BeginTransactionWithFingerprint(ctx, result.TransactionID, participantIDs, deadline, fingerprint); err != nil {
		return nil, err
	}
	if err := db.controllerQuorum.ExecuteTransactionWithFingerprint(ctx, result.TransactionID, participants, deadline, fingerprint); err != nil {
		for _, partition := range partitions {
			result.Failures[partition] = boundedQueueFailure(err.Error())
		}
		return result, err
	}
	result.Completed = append(result.Completed, partitions...)
	if action == "purge" {
		var failures []error
		for _, partition := range partitions {
			id := cluster.PartitionGroupID{Kind: cluster.PartitionResourceQueue, Topic: queue, Partition: partition}.Canonical()
			participantID := PartitionTransactionParticipantID(id)
			participant, ok := participants[participantID].(interface {
				PurgedCount(context.Context, string) (int, bool, error)
			})
			if !ok {
				err := fmt.Errorf("queue %s partition %d has no purge result reader", queue, partition)
				result.Failures[partition] = boundedQueueFailure(err.Error())
				failures = append(failures, err)
				continue
			}
			count, found, err := participant.PurgedCount(ctx, result.TransactionID)
			if err == nil && !found {
				err = fmt.Errorf("committed purge result for queue %s partition %d is unavailable", queue, partition)
			}
			if err != nil {
				result.Failures[partition] = boundedQueueFailure(err.Error())
				failures = append(failures, err)
				continue
			}
			result.Purged[partition] = count
		}
		if err := errors.Join(failures...); err != nil {
			return result, err
		}
	}
	return result, nil
}

func (db *DistributedBroker) queueControlFromLeader(ctx context.Context, queue string, partition int, action string) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	route, err := db.PartitionResourceRoute(ctx, cluster.PartitionResourceQueue, queue, partition)
	if err != nil {
		return 0, err
	}
	if route.LocalLeader {
		return db.applyQueueControlOnLeader(ctx, queue, partition, action)
	}
	if db.partitionForwardClient == nil || !strings.HasPrefix(strings.ToLower(route.LeaderClientAddress), "https://") {
		return 0, fmt.Errorf("assigned queue control requires a configured HTTPS mTLS client")
	}
	requestBody, err := json.Marshal(partitionQueueControlRequest{Queue: queue, Partition: partition, Action: action})
	if err != nil {
		return 0, err
	}
	response, err := SendHTTP(ctx, db.partitionForwardClient, fh.MethodPOST, strings.TrimRight(route.LeaderClientAddress, "/")+internalPartitionQueueControlPath, requestBody, jsonRequestHeaders)
	if err != nil {
		return 0, fmt.Errorf("forward queue control to partition leader: %w", err)
	}
	defer response.Close()
	if response.StatusCode() != fh.StatusOK {
		body, readErr := io.ReadAll(io.LimitReader(response.Reader(), 4096))
		if readErr != nil {
			return 0, fmt.Errorf("partition leader returned %s (read error: %v)", response.Raw.Status, readErr)
		}
		return 0, fmt.Errorf("partition leader returned %s: %s", response.Raw.Status, strings.TrimSpace(string(body)))
	}
	var result partitionQueueControlResponse
	if err := decodeStrictBoundedJSON(response.Reader(), 1<<20, &result); err != nil {
		return 0, fmt.Errorf("decode partition queue control result: %w", err)
	}
	if result.Partition != partition || (action != "purge" && result.Purged != 0) {
		return 0, fmt.Errorf("partition leader returned a mismatched queue control result")
	}
	return result.Purged, nil
}

func (db *DistributedBroker) applyQueueControlOnLeader(ctx context.Context, queue string, partition int, action string) (int, error) {
	switch action {
	case "pause":
		return 0, db.PausePartitionQueue(ctx, queue, partition)
	case "resume":
		return 0, db.ResumePartitionQueue(ctx, queue, partition)
	case "purge":
		return db.purgePartitionQueueOnLeader(ctx, queue, partition)
	default:
		return 0, ErrInvalidMessage
	}
}

func (db *DistributedBroker) listQueuePartitionFromLeader(ctx context.Context, queue string, partition, limit int) ([]Message, error) {
	route, err := db.PartitionResourceRoute(ctx, cluster.PartitionResourceQueue, queue, partition)
	if err != nil {
		return nil, err
	}
	if route.LocalLeader {
		messages, err := db.ListPartitionQueue(ctx, queue, partition, limit)
		if err != nil {
			return nil, err
		}
		return validatePartitionQueueMessages(messages, queue, partition, limit)
	}
	if db.partitionForwardClient == nil || !strings.HasPrefix(strings.ToLower(route.LeaderClientAddress), "https://") {
		return nil, fmt.Errorf("assigned queue listing requires a configured HTTPS mTLS client")
	}
	payload, err := json.Marshal(partitionQueueListRequest{Queue: queue, Partition: partition, Limit: limit})
	if err != nil {
		return nil, err
	}
	response, err := SendHTTP(ctx, db.partitionForwardClient, fh.MethodPOST, strings.TrimRight(route.LeaderClientAddress, "/")+internalPartitionQueueListPath, payload, jsonRequestHeaders)
	if err != nil {
		return nil, fmt.Errorf("forward queue listing to partition leader: %w", err)
	}
	defer response.Close()
	if response.StatusCode() != fh.StatusOK {
		body, readErr := io.ReadAll(io.LimitReader(response.Reader(), 4096))
		if readErr != nil {
			return nil, fmt.Errorf("partition leader returned %s (read error: %v)", response.Raw.Status, readErr)
		}
		return nil, fmt.Errorf("partition leader returned %s: %s", response.Raw.Status, strings.TrimSpace(string(body)))
	}
	var result partitionQueueListResponse
	if err := decodeStrictBoundedJSON(response.Reader(), maxAssignedQueuePartitionResponseBytes, &result); err != nil {
		return nil, fmt.Errorf("decode partition queue listing: %w", err)
	}
	return validatePartitionQueueMessages(result.Messages, queue, partition, limit)
}

func validatePartitionQueueMessages(messages []Message, queue string, partition, limit int) ([]Message, error) {
	if len(messages) > limit {
		return nil, fmt.Errorf("partition leader returned more than the requested queue page size")
	}
	for _, message := range messages {
		if message.Queue != queue || message.Partition != partition {
			return nil, fmt.Errorf("partition leader returned a message from another queue partition")
		}
	}
	return messages, nil
}

func (a AdminServer) internalPartitionQueueList(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "internal partition queue listing") {
		return nil
	}
	if a.Distributed == nil {
		return WriteBrokerError(c, ErrUnsupportedMode)
	}
	var request partitionQueueListRequest
	decoder := json.NewDecoder(bytes.NewReader(c.Body()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return WriteBrokerError(c, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return WriteBrokerError(c, ErrInvalidMessage)
	}
	queue, validationErr := sanitizeIdentifier(request.Queue)
	if validationErr != nil || queue != request.Queue || request.Partition < 0 || request.Limit <= 0 || request.Limit > maxAssignedQueueListMessages {
		return WriteBrokerError(c, ErrInvalidMessage)
	}
	messages, err := a.Distributed.ListPartitionQueue(c.Context(), queue, request.Partition, request.Limit)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	encoded, err := json.Marshal(partitionQueueListResponse{Messages: messages})
	if err != nil {
		return WriteBrokerError(c, err)
	}
	if len(encoded) > maxAssignedQueuePartitionResponseBytes {
		return WriteBrokerError(c, ErrMessageTooLarge)
	}
	c.Set("Content-Type", "application/json")
	return c.Send(encoded)
}

func (a AdminServer) internalPartitionQueueControl(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "internal partition queue control") {
		return nil
	}
	if a.Distributed == nil {
		return WriteBrokerError(c, ErrUnsupportedMode)
	}
	var request partitionQueueControlRequest
	decoder := json.NewDecoder(bytes.NewReader(c.Body()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return WriteBrokerError(c, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return WriteBrokerError(c, ErrInvalidMessage)
	}
	queue, validationErr := sanitizeIdentifier(request.Queue)
	if validationErr != nil || queue != request.Queue || request.Partition < 0 ||
		(request.Action != "pause" && request.Action != "resume" && request.Action != "purge") {
		return WriteBrokerError(c, ErrInvalidMessage)
	}
	route, err := a.Distributed.PartitionResourceRoute(c.Context(), cluster.PartitionResourceQueue, queue, request.Partition)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	if !route.LocalLeader {
		return a.writeControllerError(c, NotLeaderError(route.LeaderClientAddress, route.RaftTerm))
	}
	purged, err := a.Distributed.applyQueueControlOnLeader(c.Context(), queue, request.Partition, request.Action)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, partitionQueueControlResponse{Partition: request.Partition, Purged: purged})
}

func boundedQueueFailure(message string) string {
	if len(message) > maxAssignedQueueFailureBytes {
		return message[:maxAssignedQueueFailureBytes]
	}
	return message
}
