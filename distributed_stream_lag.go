package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/oarkflow/broker/cluster"
)

const internalPartitionLagPath = "/cluster/internal/partition-lag"
const maxAssignedStreamLagPartitions = 10000
const maxAssignedStreamLagFailureBytes = 512

var errNoAssignedStreamPartitions = errors.New("no assigned stream partitions")

// AssignedStreamLag reports lag without treating partition-local offsets as a
// single scalar. TotalLag is additive; each partition retains its own head and
// committed offset.
type AssignedStreamLag struct {
	Stream        string               `json:"stream"`
	ConsumerGroup string               `json:"consumer_group"`
	TotalLag      uint64               `json:"total_lag"`
	Partitions    []PartitionStreamLag `json:"partitions"`
	Failures      map[int]string       `json:"failures,omitempty"`
	Error         string               `json:"error,omitempty"`
}

type partitionLagRequest struct {
	Stream        string `json:"stream"`
	Partition     int    `json:"partition"`
	ConsumerGroup string `json:"consumer_group,omitempty"`
}

// StreamLagAssigned gathers quorum-assigned stream partition reports from
// their current leaders. It is intended for globally scoped admin callers.
func (db *DistributedBroker) StreamLagAssigned(ctx context.Context, stream, consumerGroup string) (*AssignedStreamLag, error) {
	if db == nil || db.controllerQuorum == nil || stream == "" {
		return nil, fmt.Errorf("%w: assigned stream lag requires a distributed stream and name", ErrUnsupportedMode)
	}
	if validated, err := sanitizeIdentifier(stream); err != nil || validated != stream {
		return nil, fmt.Errorf("%w: invalid stream name", ErrInvalidMessage)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	assignments, err := db.controllerQuorum.ListPartitionsLinearizable(ctx)
	if err != nil {
		return nil, err
	}
	partitions := make([]int, 0)
	for _, assignment := range assignments {
		if assignment != nil && assignment.ID.Kind == cluster.PartitionResourceStream && assignment.ID.Topic == stream {
			partitions = append(partitions, assignment.ID.Partition)
		}
	}
	sort.Ints(partitions)
	if len(partitions) == 0 {
		return nil, fmt.Errorf("%w: %w: stream %q has no assigned partitions", ErrUnsupportedMode, errNoAssignedStreamPartitions, stream)
	}
	if len(partitions) > maxAssignedStreamLagPartitions {
		return nil, fmt.Errorf("%w: stream %q has %d partitions; server fan-out limit is %d", ErrUnsupportedMode, stream, len(partitions), maxAssignedStreamLagPartitions)
	}
	type result struct {
		report PartitionStreamLag
		err    error
	}
	results := make([]result, len(partitions))
	jobs := make(chan int)
	workerCount := 16
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
					results[job].err = ctx.Err()
				default:
					results[job].report, results[job].err = db.partitionStreamLagFromLeader(ctx, stream, partitions[job], consumerGroup)
				}
			}
		}()
	}
	for i := range partitions {
		jobs <- i
	}
	close(jobs)
	wait.Wait()
	report := &AssignedStreamLag{Stream: stream, ConsumerGroup: consumerGroup, Partitions: make([]PartitionStreamLag, 0, len(partitions)), Failures: make(map[int]string)}
	var failures []error
	for i, result := range results {
		if result.err != nil {
			report.Failures[partitions[i]] = boundedStreamLagFailure(result.err.Error())
			failures = append(failures, fmt.Errorf("stream %s partition %d: %w", stream, partitions[i], result.err))
			continue
		}
		report.Partitions = append(report.Partitions, result.report)
		if result.report.Lag > ^uint64(0)-report.TotalLag {
			report.Error = "aggregate lag overflows uint64"
			failures = append(failures, fmt.Errorf("stream %s %s", stream, report.Error))
			report.TotalLag = ^uint64(0)
		} else {
			report.TotalLag += result.report.Lag
		}
	}
	return report, errors.Join(failures...)
}

func boundedStreamLagFailure(message string) string {
	if len(message) > maxAssignedStreamLagFailureBytes {
		return message[:maxAssignedStreamLagFailureBytes]
	}
	return message
}

func (db *DistributedBroker) partitionStreamLagFromLeader(ctx context.Context, stream string, partition int, consumerGroup string) (PartitionStreamLag, error) {
	route, err := db.PartitionResourceRoute(ctx, cluster.PartitionResourceStream, stream, partition)
	if err != nil {
		return PartitionStreamLag{}, err
	}
	if route.LocalLeader {
		return db.PartitionStreamLag(ctx, stream, partition, consumerGroup)
	}
	if db.partitionForwardClient == nil || !strings.HasPrefix(strings.ToLower(route.LeaderClientAddress), "https://") {
		return PartitionStreamLag{}, fmt.Errorf("assigned stream lag forwarding requires a configured HTTPS mTLS client")
	}
	payload, err := json.Marshal(partitionLagRequest{Stream: stream, Partition: partition, ConsumerGroup: consumerGroup})
	if err != nil {
		return PartitionStreamLag{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(route.LeaderClientAddress, "/")+internalPartitionLagPath, bytes.NewReader(payload))
	if err != nil {
		return PartitionStreamLag{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := db.partitionForwardClient.Do(request)
	if err != nil {
		return PartitionStreamLag{}, fmt.Errorf("forward stream lag request to partition leader: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
		if readErr != nil {
			return PartitionStreamLag{}, fmt.Errorf("partition leader returned %s (read error: %v)", response.Status, readErr)
		}
		return PartitionStreamLag{}, fmt.Errorf("partition leader returned %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	var report PartitionStreamLag
	if err := decodeStrictBoundedJSON(response.Body, 1<<20, &report); err != nil {
		return PartitionStreamLag{}, fmt.Errorf("decode partition stream lag: %w", err)
	}
	if report.Stream != stream || report.Partition != partition || report.ConsumerGroup != consumerGroup {
		return PartitionStreamLag{}, fmt.Errorf("partition leader returned a mismatched stream lag report")
	}
	return report, nil
}
