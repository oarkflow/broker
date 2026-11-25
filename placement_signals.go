package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/broker/cluster"
	"github.com/oarkflow/fh"
)

const internalPlacementSignalsPath = "/cluster/internal/placement-signals"
const localPlacementSignalsCacheTTL = time.Second
const maxPlacementSignalAge = 10 * time.Second

type localPlacementSignalsCacheEntry struct {
	groupKey           string
	fetchedAt          time.Time
	workloadKnown      bool
	workload           StoreStats
	partitionWorkload  []PlacementPartitionWorkload
	partitionDiskUsage []PlacementPartitionDiskUsage
	partitionDiskKnown bool
	diskUsage          StorageUsage
	diskKnown          bool
}

type PlacementPartitionWorkload struct {
	ID       cluster.PartitionGroupID `json:"id"`
	Messages int                      `json:"messages"`
	Leased   int                      `json:"leased"`
}

type PlacementPartitionDiskUsage struct {
	ID        cluster.PartitionGroupID `json:"id"`
	UsedBytes uint64                   `json:"used_bytes"`
}

// PlacementNodeSignals is one broker's point-in-time view of its local
// partition stores and the partitions it currently leads.
type PlacementNodeSignals struct {
	NodeID             string                        `json:"node_id"`
	ObservedAt         time.Time                     `json:"observed_at"`
	HostedPartitions   int                           `json:"hosted_partitions"`
	WorkloadKnown      bool                          `json:"workload_known"`
	WorkloadObservedAt time.Time                     `json:"workload_observed_at,omitempty"`
	WorkloadMessages   int                           `json:"workload_messages"`
	WorkloadLeased     int                           `json:"workload_leased"`
	PartitionWorkload  []PlacementPartitionWorkload  `json:"partition_workload"`
	DiskUsageKnown     bool                          `json:"disk_usage_known"`
	PartitionDiskKnown bool                          `json:"partition_disk_known"`
	PartitionDiskUsage []PlacementPartitionDiskUsage `json:"partition_disk_usage"`
	DiskObservedAt     time.Time                     `json:"disk_observed_at,omitempty"`
	DiskUsedBytes      uint64                        `json:"disk_used_bytes"`
	DiskAvailableBytes uint64                        `json:"disk_available_bytes"`
	DiskCapacityBytes  uint64                        `json:"disk_capacity_bytes"`
	PartitionProgress  []cluster.PartitionProgress   `json:"partition_progress"`
}

type PlacementNodeLoadSignals struct {
	WorkloadKnown          bool
	WorkloadMessages       int
	WorkloadLeased         int
	DiskUsageKnown         bool
	DiskUsedBytes          uint64
	DiskAvailableBytes     uint64
	DiskCapacityBytes      uint64
	PartitionProgressKnown bool
	PartitionRecentActive  bool
	PartitionInSync        bool
	PartitionLag           uint64
	PartitionWorkloads     map[string]StoreStats
	PartitionDiskUsage     map[string]uint64
}

func (db *DistributedBroker) localPlacementSignals(ctx context.Context) (PlacementNodeSignals, error) {
	if db == nil || db.raftCtrl == nil || db.controllerQuorum == nil {
		return PlacementNodeSignals{}, ErrUnsupportedMode
	}
	peerID := db.distCfg.RaftNodeID
	if peerID == 0 {
		peerID = 1
	}
	address, ok := db.raftCtrl.PeerAddress(peerID)
	if !ok || address == "" {
		return PlacementNodeSignals{}, fmt.Errorf("local Raft peer address is unavailable")
	}
	snapshot, err := db.controllerQuorum.TopologySnapshotLinearizable(ctx)
	if err != nil {
		return PlacementNodeSignals{}, err
	}
	nodeID := ""
	for _, node := range snapshot.Nodes {
		if node != nil && node.Address == address {
			nodeID = node.ID
			break
		}
	}
	if nodeID == "" {
		return PlacementNodeSignals{}, fmt.Errorf("local Raft peer is not registered in controller metadata")
	}

	db.startMu.Lock()
	groups := make(map[string]*cluster.PartitionGroup, len(db.partitionGroups))
	stores := make(map[string]Store, len(db.partitionStores))
	for id, group := range db.partitionGroups {
		groups[id] = group
	}
	for id, store := range db.partitionStores {
		stores[id] = store
	}
	db.startMu.Unlock()

	groupIDs := make([]string, 0, len(groups))
	for groupID := range groups {
		groupIDs = append(groupIDs, groupID)
	}
	sort.Strings(groupIDs)
	groupKey := strings.Join(groupIDs, "\x00")
	db.localPlacementSignalsMu.Lock()
	cached := db.localPlacementSignalsCache
	cacheHit := cached != nil && cached.groupKey == groupKey && time.Since(cached.fetchedAt) <= localPlacementSignalsCacheTTL
	db.localPlacementSignalsMu.Unlock()
	var workload StoreStats
	var partitionWorkload []PlacementPartitionWorkload
	var partitionDiskUsage []PlacementPartitionDiskUsage
	partitionUsageByGroup := make(map[string]StorageUsage, len(groupIDs))
	workloadKnown := true
	partitionDiskKnown := true
	var usage StorageUsage
	usageKnown := false
	metricsObservedAt := time.Now().UTC()
	if cacheHit {
		workload = cached.workload
		partitionWorkload = append([]PlacementPartitionWorkload(nil), cached.partitionWorkload...)
		partitionDiskUsage = append([]PlacementPartitionDiskUsage(nil), cached.partitionDiskUsage...)
		workloadKnown = cached.workloadKnown
		partitionDiskKnown = cached.partitionDiskKnown
		usage = cached.diskUsage
		usageKnown = cached.diskKnown
		metricsObservedAt = cached.fetchedAt.UTC()
	} else {
		for _, groupID := range groupIDs {
			store := stores[groupID]
			if store == nil {
				workloadKnown = false
				partitionDiskKnown = false
				continue
			}
			stats, statsErr := store.Stats(ctx)
			if statsErr != nil {
				return PlacementNodeSignals{}, fmt.Errorf("read placement workload for %s: %w", groupID, statsErr)
			}
			workload.Messages += stats.Messages
			workload.Leased += stats.Leased
			partitionWorkload = append(partitionWorkload, PlacementPartitionWorkload{
				ID: groups[groupID].ID().Canonical(), Messages: stats.Messages, Leased: stats.Leased,
			})
			if reporter, ok := store.(StorageUsageReporter); ok {
				partitionUsage, usageErr := reporter.StorageUsage(ctx)
				if usageErr != nil || partitionUsage.Location == "" || partitionUsage.VolumeID == "" {
					partitionDiskKnown = false
				} else {
					partitionUsageByGroup[groupID] = partitionUsage
					partitionDiskUsage = append(partitionDiskUsage, PlacementPartitionDiskUsage{
						ID: groups[groupID].ID().Canonical(), UsedBytes: partitionUsage.UsedBytes,
					})
				}
			} else {
				partitionDiskKnown = false
			}
		}
		sort.Slice(partitionWorkload, func(i, j int) bool {
			return partitionWorkload[i].ID.String() < partitionWorkload[j].ID.String()
		})
		sort.Slice(partitionDiskUsage, func(i, j int) bool {
			return partitionDiskUsage[i].ID.String() < partitionDiskUsage[j].ID.String()
		})
		usage, usageKnown = db.localPartitionStorageUsage(ctx, groups, stores, partitionUsageByGroup)
		metricsObservedAt = time.Now().UTC()
		cacheEntry := &localPlacementSignalsCacheEntry{
			groupKey: groupKey, fetchedAt: metricsObservedAt, workloadKnown: workloadKnown,
			workload: workload, partitionWorkload: append([]PlacementPartitionWorkload(nil), partitionWorkload...),
			partitionDiskUsage: append([]PlacementPartitionDiskUsage(nil), partitionDiskUsage...),
			partitionDiskKnown: partitionDiskKnown, diskUsage: usage, diskKnown: usageKnown,
		}
		db.localPlacementSignalsMu.Lock()
		db.localPlacementSignalsCache = cacheEntry
		db.localPlacementSignalsMu.Unlock()
	}
	report := PlacementNodeSignals{
		NodeID: nodeID, ObservedAt: time.Now().UTC(), HostedPartitions: len(groups),
		WorkloadKnown: workloadKnown, WorkloadMessages: workload.Messages,
		WorkloadLeased: workload.Leased, DiskUsageKnown: usageKnown,
		PartitionDiskKnown: partitionDiskKnown, PartitionDiskUsage: partitionDiskUsage,
		PartitionProgress: make([]cluster.PartitionProgress, 0), PartitionWorkload: partitionWorkload,
	}
	if usageKnown {
		report.DiskObservedAt = metricsObservedAt
		report.DiskUsedBytes = usage.UsedBytes
		report.DiskAvailableBytes = usage.AvailableBytes
		report.DiskCapacityBytes = usage.CapacityBytes
	}
	if workloadKnown {
		report.WorkloadObservedAt = metricsObservedAt
	}
	for _, progress := range db.LivePartitionProgress() {
		report.PartitionProgress = append(report.PartitionProgress, progress)
	}
	sort.Slice(report.PartitionProgress, func(i, j int) bool {
		return report.PartitionProgress[i].ID.String() < report.PartitionProgress[j].ID.String()
	})
	return report, nil
}

func (db *DistributedBroker) localPartitionStorageUsage(ctx context.Context, groups map[string]*cluster.PartitionGroup, stores map[string]Store, reported map[string]StorageUsage) (StorageUsage, bool) {
	if db.distCfg.PartitionStoragePath != "" {
		usage, err := MeasurePathStorageUsage(ctx, db.distCfg.PartitionStoragePath)
		return usage, err == nil
	}
	if len(groups) == 0 {
		return StorageUsage{}, false
	}
	byLocation := make(map[string]StorageUsage, len(groups))
	byVolume := make(map[string]StorageUsage, len(groups))
	for groupID := range groups {
		usage, ok := reported[groupID]
		if !ok || usage.Location == "" || usage.VolumeID == "" {
			return StorageUsage{}, false
		}
		byLocation[usage.Location] = usage
		if prior, exists := byVolume[usage.VolumeID]; !exists {
			byVolume[usage.VolumeID] = usage
		} else {
			if usage.AvailableBytes < prior.AvailableBytes {
				prior.AvailableBytes = usage.AvailableBytes
			}
			if usage.CapacityBytes > prior.CapacityBytes {
				prior.CapacityBytes = usage.CapacityBytes
			}
			byVolume[usage.VolumeID] = prior
		}
	}
	total := StorageUsage{}
	for _, usage := range byLocation {
		total.UsedBytes = saturatingAddUint64(total.UsedBytes, usage.UsedBytes)
	}
	for _, usage := range byVolume {
		total.AvailableBytes = saturatingAddUint64(total.AvailableBytes, usage.AvailableBytes)
		total.CapacityBytes = saturatingAddUint64(total.CapacityBytes, usage.CapacityBytes)
	}
	return total, true
}

func (a AdminServer) internalPlacementSignals(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "internal placement signals") {
		return nil
	}
	if a.Distributed == nil {
		return WriteBrokerError(c, ErrUnsupportedMode)
	}
	report, err := a.Distributed.localPlacementSignals(c.Context())
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, report)
}

// gatherPlacementSignals uses the broker-to-broker mTLS client to collect a
// complete, authenticated view. Partial responses are discarded so a failed
// node cannot appear artificially idle or caught up.
func (db *DistributedBroker) gatherPlacementSignals(
	ctx context.Context,
	nodes []*cluster.NodeInfo,
	partitions []*cluster.PartitionMetadata,
) (map[string]PlacementNodeLoadSignals, bool, bool, bool, bool) {
	if db.partitionForwardClient == nil || len(nodes) == 0 {
		return nil, false, false, false, false
	}
	for _, node := range nodes {
		if node == nil || !strings.HasPrefix(strings.ToLower(node.ClientAddress), "https://") {
			return nil, false, false, false, false
		}
	}
	reports := make(map[string]PlacementNodeSignals, len(nodes))

	type result struct {
		nodeID string
		report PlacementNodeSignals
		err    error
	}
	results := make(chan result, len(nodes))
	workerCount := minInt(16, len(nodes))
	jobs := make(chan *cluster.NodeInfo, workerCount)
	var workers sync.WaitGroup
	for range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for node := range jobs {
				report, err := db.requestPlacementSignals(ctx, node)
				results <- result{nodeID: node.ID, report: report, err: err}
			}
		}()
	}
	queueCanceled := false
	for _, node := range nodes {
		select {
		case jobs <- node:
		case <-ctx.Done():
			queueCanceled = true
		}
		if queueCanceled {
			break
		}
	}
	close(jobs)
	workers.Wait()
	close(results)
	if queueCanceled {
		return nil, false, false, false, false
	}
	for result := range results {
		if result.err != nil {
			return nil, false, false, false, false
		}
		reports[result.nodeID] = result.report
	}
	return aggregatePlacementSignals(nodes, partitions, reports)
}

func aggregatePlacementSignals(
	nodes []*cluster.NodeInfo,
	partitions []*cluster.PartitionMetadata,
	reports map[string]PlacementNodeSignals,
) (map[string]PlacementNodeLoadSignals, bool, bool, bool, bool) {
	if len(reports) != len(nodes) {
		return nil, false, false, false, false
	}

	loads := make(map[string]PlacementNodeLoadSignals, len(nodes))
	workloadComplete := true
	diskComplete := true
	partitionDiskComplete := true
	expectedHosted := make(map[string]int, len(nodes))
	expectedHostedPartitions := make(map[string]map[string]struct{}, len(nodes))
	for _, node := range nodes {
		expectedHosted[node.ID] = 0
		expectedHostedPartitions[node.ID] = make(map[string]struct{})
	}
	for _, partition := range partitions {
		if partition.Reassignment != nil {
			workloadComplete = false
		}
		for _, replica := range partition.Replicas {
			if _, candidate := expectedHosted[replica]; candidate {
				expectedHosted[replica]++
				expectedHostedPartitions[replica][partition.ID.Canonical().String()] = struct{}{}
			}
		}
	}
	for _, node := range nodes {
		report := reports[node.ID]
		if !placementSignalFresh(report.ObservedAt, time.Now()) {
			return nil, false, false, false, false
		}
		if report.WorkloadKnown && placementSignalFresh(report.WorkloadObservedAt, time.Now()) && report.WorkloadMessages >= 0 && report.WorkloadLeased >= 0 &&
			report.HostedPartitions == expectedHosted[node.ID] {
			partitionWorkloads := make(map[string]StoreStats, len(expectedHostedPartitions[node.ID]))
			validPartitionWorkload := len(report.PartitionWorkload) == len(expectedHostedPartitions[node.ID])
			var workloadMessages, workloadLeased uint64
			for _, partitionWorkload := range report.PartitionWorkload {
				key := partitionWorkload.ID.Canonical().String()
				if _, expected := expectedHostedPartitions[node.ID][key]; !expected || partitionWorkload.Messages < 0 || partitionWorkload.Leased < 0 {
					validPartitionWorkload = false
					break
				}
				if _, duplicate := partitionWorkloads[key]; duplicate {
					validPartitionWorkload = false
					break
				}
				partitionWorkloads[key] = StoreStats{Messages: partitionWorkload.Messages, Leased: partitionWorkload.Leased}
				workloadMessages = saturatingAddUint64(workloadMessages, uint64(partitionWorkload.Messages))
				workloadLeased = saturatingAddUint64(workloadLeased, uint64(partitionWorkload.Leased))
			}
			if workloadMessages != uint64(report.WorkloadMessages) || workloadLeased != uint64(report.WorkloadLeased) {
				validPartitionWorkload = false
			}
			if !validPartitionWorkload {
				workloadComplete = false
			} else {
				loads[node.ID] = PlacementNodeLoadSignals{
					WorkloadKnown: true, WorkloadMessages: report.WorkloadMessages,
					WorkloadLeased: report.WorkloadLeased, PartitionWorkloads: partitionWorkloads,
				}
			}
		} else {
			workloadComplete = false
		}
		if report.DiskUsageKnown && placementSignalFresh(report.DiskObservedAt, time.Now()) && report.DiskCapacityBytes >= report.DiskAvailableBytes {
			load := loads[node.ID]
			load.DiskUsageKnown = true
			load.DiskUsedBytes = report.DiskUsedBytes
			load.DiskAvailableBytes = report.DiskAvailableBytes
			load.DiskCapacityBytes = report.DiskCapacityBytes
			partitionDisk := make(map[string]uint64, len(expectedHostedPartitions[node.ID]))
			validPartitionDisk := report.PartitionDiskKnown && len(report.PartitionDiskUsage) == len(expectedHostedPartitions[node.ID])
			var partitionDiskBytes uint64
			for _, usage := range report.PartitionDiskUsage {
				key := usage.ID.Canonical().String()
				if _, expected := expectedHostedPartitions[node.ID][key]; !expected {
					validPartitionDisk = false
					break
				}
				if _, duplicate := partitionDisk[key]; duplicate {
					validPartitionDisk = false
					break
				}
				partitionDisk[key] = usage.UsedBytes
				partitionDiskBytes = saturatingAddUint64(partitionDiskBytes, usage.UsedBytes)
			}
			if partitionDiskBytes > report.DiskUsedBytes {
				validPartitionDisk = false
			}
			if validPartitionDisk {
				load.PartitionDiskUsage = partitionDisk
			} else {
				partitionDiskComplete = false
			}
			loads[node.ID] = load
		} else {
			diskComplete = false
			partitionDiskComplete = false
		}
	}
	if !workloadComplete {
		for id, load := range loads {
			load.WorkloadKnown = false
			load.WorkloadMessages = 0
			load.WorkloadLeased = 0
			load.PartitionWorkloads = nil
			loads[id] = load
		}
	}
	if !diskComplete {
		for id, load := range loads {
			load.DiskUsageKnown = false
			load.DiskUsedBytes = 0
			load.DiskAvailableBytes = 0
			load.DiskCapacityBytes = 0
			loads[id] = load
		}
	}
	if !partitionDiskComplete {
		for id, load := range loads {
			load.PartitionDiskUsage = nil
			loads[id] = load
		}
	}

	partitionByID := make(map[string]*cluster.PartitionMetadata, len(partitions))
	progressByID := make(map[string]cluster.PartitionProgress, len(partitions))
	progressComplete := true
	for _, partition := range partitions {
		if partition.Reassignment != nil {
			progressComplete = false
			break
		}
		partitionByID[partition.ID.Canonical().String()] = partition
	}
	if progressComplete {
		for reporterID, report := range reports {
			for _, progress := range report.PartitionProgress {
				key := progress.ID.Canonical().String()
				partition := partitionByID[key]
				if partition == nil || progress.Leader != partition.Leader || reporterID != partition.Leader || len(progress.Replicas) != len(partition.Replicas) {
					progressComplete = false
					break
				}
				if _, duplicate := progressByID[key]; duplicate {
					progressComplete = false
					break
				}
				assigned := make(map[string]struct{}, len(partition.Replicas))
				for _, replica := range partition.Replicas {
					assigned[replica] = struct{}{}
				}
				for _, replica := range progress.Replicas {
					if _, ok := assigned[replica.ReplicaID]; !ok || !replica.ProgressKnown {
						progressComplete = false
						break
					}
					delete(assigned, replica.ReplicaID)
				}
				if !progressComplete || len(assigned) != 0 {
					progressComplete = false
					break
				}
				progressByID[key] = progress
			}
			if !progressComplete {
				break
			}
		}
		if len(progressByID) != len(partitionByID) {
			progressComplete = false
		}
	}
	if progressComplete {
		for _, node := range nodes {
			load := loads[node.ID]
			load.PartitionProgressKnown = true
			load.PartitionRecentActive = true
			load.PartitionInSync = true
			for _, progress := range progressByID {
				for _, replica := range progress.Replicas {
					if replica.ReplicaID != node.ID {
						continue
					}
					if !replica.RecentActive {
						load.PartitionRecentActive = false
					}
					if !replica.InSync {
						load.PartitionInSync = false
					}
					if replica.Lag > load.PartitionLag {
						load.PartitionLag = replica.Lag
					}
				}
			}
			loads[node.ID] = load
		}
	}
	return loads, workloadComplete, progressComplete && len(partitionByID) > 0, diskComplete, partitionDiskComplete
}

func placementSignalFresh(observedAt, now time.Time) bool {
	if observedAt.IsZero() || observedAt.After(now.Add(2*time.Second)) {
		return false
	}
	return now.Sub(observedAt) <= maxPlacementSignalAge
}

func (db *DistributedBroker) requestPlacementSignals(ctx context.Context, node *cluster.NodeInfo) (PlacementNodeSignals, error) {
	response, err := SendHTTP(ctx, db.partitionForwardClient, fh.MethodGET, strings.TrimRight(node.ClientAddress, "/")+internalPlacementSignalsPath, nil, nil)
	if err != nil {
		return PlacementNodeSignals{}, err
	}
	defer response.Close()
	if response.StatusCode() != fh.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Reader(), 4096))
		return PlacementNodeSignals{}, fmt.Errorf("placement signal endpoint returned %s", response.Raw.Status)
	}
	var report PlacementNodeSignals
	decoder := json.NewDecoder(io.LimitReader(response.Reader(), 64<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&report); err != nil {
		return PlacementNodeSignals{}, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return PlacementNodeSignals{}, fmt.Errorf("placement signal response has trailing data")
	}
	if report.NodeID != node.ID {
		return PlacementNodeSignals{}, fmt.Errorf("placement signal node identity mismatch")
	}
	return report, nil
}

// samePlacementTopology rejects telemetry collected across a concurrent node
// or assignment change. ISR refreshes and observation timestamps are excluded;
// placement inputs, leaders, epochs, and reassignment phases must be unchanged.
func samePlacementTopology(before, after *cluster.ControllerTopologySnapshot) bool {
	if before == nil || after == nil || len(before.Nodes) != len(after.Nodes) || len(before.Partitions) != len(after.Partitions) {
		return false
	}
	nodes := make(map[string]*cluster.NodeInfo, len(before.Nodes))
	for _, node := range before.Nodes {
		if node != nil {
			nodes[node.ID] = node
		}
	}
	for _, node := range after.Nodes {
		if node == nil {
			return false
		}
		prior := nodes[node.ID]
		if prior == nil || node.State != prior.State || node.Address != prior.Address ||
			node.ClientAddress != prior.ClientAddress || !reflect.DeepEqual(node.Metadata, prior.Metadata) {
			return false
		}
		delete(nodes, node.ID)
	}
	if len(nodes) != 0 {
		return false
	}
	partitions := make(map[string]*cluster.PartitionMetadata, len(before.Partitions))
	for _, partition := range before.Partitions {
		if partition != nil {
			partitions[partition.ID.Canonical().String()] = partition
		}
	}
	for _, partition := range after.Partitions {
		if partition == nil {
			return false
		}
		key := partition.ID.Canonical().String()
		prior := partitions[key]
		if prior == nil || partition.Leader != prior.Leader || partition.LeaderEpoch != prior.LeaderEpoch ||
			partition.Routable != prior.Routable || !slices.Equal(partition.Replicas, prior.Replicas) ||
			!reflect.DeepEqual(partition.Reassignment, prior.Reassignment) {
			return false
		}
		delete(partitions, key)
	}
	return len(partitions) == 0
}
