package broker

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// DLQDashboardResponse contains an aggregated overview of all dead-letter queues.
type DLQDashboardResponse struct {
	TotalDepth int               `json:"total_depth"`
	QueueCount int               `json:"queue_count"`
	Queues     []DLQQueueSummary `json:"queues"`
}

// DLQQueueSummary contains depth, error distribution, and recency for a specific DLQ.
type DLQQueueSummary struct {
	Name            string          `json:"name"`
	Depth           int             `json:"depth"`
	TopErrors       []DLQErrorCount `json:"top_errors"`
	LastError       string          `json:"last_error,omitempty"`
	LastErrorAt     *time.Time      `json:"last_error_at,omitempty"`
	OldestMessageAt *time.Time      `json:"oldest_message_at,omitempty"`
}

// DLQErrorCount aggregates error frequencies.
type DLQErrorCount struct {
	Error string `json:"error"`
	Count int    `json:"count"`
}

// DLQErrorsResponse returns paginated error report for a queue.
type DLQErrorsResponse struct {
	Queue         string         `json:"queue"`
	TotalErrors   int            `json:"total_errors"`
	TotalMessages int            `json:"total_messages"`
	Page          int            `json:"page"`
	Limit         int            `json:"limit"`
	TotalPages    int            `json:"total_pages"`
	Errors        []DLQErrorInfo `json:"errors"`
}

// DLQErrorInfo details a single error pattern.
type DLQErrorInfo struct {
	Error            string     `json:"error"`
	Count            int        `json:"count"`
	FirstSeenAt      *time.Time `json:"first_seen_at,omitempty"`
	LastSeenAt       *time.Time `json:"last_seen_at,omitempty"`
	SampleMessageIDs []string   `json:"sample_message_ids"`
}

// DLQReplayAllRequest configures bulk replay of DLQ messages.
type DLQReplayAllRequest struct {
	BatchSize   int      `json:"batch_size"`
	Destination string   `json:"destination"`
	DryRun      bool     `json:"dry_run"`
	Mode        MoveMode `json:"mode,omitempty"`
}

// DLQReplayAllResponse summarizes bulk replay results.
type DLQReplayAllResponse struct {
	Source      string   `json:"source"`
	Destination string   `json:"destination"`
	Replayed    int      `json:"replayed"`
	Batches     int      `json:"batches"`
	DryRun      bool     `json:"dry_run"`
	MessageIDs  []string `json:"message_ids,omitempty"`
}

// DLQArchiveRequest configures archiving DLQ messages.
type DLQArchiveRequest struct {
	Destination string `json:"destination"`
	Limit       int    `json:"limit"`
	DryRun      bool   `json:"dry_run"`
}

// DLQArchiveResponse summarizes archiving results.
type DLQArchiveResponse struct {
	Source      string   `json:"source"`
	Destination string   `json:"destination"`
	Archived    int      `json:"archived"`
	DryRun      bool     `json:"dry_run"`
	MessageIDs  []string `json:"message_ids,omitempty"`
}

func extractMessageError(m Message) string {
	if m.LastError != "" {
		return m.LastError
	}
	if m.DeadLetterReason != "" {
		return m.DeadLetterReason
	}
	if m.Metadata != nil && m.Metadata["error"] != "" {
		return m.Metadata["error"]
	}
	if m.Headers != nil && m.Headers["error"] != "" {
		return m.Headers["error"]
	}
	return "unspecified_error"
}

// dlqDashboard handles GET /dlq/dashboard
func (a AdminServer) dlqDashboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodBroker(w)
		return
	}

	ctx := r.Context()
	st, err := a.Broker.store.Stats(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	dlqMap := st.DLQ
	if dlqMap == nil {
		dlqMap = make(map[string]int)
	}

	resp := DLQDashboardResponse{
		Queues: make([]DLQQueueSummary, 0, len(dlqMap)),
	}

	for qName, depth := range dlqMap {
		if !tenantAllowed(ctx, qName) {
			continue
		}

		summary := DLQQueueSummary{
			Name:      qName,
			Depth:     depth,
			TopErrors: make([]DLQErrorCount, 0),
		}

		resp.TotalDepth += depth

		// Sample up to 200 messages for quick dashboard insights
		sampleLimit := 200
		if depth < sampleLimit {
			sampleLimit = depth
		}
		if sampleLimit <= 0 {
			sampleLimit = 50
		}

		msgs, err := a.Broker.store.ListDLQ(ctx, qName, sampleLimit)
		if err == nil && len(msgs) > 0 {
			errFreq := make(map[string]int)
			var latestErrAt *time.Time
			var oldestMsgAt *time.Time
			var latestErrMsg string

			for _, m := range msgs {
				errMsg := extractMessageError(m)
				errFreq[errMsg]++

				// Track timestamps
				msgTime := m.CreatedAt
				if msgTime.IsZero() {
					msgTime = time.Now()
				}
				if oldestMsgAt == nil || msgTime.Before(*oldestMsgAt) {
					t := msgTime
					oldestMsgAt = &t
				}
				if latestErrAt == nil || msgTime.After(*latestErrAt) {
					t := msgTime
					latestErrAt = &t
					latestErrMsg = errMsg
				}
			}

			// Sort errors by frequency
			type kv struct {
				k string
				v int
			}
			var sorted []kv
			for k, v := range errFreq {
				sorted = append(sorted, kv{k, v})
			}
			sort.Slice(sorted, func(i, j int) bool {
				return sorted[i].v > sorted[j].v
			})

			for i := 0; i < len(sorted) && i < 5; i++ {
				summary.TopErrors = append(summary.TopErrors, DLQErrorCount{
					Error: sorted[i].k,
					Count: sorted[i].v,
				})
			}

			summary.LastError = latestErrMsg
			summary.LastErrorAt = latestErrAt
			summary.OldestMessageAt = oldestMsgAt
		}

		resp.Queues = append(resp.Queues, summary)
	}

	sort.Slice(resp.Queues, func(i, j int) bool {
		return resp.Queues[i].Name < resp.Queues[j].Name
	})
	resp.QueueCount = len(resp.Queues)

	writeBrokerJSON(w, resp)
}

// dlqErrors handles GET /dlq/:queue/errors
func (a AdminServer) dlqErrors(w http.ResponseWriter, r *http.Request, queue string) {
	if r.Method != http.MethodGet {
		methodBroker(w)
		return
	}

	ctx := r.Context()
	sampleLimit := 1000
	if s := r.URL.Query().Get("sample"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			sampleLimit = v
		}
	}

	page := 1
	if p := r.URL.Query().Get("page"); p != "" {
		if v, err := strconv.Atoi(p); err == nil && v > 0 {
			page = v
		}
	}

	limit := 20
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			limit = v
			if limit > 100 {
				limit = 100
			}
		}
	}

	msgs, err := a.Broker.store.ListDLQ(ctx, queue, sampleLimit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	type errStats struct {
		count       int
		firstSeen   *time.Time
		lastSeen    *time.Time
		sampleMsgIDs []string
	}
	grouped := make(map[string]*errStats)

	for _, m := range msgs {
		errMsg := extractMessageError(m)
		st, ok := grouped[errMsg]
		if !ok {
			st = &errStats{}
			grouped[errMsg] = st
		}
		st.count++
		if len(st.sampleMsgIDs) < 5 {
			st.sampleMsgIDs = append(st.sampleMsgIDs, m.ID)
		}
		msgTime := m.CreatedAt
		if !msgTime.IsZero() {
			if st.firstSeen == nil || msgTime.Before(*st.firstSeen) {
				t := msgTime
				st.firstSeen = &t
			}
			if st.lastSeen == nil || msgTime.After(*st.lastSeen) {
				t := msgTime
				st.lastSeen = &t
			}
		}
	}

	type pair struct {
		errStr string
		stats  *errStats
	}
	var pairs []pair
	for k, v := range grouped {
		pairs = append(pairs, pair{k, v})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].stats.count > pairs[j].stats.count
	})

	totalErrors := len(pairs)
	totalPages := (totalErrors + limit - 1) / limit
	if totalPages == 0 {
		totalPages = 1
	}

	start := (page - 1) * limit
	end := start + limit
	if start > totalErrors {
		start = totalErrors
	}
	if end > totalErrors {
		end = totalErrors
	}

	resp := DLQErrorsResponse{
		Queue:         queue,
		TotalErrors:   totalErrors,
		TotalMessages: len(msgs),
		Page:          page,
		Limit:         limit,
		TotalPages:    totalPages,
		Errors:        make([]DLQErrorInfo, 0, end-start),
	}

	for i := start; i < end; i++ {
		p := pairs[i]
		resp.Errors = append(resp.Errors, DLQErrorInfo{
			Error:            p.errStr,
			Count:            p.stats.count,
			FirstSeenAt:      p.stats.firstSeen,
			LastSeenAt:       p.stats.lastSeen,
			SampleMessageIDs: p.stats.sampleMsgIDs,
		})
	}

	writeBrokerJSON(w, resp)
}

// dlqReplayAll handles POST /dlq/:queue/replay-all
func (a AdminServer) dlqReplayAll(w http.ResponseWriter, r *http.Request, queue string) {
	if r.Method != http.MethodPost {
		methodBroker(w)
		return
	}

	ctx := r.Context()
	var req DLQReplayAllRequest
	if r.Header.Get("Content-Type") == "application/json" {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	if req.Destination == "" {
		req.Destination = r.URL.Query().Get("dest")
	}
	if req.Destination == "" {
		req.Destination = strings.TrimSuffix(queue, ".dlq")
	}
	if req.BatchSize <= 0 {
		if bs := r.URL.Query().Get("batch_size"); bs != "" {
			if v, err := strconv.Atoi(bs); err == nil && v > 0 {
				req.BatchSize = v
			}
		}
	}
	if req.BatchSize <= 0 {
		req.BatchSize = 100
	}
	if req.BatchSize > 1000 {
		req.BatchSize = 1000
	}
	if !req.DryRun && r.URL.Query().Get("dry_run") == "true" {
		req.DryRun = true
	}

	_ = a.Broker.store.DeclareQueue(ctx, QueueConfig{Name: req.Destination})

	mode := MoveRemoveSource
	if req.Mode != "" {
		mode = req.Mode
	} else if m := r.URL.Query().Get("mode"); m != "" {
		mode = MoveMode(m)
	}

	totalReplayed := 0
	batches := 0
	var allMovedIDs []string

	for {
		msgs, err := a.Broker.store.ListDLQ(ctx, queue, req.BatchSize)
		if err != nil || len(msgs) == 0 {
			break
		}

		ids := make([]string, len(msgs))
		for i, m := range msgs {
			ids[i] = m.ID
		}

		res, err := a.Broker.store.Move(ctx, MoveRequest{
			SourceType:      DestinationQueue,
			Source:          queue,
			DestinationType: DestinationQueue,
			Destination:     req.Destination,
			MessageIDs:      ids,
			Mode:            mode,
			ResetAttempts:   true,
			ResetError:      true,
			ResetLease:      true,
			PreserveID:      true,
			Reason:          "dlq replay-all",
			Actor:           "admin",
			DryRun:          req.DryRun,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}

		batches++
		batchCount := res.Moved + res.Copied
		totalReplayed += batchCount
		allMovedIDs = append(allMovedIDs, res.MessageIDs...)

		if req.DryRun || batchCount == 0 || mode == MoveReplay || mode == MoveCopy {
			break
		}
	}

	writeBrokerJSON(w, DLQReplayAllResponse{
		Source:      queue,
		Destination: req.Destination,
		Replayed:    totalReplayed,
		Batches:     batches,
		DryRun:      req.DryRun,
		MessageIDs:  allMovedIDs,
	})
}

// dlqArchive handles POST /dlq/:queue/archive
func (a AdminServer) dlqArchive(w http.ResponseWriter, r *http.Request, queue string) {
	if r.Method != http.MethodPost {
		methodBroker(w)
		return
	}

	ctx := r.Context()
	var req DLQArchiveRequest
	if r.Header.Get("Content-Type") == "application/json" {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	if req.Destination == "" {
		req.Destination = r.URL.Query().Get("dest")
	}
	if req.Destination == "" {
		req.Destination = queue + ".archive"
	}
	if req.Limit <= 0 {
		if l := r.URL.Query().Get("limit"); l != "" {
			if v, err := strconv.Atoi(l); err == nil && v > 0 {
				req.Limit = v
			}
		}
	}
	if !req.DryRun && r.URL.Query().Get("dry_run") == "true" {
		req.DryRun = true
	}

	_ = a.Broker.store.DeclareQueue(ctx, QueueConfig{Name: req.Destination})

	var ids []string
	if req.Limit > 0 {
		msgs, err := a.Broker.store.ListDLQ(ctx, queue, req.Limit)
		if err == nil {
			for _, m := range msgs {
				ids = append(ids, m.ID)
			}
		}
	}

	res, err := a.Broker.store.Move(ctx, MoveRequest{
		SourceType:      DestinationQueue,
		Source:          queue,
		DestinationType: DestinationQueue,
		Destination:     req.Destination,
		MessageIDs:      ids,
		Mode:            MoveArchive,
		PreserveID:      true,
		Reason:          "dlq archive",
		Actor:           "admin",
		DryRun:          req.DryRun,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	writeBrokerJSON(w, DLQArchiveResponse{
		Source:      queue,
		Destination: req.Destination,
		Archived:    res.Moved,
		DryRun:      req.DryRun,
		MessageIDs:  res.MessageIDs,
	})
}

// dlqExport handles GET /dlq/:queue/export streaming NDJSON
func (a AdminServer) dlqExport(w http.ResponseWriter, r *http.Request, queue string) {
	if r.Method != http.MethodGet {
		methodBroker(w)
		return
	}

	ctx := r.Context()
	limit := 1000
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			limit = v
		}
	}

	msgs, err := a.Broker.store.ListDLQ(ctx, queue, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	flusher, _ := w.(http.Flusher)
	enc := json.NewEncoder(w)

	for _, m := range msgs {
		if err := enc.Encode(m); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
}
