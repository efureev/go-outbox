package app

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/store"
)

const (
	defaultPageSize = 50
	maxPageSize     = 500
	maxRequeueIDs   = 10_000
)

// statsResponse is the observability payload.
type statsResponse struct {
	Version   versionInfo      `json:"version"`
	StartedAt time.Time        `json:"started_at"`
	Uptime    string           `json:"uptime"`
	Messages  messageCounts    `json:"messages"`
	Streams   []streamInfo     `json:"streams"`
	Drivers   []driverInfo     `json:"drivers"`
	Settings  dispatchSettings `json:"settings"`
}

type versionInfo struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Built   string `json:"built"`
}

// messageCounts deliberately omits delivered rows: counting them means scanning
// them, and the count is already available as outbox_messages_dispatched_total.
type messageCounts struct {
	Pending    int64 `json:"pending"`
	Processing int64 `json:"processing"`
	Failed     int64 `json:"failed"`
	// Deferred counts rows waiting on a broker that could not be reached. It
	// overlaps pending and processing rather than adding to them, and it is the
	// field that says whether a backlog is moving slowly or not at all.
	Deferred          int64   `json:"deferred"`
	OldestPendingSecs float64 `json:"oldest_pending_seconds"`
}

type streamInfo struct {
	Name   string `json:"name"`
	Driver string `json:"driver"`
}

type driverInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
	// Endpoint is where this driver connects, with credentials removed. Without
	// it two drivers pointed at different brokers render identically, so the
	// response could not be used to confirm that a stream reaches the instance
	// it was meant to.
	Endpoint string `json:"endpoint"`
	// Prefix and the separators are what a consumer needs to work out the
	// effective topic name it must subscribe to.
	Prefix     string `json:"prefix,omitempty"`
	PrefixSep  string `json:"prefix_separator"`
	VersionSep string `json:"version_separator"`
}

type dispatchSettings struct {
	BatchSize    int    `json:"batch_size"`
	Workers      int    `json:"workers"`
	PollInterval string `json:"poll_interval"`
	LeaseTTL     string `json:"lease_ttl"`
	MaxAttempts  int    `json:"max_attempts"`
	// MaxDefer is how long an unreachable broker may hold a message back before
	// it fails anyway. Reported as "unbounded" when it is off, because a "0s"
	// here reads as "fails immediately", which is its opposite.
	MaxDefer   string `json:"max_defer"`
	NotifyMode string `json:"notify_mode"`
}

func (m *httpModule) handleStats(w http.ResponseWriter, r *http.Request) {
	if m.store == nil {
		writeError(w, http.StatusServiceUnavailable, "the store is not available yet")

		return
	}

	stats, err := m.store.Stats(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not read message counts: "+err.Error())

		return
	}

	writeJSON(w, http.StatusOK, statsResponse{
		Version:   m.version,
		StartedAt: m.started.UTC(),
		Uptime:    time.Since(m.started).Truncate(time.Second).String(),
		Messages: messageCounts{
			Pending:           stats.Pending,
			Processing:        stats.Processing,
			Failed:            stats.Failed,
			Deferred:          stats.Deferred,
			OldestPendingSecs: stats.OldestPending.Seconds(),
		},
		Streams:  streamsOf(m.cfg.Brokers),
		Drivers:  driversOf(m.cfg.Brokers),
		Settings: settingsOf(m.cfg),
	})
}

// handleListFailed pages through failed messages, so the answer to "what
// stopped, and why" does not require database access.
func (m *httpModule) handleListFailed(w http.ResponseWriter, r *http.Request) {
	if m.store == nil {
		writeError(w, http.StatusServiceUnavailable, "the store is not available yet")

		return
	}

	limit := intParam(r, "limit", defaultPageSize, maxPageSize)

	var cursor store.Cursor
	if raw := r.URL.Query().Get("after"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &cursor); err != nil {
			writeError(w, http.StatusBadRequest, "the after cursor is not valid JSON")

			return
		}
	}

	// An unconfigured stream is not an error: it simply matches nothing, which
	// is the same answer as a configured stream with no failures.
	messages, err := m.store.ListFailed(r.Context(), cursor, limit, r.URL.Query().Get("stream"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not list failed messages: "+err.Error())

		return
	}

	body := map[string]any{"messages": messages}

	// A next cursor only when the page was full; an empty one means the end,
	// which a client can act on without a second request.
	if len(messages) == limit {
		last := messages[len(messages)-1]
		body["next"] = store.Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}

	writeJSON(w, http.StatusOK, body)
}

type requeueRequest struct {
	IDs          []string   `json:"ids,omitempty"`
	FailedBefore *time.Time `json:"failed_before,omitempty"`
	Limit        int        `json:"limit,omitempty"`
}

// handleRequeue returns failed messages to the queue.
//
// It has to reset the attempt counter and the availability time along with the
// status; a version that changes only the status leaves a row that is nominally
// pending and will never be selected again. Exposing it as an endpoint over a
// database function leaves one correct implementation instead of a snippet
// everyone copies and half of them get wrong.
func (m *httpModule) handleRequeue(w http.ResponseWriter, r *http.Request) {
	if m.store == nil {
		writeError(w, http.StatusServiceUnavailable, "the store is not available yet")

		return
	}

	var req requeueRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "could not read the request body: "+err.Error())

		return
	}

	switch {
	case len(req.IDs) > 0 && req.FailedBefore != nil:
		writeError(w, http.StatusBadRequest, "give either ids or failed_before, not both")

		return
	case len(req.IDs) > maxRequeueIDs:
		writeError(w, http.StatusBadRequest,
			"at most "+strconv.Itoa(maxRequeueIDs)+" ids per request")

		return
	case len(req.IDs) == 0 && req.FailedBefore == nil:
		writeError(w, http.StatusBadRequest, "give either ids or failed_before")

		return
	}

	var (
		requeued []string
		err      error
	)

	if len(req.IDs) > 0 {
		requeued, err = m.store.Requeue(r.Context(), req.IDs)
	} else {
		limit := req.Limit
		if limit <= 0 || limit > maxRequeueIDs {
			limit = 1000
		}
		requeued, err = m.store.RequeueFailedBefore(r.Context(), *req.FailedBefore, limit)
	}

	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not requeue: "+err.Error())

		return
	}

	m.log.Info("messages returned to the queue", "count", len(requeued))

	writeJSON(w, http.StatusOK, map[string]any{
		"requeued": len(requeued),
		"ids":      requeued,
	})
}

// requireToken guards the mutating endpoints.
//
// A constant-time comparison, because the alternative leaks the token one byte
// at a time to anyone who can measure the response.
func requireToken(token string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		given := r.Header.Get("Authorization")
		given, _ = cutBearer(given)

		if subtle.ConstantTimeCompare([]byte(given), []byte(token)) != 1 {
			writeError(w, http.StatusUnauthorized, "a valid admin token is required")

			return
		}

		next(w, r)
	}
}

func cutBearer(header string) (string, bool) {
	const prefix = "Bearer "
	if len(header) > len(prefix) && header[:len(prefix)] == prefix {
		return header[len(prefix):], true
	}

	return header, false
}

func intParam(r *http.Request, name string, def, maximum int) int {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return def
	}

	v, err := strconv.Atoi(raw)
	if err != nil || v < 1 {
		return def
	}
	if v > maximum {
		return maximum
	}

	return v
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func streamsOf(cfg config.BrokerConfig) []streamInfo {
	out := make([]streamInfo, 0, len(cfg.Streams))
	for _, name := range cfg.StreamNames() {
		out = append(out, streamInfo{Name: name, Driver: cfg.Streams[name].Driver})
	}

	return out
}

func driversOf(cfg config.BrokerConfig) []driverInfo {
	// Sorted, like the streams above: iterating the map directly would reorder
	// the array on every request, and comparing two captures is exactly what
	// this endpoint is for.
	names := make([]string, 0, len(cfg.Drivers))
	for name := range cfg.Drivers {
		names = append(names, name)
	}
	slices.Sort(names)

	out := make([]driverInfo, 0, len(names))
	for _, name := range names {
		d := cfg.Drivers[name]
		naming := d.Naming()
		out = append(out, driverInfo{
			Name:       name,
			Type:       string(d.Type()),
			Endpoint:   d.Endpoint(),
			Prefix:     naming.Prefix,
			PrefixSep:  naming.PrefixSep,
			VersionSep: naming.VersionSep,
		})
	}

	return out
}

func settingsOf(cfg config.Config) dispatchSettings {
	notify := "polling only"
	if cfg.Dispatch.NotifyEnabled {
		notify = "listen/notify with polling as reconciliation"
	}

	maxDefer := "unbounded"
	if cfg.Dispatch.MaxDefer > 0 {
		maxDefer = cfg.Dispatch.MaxDefer.String()
	}

	return dispatchSettings{
		BatchSize:    cfg.Dispatch.BatchSize,
		Workers:      cfg.Dispatch.Workers,
		PollInterval: cfg.Dispatch.PollInterval.String(),
		LeaseTTL:     cfg.Dispatch.LeaseTTL.String(),
		MaxAttempts:  cfg.Dispatch.MaxAttempts,
		MaxDefer:     maxDefer,
		NotifyMode:   notify,
	}
}
