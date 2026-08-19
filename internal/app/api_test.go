package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	envi "github.com/efureev/envi/v2"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/logging"
	"github.com/efureev/go-outbox/internal/store"
)

// fakeReader stands in for the database. The admin API is four calls deep, so
// the whole of it can be exercised without one.
type fakeReader struct {
	stats     store.Stats
	statsErr  error
	failed    []store.FailedMessage
	failedErr error
	requeued  []string
	requeErr  error

	// What the handler asked for, so the request's own parsing is observable.
	gotLimit  int
	gotStream string
	gotCursor store.Cursor
	gotIDs    []string
	gotBefore time.Time
}

func (f *fakeReader) Stats(context.Context) (store.Stats, error) {
	return f.stats, f.statsErr
}

func (f *fakeReader) ListFailed(
	_ context.Context, after store.Cursor, limit int, stream string,
) ([]store.FailedMessage, error) {
	f.gotCursor, f.gotLimit, f.gotStream = after, limit, stream

	return f.failed, f.failedErr
}

func (f *fakeReader) Requeue(_ context.Context, ids []string) ([]string, error) {
	f.gotIDs = ids

	return f.requeued, f.requeErr
}

func (f *fakeReader) RequeueFailedBefore(
	_ context.Context, before time.Time, limit int,
) ([]string, error) {
	f.gotBefore, f.gotLimit = before, limit

	return f.requeued, f.requeErr
}

func testHTTPModule(t *testing.T, reader outboxReader, adjust ...func(*config.Config)) *httpModule {
	t.Helper()

	cfg := config.Config{}
	cfg.App.Name = "outbox"
	cfg.Dispatch.BatchSize = 200
	cfg.Dispatch.Workers = 8
	cfg.Dispatch.PollInterval = 5 * time.Second
	cfg.Dispatch.LeaseTTL = 2 * time.Minute
	cfg.Dispatch.MaxAttempts = 5
	cfg.Brokers = config.BrokerConfig{
		Streams: map[string]config.StreamConfig{"local": {Driver: "rmq"}},
	}
	for _, fn := range adjust {
		fn(&cfg)
	}

	m := newHTTPModule(cfg, logging.Nop())
	m.store = reader
	m.started = time.Now().Add(-time.Minute)

	return m
}

func do(t *testing.T, h http.Handler, method, target string, body string, headers ...string) *httptest.ResponseRecorder {
	t.Helper()

	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, target, nil)
	} else {
		r = httptest.NewRequest(method, target, strings.NewReader(body))
	}
	for i := 0; i+1 < len(headers); i += 2 {
		r.Header.Set(headers[i], headers[i+1])
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	return w
}

func decode(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not JSON (%d): %s", w.Code, w.Body.String())
	}

	return body
}

// testEnv builds a configuration source without touching the process
// environment, the same way the config package's own tests do.
func testEnv(t *testing.T, kv ...string) *envi.Env {
	t.Helper()

	e := envi.New()
	for _, pair := range kv {
		k, v, ok := strings.Cut(pair, "=")
		if !ok {
			t.Fatalf("malformed pair %q", pair)
		}
		e.Set(k, v)
	}

	return e
}

func TestHealthIsAlwaysOK(t *testing.T) {
	m := testHTTPModule(t, &fakeReader{})

	w := do(t, m.routes(), http.MethodGet, "/health", "")

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if decode(t, w)["status"] != "ok" {
		t.Errorf("body = %s", w.Body)
	}
}

// Readiness is the one an orchestrator routes on, so an unhealthy module has to
// take the pod out of rotation rather than be logged and forgotten.
func TestReadyFollowsTheHealthProbe(t *testing.T) {
	t.Run("no probe wired", func(t *testing.T) {
		m := testHTTPModule(t, &fakeReader{})

		if w := do(t, m.routes(), http.MethodGet, "/ready", ""); w.Code != http.StatusOK {
			t.Errorf("status = %d, want 200", w.Code)
		}
	})

	t.Run("healthy", func(t *testing.T) {
		m := testHTTPModule(t, &fakeReader{})
		m.SetHealthProbe(func(context.Context) error { return nil })

		w := do(t, m.routes(), http.MethodGet, "/ready", "")
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", w.Code)
		}
		if _, ok := decode(t, w)["started_at"]; !ok {
			t.Error("a healthy answer does not say since when")
		}
	})

	t.Run("unhealthy", func(t *testing.T) {
		m := testHTTPModule(t, &fakeReader{})
		m.SetHealthProbe(func(context.Context) error { return errors.New("broker unreachable") })

		w := do(t, m.routes(), http.MethodGet, "/ready", "")
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503 so the orchestrator stops routing here", w.Code)
		}
		if body := decode(t, w); body["error"] != "broker unreachable" {
			t.Errorf("the reason is not reported: %v", body)
		}
	})
}

func TestStatsReportsCountsAndSettings(t *testing.T) {
	reader := &fakeReader{stats: store.Stats{
		Pending: 12, Processing: 3, Failed: 1, Deferred: 2,
		OldestPending: 90 * time.Second,
	}}
	m := testHTTPModule(t, reader, func(c *config.Config) {
		c.Dispatch.MaxDefer = 30 * time.Minute
	})

	w := do(t, m.routes(), http.MethodGet, "/api/v1/stats", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body)
	}

	body := decode(t, w)
	msgs, _ := body["messages"].(map[string]any)

	for key, want := range map[string]float64{
		"pending": 12, "processing": 3, "failed": 1, "deferred": 2,
		"oldest_pending_seconds": 90,
	} {
		if msgs[key] != want {
			t.Errorf("messages.%s = %v, want %v", key, msgs[key], want)
		}
	}

	settings, _ := body["settings"].(map[string]any)
	if settings["max_defer"] != "30m0s" {
		t.Errorf("settings.max_defer = %v", settings["max_defer"])
	}
}

// A zero MaxDefer means unbounded, and "0s" in the response would read as its
// opposite — that a message fails immediately.
func TestStatsRendersAnUnboundedDeferralAsSuch(t *testing.T) {
	m := testHTTPModule(t, &fakeReader{})

	body := decode(t, do(t, m.routes(), http.MethodGet, "/api/v1/stats", ""))
	settings, _ := body["settings"].(map[string]any)

	if settings["max_defer"] != "unbounded" {
		t.Errorf("settings.max_defer = %v, want \"unbounded\"", settings["max_defer"])
	}
}

func TestStatsReportsADatabaseFailure(t *testing.T) {
	m := testHTTPModule(t, &fakeReader{statsErr: errors.New("connection refused")})

	w := do(t, m.routes(), http.MethodGet, "/api/v1/stats", "")

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
	if msg, _ := decode(t, w)["error"].(string); !strings.Contains(msg, "connection refused") {
		t.Errorf("the cause is not reported: %v", msg)
	}
}

// Before the database module has started there is nothing to read, and 503 is
// the honest answer — the request may well succeed a second later.
func TestEndpointsSayWhenTheStoreIsNotReadyYet(t *testing.T) {
	m := testHTTPModule(t, nil)
	m.store = nil

	for _, target := range []string{"/api/v1/stats", "/api/v1/messages/failed"} {
		w := do(t, m.routes(), http.MethodGet, target, "")
		if w.Code != http.StatusServiceUnavailable {
			t.Errorf("%s: status = %d, want 503", target, w.Code)
		}
	}
}

func TestListFailedPagesAndFilters(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		reader := &fakeReader{}
		m := testHTTPModule(t, reader)

		do(t, m.routes(), http.MethodGet, "/api/v1/messages/failed", "")

		if reader.gotLimit != defaultPageSize {
			t.Errorf("limit = %d, want the default %d", reader.gotLimit, defaultPageSize)
		}
		if reader.gotStream != "" {
			t.Errorf("stream = %q, want every stream", reader.gotStream)
		}
	})

	t.Run("the stream filter reaches the query", func(t *testing.T) {
		reader := &fakeReader{}
		m := testHTTPModule(t, reader)

		do(t, m.routes(), http.MethodGet, "/api/v1/messages/failed?stream=local", "")

		if reader.gotStream != "local" {
			t.Errorf("stream = %q", reader.gotStream)
		}
	})

	// An unbounded limit is a way to ask one request to read the whole table.
	t.Run("limit is clamped, not trusted", func(t *testing.T) {
		for query, want := range map[string]int{
			"?limit=10":     10,
			"?limit=100000": maxPageSize,
			"?limit=0":      defaultPageSize,
			"?limit=-5":     defaultPageSize,
			"?limit=abc":    defaultPageSize,
		} {
			reader := &fakeReader{}
			m := testHTTPModule(t, reader)

			do(t, m.routes(), http.MethodGet, "/api/v1/messages/failed"+query, "")

			if reader.gotLimit != want {
				t.Errorf("%s: limit = %d, want %d", query, reader.gotLimit, want)
			}
		}
	})

	t.Run("an unreadable cursor is refused", func(t *testing.T) {
		m := testHTTPModule(t, &fakeReader{})

		w := do(t, m.routes(), http.MethodGet, "/api/v1/messages/failed?after=not-json", "")

		if w.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", w.Code)
		}
	})

	t.Run("a cursor is passed through", func(t *testing.T) {
		reader := &fakeReader{}
		m := testHTTPModule(t, reader)

		do(t, m.routes(), http.MethodGet,
			`/api/v1/messages/failed?after={"created_at":"2026-01-01T00:00:00Z","id":"abc"}`, "")

		if reader.gotCursor.ID != "abc" {
			t.Errorf("cursor = %+v", reader.gotCursor)
		}
	})
}

// The next cursor appears only on a full page. An empty one means the end, and a
// client can act on that without a second request.
func TestListFailedOffersACursorOnlyWhenThereMayBeMore(t *testing.T) {
	full := make([]store.FailedMessage, 2)
	for i := range full {
		full[i] = store.FailedMessage{ID: "id", CreatedAt: time.Now()}
	}

	t.Run("full page", func(t *testing.T) {
		m := testHTTPModule(t, &fakeReader{failed: full})

		body := decode(t, do(t, m.routes(), http.MethodGet, "/api/v1/messages/failed?limit=2", ""))
		if _, ok := body["next"]; !ok {
			t.Error("a full page offers no way to ask for the rest")
		}
	})

	t.Run("short page", func(t *testing.T) {
		m := testHTTPModule(t, &fakeReader{failed: full[:1]})

		body := decode(t, do(t, m.routes(), http.MethodGet, "/api/v1/messages/failed?limit=2", ""))
		if _, ok := body["next"]; ok {
			t.Error("a short page still offers a cursor, so a client would loop forever")
		}
	})
}

func TestListFailedReportsADatabaseFailure(t *testing.T) {
	m := testHTTPModule(t, &fakeReader{failedErr: errors.New("timeout")})

	if w := do(t, m.routes(), http.MethodGet, "/api/v1/messages/failed", ""); w.Code != 500 {
		t.Errorf("status = %d, want 500", w.Code)
	}
}

const testToken = "s3cret-token"

func withToken(c *config.Config) { c.HTTP.AdminToken = testToken }

// The invariant that matters most in this file: without a token the mutating
// endpoint does not exist at all. Not guarded by a default, not returning 401 —
// absent, so nothing can reach it.
func TestRequeueIsNotEvenRoutedWithoutAToken(t *testing.T) {
	m := testHTTPModule(t, &fakeReader{})

	w := do(t, m.routes(), http.MethodPost, "/api/v1/messages/requeue", `{"ids":["a"]}`)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404: an unguarded admin endpoint must not exist", w.Code)
	}
}

func TestRequeueRequiresTheRightToken(t *testing.T) {
	cases := map[string]struct {
		header string
		want   int
	}{
		"no header":        {"", http.StatusUnauthorized},
		"wrong token":      {"Bearer wrong", http.StatusUnauthorized},
		"empty bearer":     {"Bearer ", http.StatusUnauthorized},
		"prefix of it":     {"Bearer s3cret", http.StatusUnauthorized},
		"right token":      {"Bearer " + testToken, http.StatusOK},
		"bare, no  Bearer": {testToken, http.StatusOK},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			m := testHTTPModule(t, &fakeReader{requeued: []string{"a"}}, withToken)

			var headers []string
			if tc.header != "" {
				headers = []string{"Authorization", tc.header}
			}

			w := do(t, m.routes(), http.MethodPost, "/api/v1/messages/requeue",
				`{"ids":["a"]}`, headers...)

			if w.Code != tc.want {
				t.Errorf("status = %d, want %d", w.Code, tc.want)
			}
		})
	}
}

func TestRequeueRejectsIncoherentRequests(t *testing.T) {
	cases := map[string]string{
		"not JSON":             `{`,
		"neither ids nor time": `{}`,
		"both at once":         `{"ids":["a"],"failed_before":"2026-01-01T00:00:00Z"}`,
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			m := testHTTPModule(t, &fakeReader{}, withToken)

			w := do(t, m.routes(), http.MethodPost, "/api/v1/messages/requeue", body,
				"Authorization", "Bearer "+testToken)

			if w.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400: %s", w.Code, w.Body)
			}
		})
	}
}

// An unbounded id list is a way to ask one statement to touch the whole table.
func TestRequeueBoundsTheNumberOfIDs(t *testing.T) {
	ids := make([]string, maxRequeueIDs+1)
	for i := range ids {
		ids[i] = "id"
	}
	body, err := json.Marshal(map[string]any{"ids": ids})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	m := testHTTPModule(t, &fakeReader{}, withToken)

	w := do(t, m.routes(), http.MethodPost, "/api/v1/messages/requeue", string(body),
		"Authorization", "Bearer "+testToken)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestRequeueByIDsAndByTime(t *testing.T) {
	t.Run("by ids", func(t *testing.T) {
		reader := &fakeReader{requeued: []string{"a", "b"}}
		m := testHTTPModule(t, reader, withToken)

		w := do(t, m.routes(), http.MethodPost, "/api/v1/messages/requeue",
			`{"ids":["a","b"]}`, "Authorization", "Bearer "+testToken)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d: %s", w.Code, w.Body)
		}
		if len(reader.gotIDs) != 2 {
			t.Errorf("the handler passed %v", reader.gotIDs)
		}
		if decode(t, w)["requeued"] != float64(2) {
			t.Errorf("body = %s", w.Body)
		}
	})

	t.Run("by time", func(t *testing.T) {
		reader := &fakeReader{requeued: []string{"a"}}
		m := testHTTPModule(t, reader, withToken)

		w := do(t, m.routes(), http.MethodPost, "/api/v1/messages/requeue",
			`{"failed_before":"2026-01-01T00:00:00Z","limit":50}`,
			"Authorization", "Bearer "+testToken)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d: %s", w.Code, w.Body)
		}
		if reader.gotBefore.Year() != 2026 {
			t.Errorf("cutoff = %v", reader.gotBefore)
		}
		if reader.gotLimit != 50 {
			t.Errorf("limit = %d, want 50", reader.gotLimit)
		}
	})

	t.Run("a database failure", func(t *testing.T) {
		m := testHTTPModule(t, &fakeReader{requeErr: errors.New("deadlock")}, withToken)

		w := do(t, m.routes(), http.MethodPost, "/api/v1/messages/requeue",
			`{"ids":["a"]}`, "Authorization", "Bearer "+testToken)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("status = %d, want 500", w.Code)
		}
	})
}

func TestCutBearer(t *testing.T) {
	cases := map[string]struct {
		in     string
		out    string
		bearer bool
	}{
		"with the prefix":  {"Bearer abc", "abc", true},
		"without it":       {"abc", "abc", false},
		"the prefix alone": {"Bearer ", "Bearer ", false},
		"empty":            {"", "", false},
		"case matters":     {"bearer abc", "bearer abc", false},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, ok := cutBearer(tc.in)
			if got != tc.out || ok != tc.bearer {
				t.Errorf("cutBearer(%q) = %q, %v; want %q, %v", tc.in, got, ok, tc.out, tc.bearer)
			}
		})
	}
}

// The stats response is what an operator compares against another instance, so
// its shape has to be stable and its endpoints must not carry a password.
func TestStatsDescribesStreamsAndDriversWithoutLeaking(t *testing.T) {
	rmq, err := config.LoadFrom(testEnv(t,
		"OUTBOX_DB_USER=outbox", "OUTBOX_DB_NAME=app",
		"OUTBOX_STREAMS=local,global",
		"OUTBOX_STREAM_LOCAL_DRIVER=rmq",
		"OUTBOX_STREAM_GLOBAL_DRIVER=rmq",
		"OUTBOX_DRIVER_RMQ_TYPE=rabbitmq",
		"OUTBOX_DRIVER_RMQ_DSN=amqp://user:hunter2@rabbit:5672/",
		"OUTBOX_DRIVER_RMQ_PREFIX=app",
	))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	m := testHTTPModule(t, &fakeReader{}, func(c *config.Config) { c.Brokers = rmq.Brokers })

	w := do(t, m.routes(), http.MethodGet, "/api/v1/stats", "")
	if strings.Contains(w.Body.String(), "hunter2") {
		t.Fatalf("the stats response leaks a password: %s", w.Body)
	}

	body := decode(t, w)

	streams, _ := body["streams"].([]any)
	if len(streams) != 2 {
		t.Fatalf("streams = %v, want two", streams)
	}
	// Sorted, or two captures of the same deployment compare as different.
	first, _ := streams[0].(map[string]any)
	if first["name"] != "global" {
		t.Errorf("streams are not sorted: %v", streams)
	}

	drivers, _ := body["drivers"].([]any)
	if len(drivers) != 1 {
		t.Fatalf("drivers = %v, want one", drivers)
	}
	d, _ := drivers[0].(map[string]any)
	if d["type"] != "rabbitmq" || d["prefix"] != "app" {
		t.Errorf("driver = %v", d)
	}
	// Without the endpoint, two drivers aimed at different brokers render
	// identically and the response cannot confirm where a stream actually goes.
	if endpoint, _ := d["endpoint"].(string); !strings.Contains(endpoint, "rabbit:5672") {
		t.Errorf("endpoint = %q", endpoint)
	}
}

func TestSettingsReportTheNotifyMode(t *testing.T) {
	for enabled, want := range map[bool]string{
		true:  "listen/notify",
		false: "polling only",
	} {
		m := testHTTPModule(t, &fakeReader{}, func(c *config.Config) {
			c.Dispatch.NotifyEnabled = enabled
		})

		body := decode(t, do(t, m.routes(), http.MethodGet, "/api/v1/stats", ""))
		settings, _ := body["settings"].(map[string]any)

		if mode, _ := settings["notify_mode"].(string); !strings.Contains(mode, want) {
			t.Errorf("notify_mode = %q, want it to mention %q", mode, want)
		}
	}
}

func TestIntParam(t *testing.T) {
	cases := map[string]int{
		"":        7, // absent → default
		"?n=3":    3,
		"?n=0":    7, // below one → default
		"?n=-1":   7,
		"?n=oops": 7,
		"?n=99":   10, // above the maximum → clamped
	}

	for query, want := range cases {
		r := httptest.NewRequest(http.MethodGet, "/x"+query, nil)

		if got := intParam(r, "n", 7, 10); got != want {
			t.Errorf("intParam(%q) = %d, want %d", query, got, want)
		}
	}
}
