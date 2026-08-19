package app

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/efureev/appmod/v4"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/logging"
	"github.com/efureev/go-outbox/internal/store"
)

// httpModule serves the operational endpoints.
//
// It is net/http and nothing else: the endpoints below are a handful of routes,
// which ServeMux has been able to express since patterns gained methods.
type httpModule struct {
	*appmod.BaseAppModule

	cfg     config.Config
	log     *slog.Logger
	health  func(context.Context) error
	started time.Time
	version versionInfo
	store   outboxReader
}

// outboxReader is the database side of the admin API: the four calls the
// handlers make and nothing else.
//
// It is an interface for the same reason dispatch.Store is one — the handlers
// depend on four behaviours, not on a connection pool, and a narrow seam is what
// lets them be tested against httptest with no database at all.
type outboxReader interface {
	Stats(ctx context.Context) (store.Stats, error)
	ListFailed(ctx context.Context, after store.Cursor, limit int, stream string) ([]store.FailedMessage, error)
	Requeue(ctx context.Context, ids []string) ([]string, error)
	RequeueFailedBefore(ctx context.Context, before time.Time, limit int) ([]string, error)
}

var _ outboxReader = (*store.Store)(nil)

func newHTTPModule(cfg config.Config, log *slog.Logger) *httpModule {
	m := &httpModule{
		BaseAppModule: appmod.New(appmod.WithConfig(appmod.NewConfig(ModuleHTTP, "v1"))),
		cfg:           cfg,
		log:           log.With(slog.String(logging.ModuleKey, ModuleHTTP)),
	}

	m.AfterStart(m.listen)

	return m
}

// SetVersion records the build information reported by /api/v1/stats.
func (m *httpModule) SetVersion(v versionInfo) { m.version = v }

// SetHealthProbe injects the readiness probe. It is the Manager's own
// health check, which the module cannot reach from inside the graph.
func (m *httpModule) SetHealthProbe(fn func(context.Context) error) { m.health = fn }

func (m *httpModule) listen(ctx context.Context, _ appmod.HookModule) error {
	if !m.cfg.HTTP.Enabled {
		return nil
	}

	st, err := appmod.Require[*store.Store](m.AppContext().Registry)
	if err != nil {
		return err
	}
	m.store = st

	m.started = time.Now()

	stop, serveErr := serve(ctx, serveOptions{
		name:            "api",
		port:            m.cfg.HTTP.Port,
		handler:         m.routes(),
		readTimeout:     m.cfg.HTTP.ReadTimeout,
		writeTimeout:    m.cfg.HTTP.WriteTimeout,
		shutdownTimeout: m.cfg.HTTP.ShutdownTimeout,
		log:             m.log,
	})
	if serveErr != nil {
		return serveErr
	}

	m.AddCleanup(stop)

	return nil
}

func (m *httpModule) routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", m.handleHealth)
	mux.HandleFunc("GET /ready", m.handleReady)
	mux.HandleFunc("GET /api/v1/stats", m.handleStats)
	mux.HandleFunc("GET /api/v1/messages/failed", m.handleListFailed)

	// The mutating endpoint exists only when there is a token to guard it. An
	// admin API reachable by anything that can route to the pod is worse than no
	// admin API — and so is one behind a default token, which is the same thing
	// with extra steps.
	if m.cfg.HTTP.AdminToken != "" {
		mux.HandleFunc("POST /api/v1/messages/requeue",
			requireToken(m.cfg.HTTP.AdminToken, m.handleRequeue))
	} else {
		m.log.Info("the requeue endpoint is disabled: set OUTBOX_HTTP_ADMIN_TOKEN to enable it")
	}

	return mux
}

// handleHealth is liveness: the process is running and able to serve. It
// deliberately does not probe dependencies — a database blip should not make
// an orchestrator kill a process that would recover on its own.
func (m *httpModule) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

// handleReady is readiness: every module that reports its health is healthy.
func (m *httpModule) handleReady(w http.ResponseWriter, r *http.Request) {
	if m.health == nil {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})

		return
	}

	if err := m.health(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "unavailable",
			"error":  err.Error(),
		})

		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":     "ok",
		"started_at": m.started.UTC(),
	})
}
