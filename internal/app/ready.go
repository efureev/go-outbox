package app

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/efureev/appmod/v4"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/logging"
)

// readyModule announces, in one line, the configuration the process actually
// came up with.
//
// The information exists elsewhere — GET /api/v1/stats reports it live — but
// only the log answers it for a process that has since been replaced. Working
// out what a pod was running with an hour ago otherwise means reading six
// startup lines from six modules and hoping none scrolled away.
//
// It is a module rather than a line in App.Run because Manager.Run blocks from
// the moment it starts the graph: there is no point in it at which everything
// is up and nothing is yet waiting. A module that depends on all the others
// lands alone in the last topological layer, so its AfterStart is the first
// thing to run once the whole graph is up.
type readyModule struct {
	*appmod.BaseAppModule

	cfg     config.Config
	log     *slog.Logger
	version versionInfo
}

func newReadyModule(cfg config.Config, log *slog.Logger, version versionInfo) *readyModule {
	m := &readyModule{
		BaseAppModule: appmod.New(appmod.WithConfig(appmod.NewConfig(ModuleReady, "v1"))),
		cfg:           cfg,
		log:           log.With(slog.String(logging.ModuleKey, cfg.App.Name)),
		version:       version,
	}

	m.AfterStart(m.announce)

	return m
}

func (m *readyModule) announce(context.Context, appmod.HookModule) error {
	d := m.cfg.Dispatch

	m.log.Info("ready",
		slog.String("version", m.version.Version),
		slog.String("streams", streamSummary(m.cfg.Brokers)),
		slog.Int("batch_size", d.BatchSize),
		slog.Int("workers", d.Workers),
		slog.Duration("poll_interval", d.PollInterval),
		slog.Duration("lease_ttl", d.LeaseTTL),
		slog.Int("max_attempts", d.MaxAttempts),
		slog.String("max_defer", deferWindow(d.MaxDefer)),
		slog.Bool("notify", d.NotifyEnabled),
		slog.Duration("retention", m.cfg.Janitor.Retention),
	)

	return nil
}

// streamSummary renders the routing table as "stream:driver" pairs, so one
// field answers where each stream goes rather than two fields listing names
// that have to be paired up by hand.
func streamSummary(brokers config.BrokerConfig) string {
	pairs := make([]string, 0, len(brokers.Streams))
	for _, name := range brokers.StreamNames() {
		pairs = append(pairs, name+":"+brokers.Streams[name].Driver)
	}
	sort.Strings(pairs)

	return strings.Join(pairs, ",")
}

// deferWindow renders the deferral bound the way an operator reads it. A bare
// "0s" says "fails at once", which is the opposite of what zero means here.
func deferWindow(d time.Duration) string {
	if d <= 0 {
		return "unbounded"
	}

	return d.String()
}
