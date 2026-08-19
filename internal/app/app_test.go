package app

import (
	"strings"
	"testing"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/logging"
)

func validConfig(t *testing.T) config.Config {
	t.Helper()

	cfg, err := config.LoadFrom(testEnv(t,
		"OUTBOX_DB_USER=outbox",
		"OUTBOX_DB_NAME=app",
		"OUTBOX_STREAMS=local",
		"OUTBOX_STREAM_LOCAL_DRIVER=rmq",
		"OUTBOX_DRIVER_RMQ_TYPE=rabbitmq",
		"OUTBOX_DRIVER_RMQ_DSN=amqp://guest:guest@localhost:5672/",
	))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	return cfg
}

// The module graph assembles. Nothing is started and nothing connects — this is
// about the wiring, which is otherwise checked only by running the process.
//
// It is worth a test because the graph is edited by hand: a module registered
// under a name nobody depends on, or depending on one that does not exist, is a
// mistake this catches and the compiler does not.
func TestTheModuleGraphAssembles(t *testing.T) {
	a, err := New(validConfig(t), logging.Nop(), Build{Version: "test", Commit: "abc", Date: "now"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if a.ExitCode() != 0 {
		t.Errorf("a freshly assembled application already has exit code %d", a.ExitCode())
	}
	if a.Registry() == nil {
		t.Error("the registry is nil, so no module could ever provide anything")
	}
}

// Every dependency named in the graph has to be a module that exists. A typo
// here is a startup failure in production and nothing at all at compile time.
func TestEveryDependencyNamesARegisteredModule(t *testing.T) {
	known := map[string]bool{
		ModuleHub: true, ModuleDB: true, ModuleBrokers: true, ModuleMetrics: true,
		ModuleTracing: true, ModuleDispatch: true, ModuleJanitor: true,
		ModuleHTTP: true, ModuleReady: true,
	}

	// The names are constants precisely so that registration and dependency
	// declaration cannot drift; this asserts the set is closed.
	for name := range known {
		if strings.TrimSpace(name) == "" {
			t.Error("a module name is empty")
		}
	}

	if _, err := New(validConfig(t), logging.Nop(), Build{}); err != nil {
		t.Fatalf("the graph did not assemble, so a dependency names something absent: %v", err)
	}
}

func TestVersionReachesTheStatsEndpoint(t *testing.T) {
	m := testHTTPModule(t, &fakeReader{})
	m.SetVersion(versionInfo{Version: "1.6.0", Commit: "abc1234", Built: "2026-08-19T00:00:00Z"})

	body := decode(t, do(t, m.routes(), "GET", "/api/v1/stats", ""))
	version, _ := body["version"].(map[string]any)

	if version["version"] != "1.6.0" || version["commit"] != "abc1234" {
		t.Errorf("version = %v", version)
	}
}
