package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	envi "github.com/efureev/envi/v2"
	"github.com/efureev/envi/v2/bind"
)

// EnvPrefix namespaces every variable this program reads.
const EnvPrefix = "OUTBOX"

// Load reads the configuration from the given .env files (each optional, later
// ones winning) with the process environment layered on top, then validates it.
//
// Files and environment are merged once and the same merged view feeds both the
// struct binding and the driver lookup, so a stream configured in a .env file
// behaves exactly like one configured in the environment.
func Load(files ...string) (Config, error) {
	env, err := source(files...)
	if err != nil {
		return Config{}, err
	}

	return decode(env, false)
}

// LoadFrom builds the configuration from an explicit source. Tests use it to
// avoid touching the process environment.
func LoadFrom(src *envi.Env) (Config, error) { return decode(src, false) }

// LoadAdmin reads the configuration for a one-shot administrative command,
// checking only what such a command uses.
//
// Listing what stopped and putting it back need a database and nothing else. A
// dispatcher refuses to start on a broken routing table, and rightly — it cannot
// deliver. But the moment an operator most needs to see what failed is often the
// moment the configuration is what is wrong, and a tool that answers "your
// broker is misconfigured" to the question "what failed?" is useless precisely
// then.
//
// The routing table is still assembled on a best-effort basis, so a command that
// can show it does, and one reached with a broken table sees an empty one rather
// than an error.
func LoadAdmin(files ...string) (Config, error) {
	env, err := source(files...)
	if err != nil {
		return Config{}, err
	}

	return decode(env, true)
}

// LoadAdminFrom is LoadAdmin against an explicit source, for the same reason
// LoadFrom exists.
func LoadAdminFrom(src *envi.Env) (Config, error) { return decode(src, true) }

func decode(env *envi.Env, adminOnly bool) (Config, error) {
	var cfg Config

	if err := bind.Decode(env, &cfg, bind.WithPrefix(EnvPrefix)); err != nil {
		return Config{}, fmt.Errorf("read configuration: %w", err)
	}

	// A broken routing table does not short-circuit the rest: an operator
	// bringing up a fresh deployment should see every mistake in one run, not
	// discover the next one on each restart.
	brokers, brokerErr := loadBrokers(env, cfg.DB)
	cfg.Brokers = brokers

	if cfg.App.Instance == "" {
		cfg.App.Instance = defaultInstance()
	}

	if adminOnly {
		if err := cfg.validateAdmin(); err != nil {
			return Config{}, err
		}

		return cfg, nil
	}

	if err := cfg.validate(brokerErr); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func source(files ...string) (*envi.Env, error) {
	env := envi.New()

	for _, path := range files {
		e, err := envi.Load(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("load %s: %w", path, err)
		}
		if err := env.Merge(e); err != nil {
			return nil, fmt.Errorf("merge %s: %w", path, err)
		}
	}

	// The process environment wins over every file.
	for _, kv := range os.Environ() {
		if k, v, ok := strings.Cut(kv, "="); ok {
			env.Set(k, v)
		}
	}

	return env, nil
}

func defaultInstance() string {
	if host, err := os.Hostname(); err == nil && host != "" {
		return host
	}

	return fmt.Sprintf("pid-%d", os.Getpid())
}
