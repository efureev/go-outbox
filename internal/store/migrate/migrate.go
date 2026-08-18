// Package migrate applies the embedded SQL migrations.
//
// It is deliberately small: forward-only, one transaction per file, a recorded
// checksum per file, and an advisory lock around the whole run so that N
// replicas starting at the same moment do not race. The previous version had no
// runner and no schema-version table at all; migrations were applied by hand or
// by a docker-entrypoint script, and a file was edited after release without
// anything noticing.
package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/efureev/go-outbox/migrations"
)

// Options configures a run.
type Options struct {
	Schema  string
	Table   string
	Channel string
	// LockKey namespaces the advisory lock held for the duration of the run.
	LockKey int64
	// LockTimeout bounds how long to wait for another replica's run to finish.
	LockTimeout time.Duration
	Logger      *slog.Logger
}

// Record is one applied migration.
type Record struct {
	Version   int
	Name      string
	Checksum  string
	AppliedAt time.Time
}

// ErrChecksumMismatch reports that a migration file changed after it was
// applied. It is not recoverable automatically: either the file was edited,
// which must be undone, or the database was migrated by a different build.
var ErrChecksumMismatch = errors.New("migration file changed after it was applied")

var fileName = regexp.MustCompile(`^(\d+)_([a-z0-9_]+)\.sql$`)

type migration struct {
	version  int
	name     string
	template string
	checksum string
}

// Run applies every migration not yet recorded and reports the ones it applied.
//
// conn is used exclusively for the duration of the run and is left with its
// settings unchanged.
func Run(ctx context.Context, conn *pgx.Conn, opts Options) ([]Record, error) {
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	pending, err := load()
	if err != nil {
		return nil, err
	}

	release, err := lock(ctx, conn, opts)
	if err != nil {
		return nil, err
	}
	defer release()

	if err := bootstrap(ctx, conn, opts); err != nil {
		return nil, err
	}

	applied, err := recorded(ctx, conn, opts)
	if err != nil {
		return nil, err
	}

	var done []Record
	for _, m := range pending {
		if prev, ok := applied[m.version]; ok {
			if prev.Checksum != m.checksum {
				return done, fmt.Errorf("%w: %04d_%s.sql (recorded %s, now %s)",
					ErrChecksumMismatch, m.version, m.name, short(prev.Checksum), short(m.checksum))
			}

			continue
		}

		rec, err := apply(ctx, conn, opts, m)
		if err != nil {
			return done, fmt.Errorf("apply migration %04d_%s: %w", m.version, m.name, err)
		}

		log.Info("migration applied",
			slog.Int("version", m.version),
			slog.String("name", m.name),
		)
		done = append(done, rec)
	}

	return done, nil
}

// Status reports the migrations recorded in the database.
func Status(ctx context.Context, conn *pgx.Conn, opts Options) ([]Record, error) {
	if err := bootstrap(ctx, conn, opts); err != nil {
		return nil, err
	}

	applied, err := recorded(ctx, conn, opts)
	if err != nil {
		return nil, err
	}

	out := make([]Record, 0, len(applied))
	for _, rec := range applied {
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })

	return out, nil
}

// Pending reports the migrations shipped with this build, in order.
func Pending() ([]Record, error) {
	ms, err := load()
	if err != nil {
		return nil, err
	}

	out := make([]Record, len(ms))
	for i, m := range ms {
		out[i] = Record{Version: m.version, Name: m.name, Checksum: m.checksum}
	}

	return out, nil
}

func load() ([]migration, error) {
	entries, err := fs.ReadDir(migrations.FS, ".")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}

	var out []migration
	seen := map[int]string{}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}

		m := fileName.FindStringSubmatch(e.Name())
		if m == nil {
			return nil, fmt.Errorf("migration %q does not match <version>_<name>.sql", e.Name())
		}

		version, err := strconv.Atoi(m[1])
		if err != nil {
			return nil, fmt.Errorf("migration %q has an unreadable version: %w", e.Name(), err)
		}
		if prev, dup := seen[version]; dup {
			return nil, fmt.Errorf("migrations %q and %q share version %d", prev, e.Name(), version)
		}
		seen[version] = e.Name()

		body, err := migrations.FS.ReadFile(e.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", e.Name(), err)
		}

		sum := sha256.Sum256(body)
		out = append(out, migration{
			version:  version,
			name:     m[2],
			template: string(body),
			// The checksum covers the template, not the substituted SQL, so
			// pointing the dispatcher at a different schema is not mistaken
			// for someone having edited a released file.
			checksum: hex.EncodeToString(sum[:]),
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })

	return out, nil
}
