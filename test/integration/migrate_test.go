//go:build integration

package integration

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/efureev/go-outbox/internal/store/migrate"
)

func TestMigrateIsIdempotent(t *testing.T) {
	f := newFixture(t)

	// newFixture already migrated; a second run must find nothing to do.
	conn := rawConn(t, f.Config)

	applied, err := migrate.Run(t.Context(), conn, migrateOptions(f.Config))
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if len(applied) != 0 {
		t.Errorf("second run applied %d migrations, want 0", len(applied))
	}

	recorded, err := migrate.Status(t.Context(), conn, migrateOptions(f.Config))
	if err != nil {
		t.Fatalf("status: %v", err)
	}

	pending, err := migrate.Pending()
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(recorded) != len(pending) {
		t.Errorf("%d migrations recorded, %d shipped with the binary", len(recorded), len(pending))
	}
}

// A migration file edited after release must be refused rather than skipped.
//
// The previous version had no schema-version table at all, and its initial
// migration was in fact edited afterwards: it grew a column and an index that a
// later migration also added. A fresh install and an upgraded install therefore
// ended up with different schemas, and nothing anywhere noticed.
func TestMigrateRefusesAChangedFile(t *testing.T) {
	f := newFixture(t)
	conn := rawConn(t, f.Config)

	_, err := conn.Exec(t.Context(), fmt.Sprintf(
		`UPDATE %q.schema_migrations SET checksum = 'tampered' WHERE version = 1`, f.Schema))
	if err != nil {
		t.Fatalf("tamper: %v", err)
	}

	if _, err := migrate.Run(t.Context(), conn, migrateOptions(f.Config)); !errors.Is(err, migrate.ErrChecksumMismatch) {
		t.Fatalf("error = %v, want ErrChecksumMismatch", err)
	}
}

// Replicas starting at the same moment must not race each other into applying
// the same migration twice.
func TestConcurrentMigrationsApplyExactlyOnce(t *testing.T) {
	f := newFixture(t)

	// Start from an empty schema so there is something for the racers to apply.
	if _, err := f.Pool.Exec(t.Context(), fmt.Sprintf(`DROP SCHEMA %q CASCADE`, f.Schema)); err != nil {
		t.Fatalf("drop schema: %v", err)
	}

	const replicas = 5

	var (
		mu     sync.Mutex
		total  int
		failed []error
		wg     sync.WaitGroup
	)

	for range replicas {
		wg.Add(1)

		go func() {
			defer wg.Done()

			conn, err := pgx.Connect(t.Context(), f.Config.DB.DSN)
			if err != nil {
				mu.Lock()
				failed = append(failed, err)
				mu.Unlock()

				return
			}
			defer func() { _ = conn.Close(t.Context()) }()

			applied, err := migrate.Run(t.Context(), conn, migrateOptions(f.Config))

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				failed = append(failed, err)

				return
			}
			total += len(applied)
		}()
	}

	wg.Wait()

	if len(failed) > 0 {
		t.Fatalf("concurrent migration failed: %v", failed)
	}

	pending, err := migrate.Pending()
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if total != len(pending) {
		t.Errorf("%d migrations were applied in total across %d replicas, want %d exactly",
			total, replicas, len(pending))
	}
}

// The notification trigger has to reach the configured channel, since the
// channel name is substituted into the migration rather than fixed.
func TestNotifyTriggerFiresOnTheConfiguredChannel(t *testing.T) {
	f := newFixture(t)

	conn := rawConn(t, f.Config)
	if _, err := conn.Exec(t.Context(), `LISTEN `+quoted(f.Config.Dispatch.NotifyChannel)); err != nil {
		t.Fatalf("listen: %v", err)
	}

	f.seed(t, "local", 1)

	ctx := mustContext(t, notifyWait)

	notification, err := conn.WaitForNotification(ctx)
	if err != nil {
		t.Fatalf("no notification arrived within %s: %v", notifyWait, err)
	}
	if notification.Payload != "local" {
		t.Errorf("payload = %q, want the stream name so only the right pipeline wakes", notification.Payload)
	}
}
