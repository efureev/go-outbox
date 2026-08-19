package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/logging"
	"github.com/efureev/go-outbox/internal/store"
)

const migrateTimeout = 5 * time.Minute

func runMigrate(args []string, stdout, stderr io.Writer) int {
	action := "up"
	if len(args) > 0 {
		action = args[0]
	}

	// The action is checked before anything is loaded. It costs nothing, and an
	// operator who mistyped it should be told that rather than being told about
	// database configuration they did not ask about yet.
	if action != "up" && action != "status" {
		fmt.Fprintf(stderr, "unknown migrate action %q (want up or status)\n", action)

		return 2
	}

	// Migrating needs a database and nothing else, so it is not held up by a
	// routing table it never consults.
	cfg, err := config.LoadAdmin(".env")
	if err != nil {
		fmt.Fprintln(stderr, err)

		return 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), migrateTimeout)
	defer cancel()

	switch action {
	case "up":
		log, err := logging.New(cfg.Log, stdout)
		if err != nil {
			fmt.Fprintln(stderr, err)

			return 1
		}

		applied, err := store.Migrate(ctx, cfg, log)
		if err != nil {
			fmt.Fprintln(stderr, err)

			return 1
		}

		if len(applied) == 0 {
			fmt.Fprintln(stdout, "schema is up to date")

			return 0
		}
		for _, r := range applied {
			fmt.Fprintf(stdout, "applied %04d_%s\n", r.Version, r.Name)
		}

		return 0

	case "status":
		applied, shipped, err := store.MigrationStatus(ctx, cfg)
		if err != nil {
			fmt.Fprintln(stderr, err)

			return 1
		}

		done := make(map[int]time.Time, len(applied))
		for _, r := range applied {
			done[r.Version] = r.AppliedAt
		}

		fmt.Fprintf(stdout, "%-6s  %-24s  %s\n", "VER", "NAME", "APPLIED")
		for _, r := range shipped {
			at := "pending"
			if t, ok := done[r.Version]; ok {
				at = t.UTC().Format(time.RFC3339)
			}
			fmt.Fprintf(stdout, "%-6d  %-24s  %s\n", r.Version, r.Name, at)
		}

		return 0

	default:
		// Unreachable: the action was checked above.
		return 2
	}
}
