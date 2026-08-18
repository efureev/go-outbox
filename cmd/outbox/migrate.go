package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/logging"
	"github.com/efureev/go-outbox/internal/store"
)

const migrateTimeout = 5 * time.Minute

func runMigrate(args []string) int {
	action := "up"
	if len(args) > 0 {
		action = args[0]
	}

	cfg, err := config.Load(".env")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)

		return 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), migrateTimeout)
	defer cancel()

	switch action {
	case "up":
		log, err := logging.New(cfg.Log, os.Stdout)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)

			return 1
		}

		applied, err := store.Migrate(ctx, cfg, log)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)

			return 1
		}

		if len(applied) == 0 {
			fmt.Println("schema is up to date")

			return 0
		}
		for _, r := range applied {
			fmt.Printf("applied %04d_%s\n", r.Version, r.Name)
		}

		return 0

	case "status":
		applied, shipped, err := store.MigrationStatus(ctx, cfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)

			return 1
		}

		done := make(map[int]time.Time, len(applied))
		for _, r := range applied {
			done[r.Version] = r.AppliedAt
		}

		fmt.Printf("%-6s  %-24s  %s\n", "VER", "NAME", "APPLIED")
		for _, r := range shipped {
			at := "pending"
			if t, ok := done[r.Version]; ok {
				at = t.UTC().Format(time.RFC3339)
			}
			fmt.Printf("%-6d  %-24s  %s\n", r.Version, r.Name, at)
		}

		return 0

	default:
		fmt.Fprintf(os.Stderr, "unknown migrate action %q (want up or status)\n", action)

		return 2
	}
}
