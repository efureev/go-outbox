package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/store"
)

// The administrative commands are the same operations the admin API exposes,
// reached over the database connection the dispatcher already uses rather than
// over HTTP.
//
// They exist because the HTTP route needs a reachable pod, a token and a JSON
// body, and at three in the morning what is to hand is a shell inside the
// container the binary already lives in. Both paths run the same store calls, so
// neither can drift into being the one that does it correctly.
//
// Authorisation differs, and deliberately. The endpoints are guarded by
// OUTBOX_HTTP_ADMIN_TOKEN because anything that can route to the pod can call
// them; these are guarded by holding the database credentials, which is a
// stronger thing to have than a token.

const adminTimeout = 2 * time.Minute

// adminStore opens a pool for a one-shot command and returns it with its
// closer. The application name is distinct from the dispatcher's so an idle
// connection in pg_stat_activity names what opened it.
func adminStore(ctx context.Context, cfg config.Config) (*store.Store, func(), error) {
	pool, err := store.NewPool(ctx, cfg.DB, cfg.App.Name+"-cli")
	if err != nil {
		return nil, nil, err
	}

	return store.New(pool, cfg.DB), pool.Close, nil
}

// withStore loads the configuration, opens a store and hands both to fn. Every
// administrative command starts this way, and none of them wants to repeat the
// three failure paths that precede doing anything useful.
func withStore(fn func(context.Context, config.Config, *store.Store) error) int {
	cfg, err := config.LoadAdmin(".env")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)

		return 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), adminTimeout)
	defer cancel()

	st, closeStore, err := adminStore(ctx, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)

		return 1
	}
	defer closeStore()

	if err := fn(ctx, cfg, st); err != nil {
		fmt.Fprintln(os.Stderr, err)

		return 1
	}

	return 0
}

func runFailed(args []string) int {
	fs := flag.NewFlagSet("outbox failed", flag.ContinueOnError)
	limit := fs.Int("limit", 20, "how many messages to show")
	stream := fs.String("stream", "", "show only this stream")
	asJSON := fs.Bool("json", false, "print JSON instead of a table")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	return withStore(func(ctx context.Context, _ config.Config, st *store.Store) error {
		messages, err := st.ListFailed(ctx, store.Cursor{}, *limit, *stream)
		if err != nil {
			return err
		}

		if *asJSON {
			return printJSON(os.Stdout, map[string]any{"messages": messages})
		}

		if len(messages) == 0 {
			fmt.Println("nothing has failed")

			return nil
		}

		fmt.Printf("%-36s  %-12s  %-24s  %4s  %-20s  %s\n",
			"ID", "STREAM", "TOPIC", "ATT", "CREATED", "ERROR")
		for _, m := range messages {
			fmt.Printf("%-36s  %-12s  %-24s  %4d  %-20s  %s\n",
				m.ID, truncate(m.Stream, 12), truncate(m.Topic, 24), m.Attempts,
				m.CreatedAt.UTC().Format(time.RFC3339), oneLine(m.LastError))
		}

		return nil
	})
}

func runRequeue(args []string) int {
	fs := flag.NewFlagSet("outbox requeue", flag.ContinueOnError)
	before := fs.String("before", "",
		"requeue everything that failed before this RFC3339 time, instead of by id")
	limit := fs.Int("limit", 1000, "with -before, how many messages to move at most")
	asJSON := fs.Bool("json", false, "print JSON instead of a summary")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	ids := fs.Args()

	switch {
	case len(ids) > 0 && *before != "":
		fmt.Fprintln(os.Stderr, "give either ids or -before, not both")

		return 2
	case len(ids) == 0 && *before == "":
		fmt.Fprintln(os.Stderr, "give either ids or -before")
		fs.Usage()

		return 2
	}

	return withStore(func(ctx context.Context, _ config.Config, st *store.Store) error {
		var (
			requeued []string
			err      error
		)

		if len(ids) > 0 {
			requeued, err = st.Requeue(ctx, ids)
		} else {
			var cutoff time.Time
			if cutoff, err = time.Parse(time.RFC3339, *before); err != nil {
				// time.Parse reports the layout it failed against and quotes
				// non-ASCII input byte by byte, which tells an operator holding
				// a mistyped timestamp nothing they can act on.
				return fmt.Errorf(
					"-before must be an RFC3339 time such as 2026-01-31T23:00:00Z, got %q", *before)
			}
			requeued, err = st.RequeueFailedBefore(ctx, cutoff, *limit)
		}

		if err != nil {
			return err
		}

		if *asJSON {
			return printJSON(os.Stdout, map[string]any{"requeued": len(requeued), "ids": requeued})
		}

		// Requeue only moves rows that were actually failed, so asking for ten
		// and moving three is the normal way to learn that seven of them were
		// not in that state — worth saying rather than reporting a bare count.
		if len(ids) > 0 && len(requeued) != len(ids) {
			fmt.Printf("requeued %d of %d; the rest were not in the failed state\n",
				len(requeued), len(ids))

			return nil
		}
		fmt.Printf("requeued %d message(s)\n", len(requeued))

		return nil
	})
}

func runStats(args []string) int {
	fs := flag.NewFlagSet("outbox stats", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "print JSON instead of a summary")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	return withStore(func(ctx context.Context, cfg config.Config, st *store.Store) error {
		st2, err := st.Stats(ctx)
		if err != nil {
			return err
		}

		if *asJSON {
			return printJSON(os.Stdout, map[string]any{
				"messages": map[string]any{
					"pending":                st2.Pending,
					"processing":             st2.Processing,
					"failed":                 st2.Failed,
					"deferred":               st2.Deferred,
					"oldest_pending_seconds": st2.OldestPending.Seconds(),
				},
				"streams": streamMap(cfg),
			})
		}

		fmt.Printf("%-14s %d\n", "pending", st2.Pending)
		fmt.Printf("%-14s %d\n", "processing", st2.Processing)
		fmt.Printf("%-14s %d\n", "failed", st2.Failed)
		// Deferred overlaps the counts above rather than adding to them, and a
		// reader who takes it for a fourth status will not be able to make the
		// numbers add up.
		fmt.Printf("%-14s %d (waiting on an unreachable broker; counted above too)\n",
			"deferred", st2.Deferred)
		fmt.Printf("%-14s %s\n", "oldest", st2.OldestPending.Truncate(time.Second))

		// Empty when the routing table could not be assembled. The counts above
		// are the point of this command and do not depend on it.
		if names := cfg.Brokers.StreamNames(); len(names) > 0 {
			fmt.Printf("\n%-16s %s\n", "STREAM", "DRIVER")
			for _, name := range names {
				fmt.Printf("%-16s %s\n", name, cfg.Brokers.Streams[name].Driver)
			}
		}

		return nil
	})
}

func streamMap(cfg config.Config) map[string]string {
	out := make(map[string]string, len(cfg.Brokers.Streams))
	for name, s := range cfg.Brokers.Streams {
		out[name] = s.Driver
	}

	return out
}

func printJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")

	return enc.Encode(v)
}

// oneLine keeps a multi-line broker error from breaking the table apart.
func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}

	return s[:n-1] + "…"
}
