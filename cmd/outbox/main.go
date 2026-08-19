// Command outbox is a Transactional Outbox dispatcher: it reads messages a
// producer wrote to a database table inside its own business transaction and
// publishes them to a message broker.
//
// Several instances may run against one table. Claims are taken with
// FOR UPDATE SKIP LOCKED and carry a lease token that every write-back must
// present, so a replica whose lease expired mid-flight cannot overwrite the
// outcome recorded by whoever reclaimed the row.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/efureev/go-outbox/internal/app"
	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/logging"
)

// Build information, stamped by the linker.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run dispatches, and the two writers are arguments rather than os.Stdout and
// os.Stderr reached for directly: the dispatch table is the one part of the
// command line that can be checked without a database behind it.
func run(args []string, stdout, stderr io.Writer) int {
	command := "run"
	if len(args) > 0 {
		command = args[0]
		args = args[1:]
	}

	switch command {
	case "run", "serve":
		return runServer(args)
	case "migrate":
		return runMigrate(args, stdout, stderr)
	case "failed":
		return runFailed(args, stdout, stderr)
	case "requeue":
		return runRequeue(args, stdout, stderr)
	case "stats":
		return runStats(args, stdout, stderr)
	case "version", "-v", "--version":
		fmt.Fprintf(stdout, "outbox %s (commit %s, built %s)\n", version, commit, date)

		return 0
	case "help", "-h", "--help":
		usage(stdout)

		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n", command)
		usage(stderr)

		return 2
	}
}

func runServer(_ []string) int {
	cfg, err := config.Load(".env")
	if err != nil {
		// The logger is configured from the very configuration that failed to
		// load, so this one message goes to stderr directly.
		fmt.Fprintln(os.Stderr, err)

		return 1
	}

	log, err := logging.New(cfg.Log, os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)

		return 1
	}

	// The same value a claim records in the owner column, so a log line and the
	// row it is about name the same replica. Without it, several replicas
	// shipping to one collector produce indistinguishable lines.
	log = log.With(slog.String(logging.InstanceKey, cfg.App.Instance))

	application, err := app.New(cfg, log, app.Build{Version: version, Commit: commit, Date: date})
	if err != nil {
		log.Error("failed to assemble the application", errAttr(err))

		return 1
	}

	if err := application.Run(context.Background()); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("shutdown did not complete cleanly", errAttr(err))

		if code := application.ExitCode(); code != 0 {
			return code
		}

		return 1
	}

	return application.ExitCode()
}

func usage(w io.Writer) {
	fmt.Fprint(w, `Usage: outbox <command>

Commands:
  run              Start the dispatcher (default)
  migrate up       Apply pending schema migrations
  migrate status   Show which migrations are applied
  stats            Show the backlog and the configured streams
  failed           List messages that stopped being retried
  requeue          Return failed messages to the queue
  version          Print build information
  help             Print this message

  stats, failed and requeue are the admin API's operations over the database
  connection instead of over HTTP. Pass -h to any of them for its flags.

Configuration is read from the environment and from .env, if present.
Every variable lives under the OUTBOX_ prefix; see docs/Config.md.
`)
}
