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
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	command := "run"
	if len(args) > 0 {
		command = args[0]
		args = args[1:]
	}

	switch command {
	case "run", "serve":
		return runServer(args)
	case "migrate":
		return runMigrate(args)
	case "version", "-v", "--version":
		fmt.Printf("outbox %s (commit %s, built %s)\n", version, commit, date)

		return 0
	case "help", "-h", "--help":
		usage(os.Stdout)

		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", command)
		usage(os.Stderr)

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

func usage(w *os.File) {
	fmt.Fprint(w, `Usage: outbox <command>

Commands:
  run              Start the dispatcher (default)
  migrate up       Apply pending schema migrations
  migrate status   Show which migrations are applied
  version          Print build information
  help             Print this message

Configuration is read from the environment and from .env, if present.
Every variable lives under the OUTBOX_ prefix; see docs/Config.md.
`)
}
