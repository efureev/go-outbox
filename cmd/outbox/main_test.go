package main

import (
	"bytes"
	"strings"
	"testing"
)

// dispatch runs the command line and returns the exit code with both streams.
// Nothing here reaches a database: these are the paths an operator hits before
// one is needed, and the ones a typo lands on.
func dispatch(args ...string) (code int, stdout, stderr string) {
	var out, errs bytes.Buffer
	code = run(args, &out, &errs)

	return code, out.String(), errs.String()
}

func TestVersionPrintsTheStampedBuild(t *testing.T) {
	for _, form := range []string{"version", "-v", "--version"} {
		code, out, _ := dispatch(form)

		if code != 0 {
			t.Errorf("%q exited %d", form, code)
		}
		for _, want := range []string{version, commit, date} {
			if !strings.Contains(out, want) {
				t.Errorf("%q printed %q, missing %q", form, out, want)
			}
		}
	}
}

func TestHelpGoesToStdout(t *testing.T) {
	for _, form := range []string{"help", "-h", "--help"} {
		code, out, errs := dispatch(form)

		if code != 0 {
			t.Errorf("%q exited %d, want 0 — help was asked for", form, code)
		}
		if !strings.Contains(out, "Usage: outbox") {
			t.Errorf("%q printed %q", form, out)
		}
		if errs != "" {
			t.Errorf("%q wrote %q to stderr", form, errs)
		}
	}
}

// An unknown command is a mistake, so it goes to stderr with a non-zero code —
// a shell script that mistypes must not carry on believing it succeeded.
func TestAnUnknownCommandFailsLoudly(t *testing.T) {
	code, out, errs := dispatch("requeu")

	if code != 2 {
		t.Errorf("exit code %d, want 2", code)
	}
	if out != "" {
		t.Errorf("an unknown command wrote %q to stdout", out)
	}
	if !strings.Contains(errs, `unknown command "requeu"`) {
		t.Errorf("stderr = %q, want it to quote what was typed", errs)
	}
	if !strings.Contains(errs, "Usage: outbox") {
		t.Error("an unknown command did not show what the commands are")
	}
}

// Every command the usage text advertises has to be one the dispatcher knows.
// The two drift apart silently otherwise: a command renamed in the switch leaves
// the help promising something that now exits 2.
func TestEveryAdvertisedCommandIsDispatched(t *testing.T) {
	var help bytes.Buffer
	usage(&help)

	// Anything in the help that is not a dispatched command reaches the default
	// branch and exits 2 rather than reaching its own flag parsing.
	for _, name := range []string{"migrate", "stats", "failed", "requeue", "version", "help"} {
		if !strings.Contains(help.String(), name) {
			t.Errorf("the usage text does not mention %q", name)
		}

		code, _, _ := dispatch(name, "-h")
		if code == 2 && strings.Contains(mustStderr(t, name, "-h"), "unknown command") {
			t.Errorf("%q is advertised but not dispatched", name)
		}
	}
}

func mustStderr(t *testing.T, args ...string) string {
	t.Helper()

	_, _, errs := dispatch(args...)

	return errs
}

// -h on a subcommand is a question, and flag's ContinueOnError reports it as an
// error. The exit code is 2 either way; what matters is that the flags are
// listed, and on stderr rather than into a pipe expecting data.
func TestSubcommandFlagsAreListedOnStderr(t *testing.T) {
	cases := map[string][]string{
		"failed":  {"-limit", "-stream", "-json"},
		"requeue": {"-before", "-limit", "-json"},
		"stats":   {"-json"},
	}

	for name, flags := range cases {
		t.Run(name, func(t *testing.T) {
			code, out, errs := dispatch(name, "-h")

			if code != 2 {
				t.Errorf("exit code %d, want 2", code)
			}
			if out != "" {
				t.Errorf("the flag listing went to stdout: %q", out)
			}
			for _, f := range flags {
				if !strings.Contains(errs, f) {
					t.Errorf("%s is not listed:\n%s", f, errs)
				}
			}
		})
	}
}

// An unparseable flag must stop before the database is opened, or a typo costs a
// connection and a two-minute timeout.
func TestABadFlagStopsBeforeTheDatabase(t *testing.T) {
	for _, args := range [][]string{
		{"failed", "-limit", "not-a-number"},
		{"requeue", "-limit", "not-a-number"},
		{"stats", "-nonsense"},
	} {
		code, out, _ := dispatch(args...)

		if code != 2 {
			t.Errorf("%v exited %d, want 2", args, code)
		}
		if out != "" {
			t.Errorf("%v printed %q to stdout", args, out)
		}
	}
}

// Requeue takes ids or a cutoff. Both is ambiguous and neither is a no-op, and
// guessing at either would move rows the operator did not ask for.
func TestRequeueRefusesAnAmbiguousRequest(t *testing.T) {
	code, _, errs := dispatch("requeue", "-before", "2026-01-31T23:00:00Z", "id-1")

	if code != 2 {
		t.Errorf("exit code %d, want 2", code)
	}
	if !strings.Contains(errs, "not both") {
		t.Errorf("stderr = %q", errs)
	}
}

func TestRequeueRefusesAnEmptyRequest(t *testing.T) {
	code, _, errs := dispatch("requeue")

	if code != 2 {
		t.Errorf("exit code %d, want 2", code)
	}
	if !strings.Contains(errs, "give either ids or -before") {
		t.Errorf("stderr = %q", errs)
	}
	// The usage follows, so the operator does not have to ask twice.
	if !strings.Contains(errs, "-before") {
		t.Errorf("the flags were not shown:\n%s", errs)
	}
}

func TestAnUnknownMigrateActionIsRefused(t *testing.T) {
	var out, errs bytes.Buffer
	code := runMigrate([]string{"down"}, &out, &errs)

	if code != 2 {
		t.Errorf("exit code %d, want 2", code)
	}
	if !strings.Contains(errs.String(), `unknown migrate action "down"`) {
		t.Errorf("stderr = %q", errs.String())
	}
	if !strings.Contains(errs.String(), "want up or status") {
		t.Errorf("the message does not say what is allowed: %q", errs.String())
	}
	if out.Len() != 0 {
		t.Errorf("it printed %q to stdout", out.String())
	}
}

// The usage text is the only documentation reachable without a browser.
func TestUsageDescribesEveryCommand(t *testing.T) {
	var buf bytes.Buffer
	usage(&buf)
	text := buf.String()

	for _, want := range []string{
		"run", "migrate up", "migrate status", "stats", "failed", "requeue",
		"version", "help",
		"OUTBOX_", // where configuration comes from
		"docs/Config.md",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the usage text is missing %q", want)
		}
	}
}

func TestErrAttrNamesTheError(t *testing.T) {
	attr := errAttr(errBoom{})

	if attr.Key != "error" {
		t.Errorf("key = %q, want error", attr.Key)
	}
	if !strings.Contains(attr.Value.String(), "boom") {
		t.Errorf("value = %q", attr.Value.String())
	}
}

type errBoom struct{}

func (errBoom) Error() string { return "boom" }
