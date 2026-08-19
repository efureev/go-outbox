package logging

import (
	"context"
	"log/slog"
)

// Field names every log line is expected to carry.
const (
	// ModuleKey names the subsystem that performed the action. One name for
	// the concept, so `grep module=db` returns everything about a subsystem —
	// what the lifecycle manager did to it and what it did itself.
	ModuleKey = "module"
	// ComponentKey names a framework whose logging is not attributable to any
	// one module. It is deliberately a different key from ModuleKey: appmod
	// attaches its own `module` attribute naming the subject of an action, and
	// reggol renders a repeated key twice rather than replacing it.
	ComponentKey = "component"
	// InstanceKey names the replica, matching the owner column a claim records.
	InstanceKey = "instance"
)

// handler sits between slog and the reggol bridge, doing the two things the
// bridge cannot.
//
// It delegates everything else rather than replacing the bridge: slogr is
// verified against testing/slogtest, and reimplementing it for cosmetics would
// trade that away.
type handler struct {
	next slog.Handler

	// module is the bound subsystem name, taken from WithAttrs.
	module string
	// prefix renders that name ahead of the message. Console only: the JSON
	// output is read by collectors that want the message unadorned and the
	// name in its own field.
	prefix bool
	// caller keeps the call site. The bridge records it on every event, so
	// without this the setting that is supposed to control it does nothing.
	caller bool
}

func (h *handler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

// WithAttrs remembers the subsystem name as it is bound.
//
// Only bound attributes are considered. A `module` attribute on an individual
// record names the *subject* of an action — appmod logs "starting module" with
// module=db — whereas the prefix answers who is speaking.
func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := *h

	// The attribute that becomes the prefix is dropped from the fields when it
	// is going to be rendered ahead of the message, or every console line would
	// carry the same name twice — "[db] … module=db". In the JSON output there
	// is no prefix, so nothing is dropped and the field stays where a collector
	// expects it.
	kept := attrs

	for i, a := range attrs {
		adopted := false

		switch a.Key {
		case ModuleKey:
			next.module, adopted = a.Value.String(), true
		case ComponentKey:
			// Only as a fallback: a module name is the more specific answer.
			if next.module == "" {
				next.module, adopted = a.Value.String(), true
			}
		}

		if adopted && h.prefix {
			kept = make([]slog.Attr, 0, len(attrs)-1)
			kept = append(kept, attrs[:i]...)
			kept = append(kept, attrs[i+1:]...)
		}
	}

	next.next = h.next.WithAttrs(kept)

	return &next
}

func (h *handler) WithGroup(name string) slog.Handler {
	next := *h
	next.next = h.next.WithGroup(name)

	return &next
}

func (h *handler) Handle(ctx context.Context, r slog.Record) error {
	// A Record is passed by value, so both edits are local to this delivery.
	if !h.caller {
		// resolvePC in reggol reports "no caller" for a zero PC, which is what
		// suppresses the call site downstream.
		r.PC = 0
	}

	if h.prefix && h.module != "" {
		r.Message = "[" + h.module + "] " + r.Message
	}

	return h.next.Handle(ctx, r)
}
