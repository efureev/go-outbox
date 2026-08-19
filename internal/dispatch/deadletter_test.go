package dispatch

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/core"
	"github.com/efureev/go-outbox/internal/events"
	"github.com/efureev/go-outbox/internal/logging"
)

type fakeFetcher struct {
	msgs []core.Message
	err  error
	got  []string
}

func (f *fakeFetcher) FetchByIDs(_ context.Context, ids []string) ([]core.Message, error) {
	f.got = ids

	return f.msgs, f.err
}

type dlqRecorder struct {
	mu     sync.Mutex
	events []events.DeadLetter
}

func (r *dlqRecorder) DeadLetter(_ context.Context, ev events.DeadLetter) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.events = append(r.events, ev)
}

func (r *dlqRecorder) all() []events.DeadLetter {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]events.DeadLetter(nil), r.events...)
}

func dlqConfig() config.Config {
	cfg := testConfig()
	cfg.DLQ = config.DLQConfig{Enabled: true, Stream: "dlq", Topic: "outbox.dead-letter"}

	return cfg
}

func failedMessage(id, topic string) core.Message {
	return core.Message{
		ID: id, Stream: "local", Topic: topic, Payload: []byte(`{"n":1}`),
		Headers: map[string]string{"traceparent": "00-ab-cd-01"},
		Target:  core.Target{Key: "k", Exchange: "orders", RoutingKey: "rk", Version: 3},
	}
}

func TestDeadLetterForwardsWhatStoppedBeingRetried(t *testing.T) {
	fetch := &fakeFetcher{msgs: []core.Message{failedMessage("a", "order.created")}}
	router := &fakeRouter{errFor: map[string]error{}}
	rec := &dlqRecorder{}

	d := NewDeadLetter(fetch, router, rec, dlqConfig(), logging.Nop())

	err := d.Handle(t.Context(), events.Iteration{
		Stream: "local", Driver: "rmq",
		Failed: []events.Terminal{{ID: "a", Attempts: 5, Permanent: true}},
	})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if len(fetch.got) != 1 || fetch.got[0] != "a" {
		t.Errorf("fetched %v, want the failed id", fetch.got)
	}
	if evs := rec.all(); len(evs) != 1 || evs[0].Err != nil {
		t.Errorf("events = %+v, want one success", evs)
	}
}

// The circumstances of the death travel with the message: without them the
// dead-letter destination holds payloads nobody can place.
func TestDeadLetterRecordsWhereTheMessageCameFrom(t *testing.T) {
	fetch := &fakeFetcher{msgs: []core.Message{failedMessage("a", "order.created")}}
	router := &fakeRouter{errFor: map[string]error{}}

	d := NewDeadLetter(fetch, router, &dlqRecorder{}, dlqConfig(), logging.Nop())

	forwarded := d.rewrite(fetch.msgs[0], events.Terminal{ID: "a", Attempts: 5, Permanent: true})

	for key, want := range map[string]string{
		"x-outbox-original-topic":  "order.created",
		"x-outbox-original-stream": "local",
		"x-outbox-attempts":        "5",
		"x-outbox-permanent":       "true",
		// Whatever the producer set has to survive: a traceparent is how the
		// dead letter is tied back to the request that produced it.
		"traceparent": "00-ab-cd-01",
	} {
		if forwarded.Headers[key] != want {
			t.Errorf("header %s = %q, want %q", key, forwarded.Headers[key], want)
		}
	}

	if forwarded.Stream != "dlq" || forwarded.Topic != "outbox.dead-letter" {
		t.Errorf("readdressed to %s/%s", forwarded.Stream, forwarded.Topic)
	}
	// The routing envelope belonged to the original destination.
	if forwarded.Target.Exchange != "" || forwarded.Target.RoutingKey != "" || forwarded.Target.Version != 0 {
		t.Errorf("the original routing survived: %+v", forwarded.Target)
	}
	if string(forwarded.Payload) != `{"n":1}` {
		t.Errorf("the payload was touched: %s", forwarded.Payload)
	}
	// The original must not be mutated: it is the row the operator will read.
	if fetch.msgs[0].Topic != "order.created" {
		t.Error("rewriting changed the message it was given")
	}
}

// The dead-letter topic is a signal, not the record. A failure to forward must
// not become a failure of the iteration that produced it, or a broken DLQ
// destination would stop delivery of everything else.
func TestDeadLetterNeverFailsTheIteration(t *testing.T) {
	cases := map[string]*DeadLetter{
		"the fetch fails": NewDeadLetter(
			&fakeFetcher{err: errors.New("database gone")},
			&fakeRouter{errFor: map[string]error{}}, &dlqRecorder{}, dlqConfig(), logging.Nop()),
		"the publish fails": NewDeadLetter(
			&fakeFetcher{msgs: []core.Message{failedMessage("a", "t")}},
			&fakeRouter{errFor: map[string]error{"a": errors.New("dlq broker down")}},
			&dlqRecorder{}, dlqConfig(), logging.Nop()),
	}

	for name, d := range cases {
		t.Run(name, func(t *testing.T) {
			err := d.Handle(t.Context(), events.Iteration{
				Failed: []events.Terminal{{ID: "a", Attempts: 5}},
			})
			if err != nil {
				t.Errorf("Handle returned %v; a dead-letter failure must not surface here", err)
			}
		})
	}
}

// A failure to forward is still reported, per message, so it is visible as
// outbox_dlq_published_total{result="error"} rather than silent.
func TestDeadLetterReportsEveryOutcome(t *testing.T) {
	fetch := &fakeFetcher{msgs: []core.Message{
		failedMessage("a", "t1"), failedMessage("b", "t2"),
	}}
	router := &fakeRouter{errFor: map[string]error{"b": errors.New("gone")}}
	rec := &dlqRecorder{}

	d := NewDeadLetter(fetch, router, rec, dlqConfig(), logging.Nop())

	if err := d.Handle(t.Context(), events.Iteration{
		Failed: []events.Terminal{{ID: "a"}, {ID: "b"}},
	}); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	evs := rec.all()
	if len(evs) != 2 {
		t.Fatalf("events = %+v, want one per message", evs)
	}

	var failed int
	for _, ev := range evs {
		if ev.Err != nil {
			failed++
		}
	}
	if failed != 1 {
		t.Errorf("%d failures reported, want 1", failed)
	}
}

// A fetch that fails still reports one event per id, so the count of attempts
// matches the count of failures and the metric does not silently under-report.
func TestDeadLetterReportsEveryIDWhenTheFetchFails(t *testing.T) {
	rec := &dlqRecorder{}
	d := NewDeadLetter(&fakeFetcher{err: errors.New("gone")},
		&fakeRouter{errFor: map[string]error{}}, rec, dlqConfig(), logging.Nop())

	if err := d.Handle(t.Context(), events.Iteration{
		Failed: []events.Terminal{{ID: "a"}, {ID: "b"}, {ID: "c"}},
	}); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if evs := rec.all(); len(evs) != 3 {
		t.Errorf("%d events for three ids", len(evs))
	}
}

func TestDeadLetterDoesNothingWhenItHasNothingToDo(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		fetch := &fakeFetcher{}
		cfg := dlqConfig()
		cfg.DLQ.Enabled = false

		d := NewDeadLetter(fetch, &fakeRouter{errFor: map[string]error{}}, &dlqRecorder{}, cfg, logging.Nop())

		if err := d.Handle(t.Context(), events.Iteration{
			Failed: []events.Terminal{{ID: "a"}},
		}); err != nil {
			t.Fatalf("Handle: %v", err)
		}
		if fetch.got != nil {
			t.Error("a disabled forwarder still read the database")
		}
	})

	t.Run("nothing failed", func(t *testing.T) {
		fetch := &fakeFetcher{}

		d := NewDeadLetter(fetch, &fakeRouter{errFor: map[string]error{}},
			&dlqRecorder{}, dlqConfig(), logging.Nop())

		if err := d.Handle(t.Context(), events.Iteration{Claimed: 10}); err != nil {
			t.Fatalf("Handle: %v", err)
		}
		if fetch.got != nil {
			t.Error("an iteration with no failures still read the database")
		}
	})
}
