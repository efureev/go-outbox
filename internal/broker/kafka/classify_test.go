package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/segmentio/kafka-go"

	"github.com/efureev/go-outbox/internal/core"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		parentLive bool
		permanent  bool
		deferred   bool
	}{
		{
			name:      "a payload the broker will never accept is permanent",
			err:       kafka.MessageSizeTooLarge,
			permanent: true,
		},
		{
			name:      "a topic that does not exist is permanent, because nothing here will create it",
			err:       kafka.UnknownTopicOrPartition,
			permanent: true,
		},
		{
			name:     "a cluster with no leader has not judged the message",
			err:      kafka.LeaderNotAvailable,
			deferred: true,
		},
		{
			name:     "under-replicated is the cluster refusing every write, not this one",
			err:      kafka.NotEnoughReplicas,
			deferred: true,
		},
		{
			name:     "a refused connection is an outage",
			err:      &net.OpError{Op: "dial", Err: errors.New("connection refused")},
			deferred: true,
		},
		{
			name:     "a connection that died mid-write is an outage",
			err:      fmt.Errorf("write: %w", io.ErrUnexpectedEOF),
			deferred: true,
		},
		{
			name:       "our own write deadline firing means nothing answered",
			err:        context.DeadlineExceeded,
			parentLive: true,
			deferred:   true,
		},
		{
			// Otherwise every message in flight during a shutdown is recorded
			// as an outage and waits out a deferral it never earned.
			name:       "the same deadline during a shutdown is not an outage",
			err:        context.DeadlineExceeded,
			parentLive: false,
		},
		{
			name: "an unrecognised error stays retryable",
			err:  errors.New("something else went wrong"),
		},
		{
			name: "nil stays nil",
			err:  nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classify(tc.err, tc.parentLive)

			if tc.err == nil {
				if got != nil {
					t.Fatalf("classify(nil) = %v", got)
				}

				return
			}

			if core.IsPermanent(got) != tc.permanent {
				t.Errorf("IsPermanent = %v, want %v (%v)", core.IsPermanent(got), tc.permanent, got)
			}
			if core.IsUnavailable(got) != tc.deferred {
				t.Errorf("IsUnavailable = %v, want %v (%v)", core.IsUnavailable(got), tc.deferred, got)
			}
			if !errors.Is(got, tc.err) {
				t.Errorf("classification lost the cause: %v", got)
			}
		})
	}
}

// kafka.Error carries Timeout and Temporary methods, which is precisely the
// shape of net.Error. Checking the network case first would therefore read
// every protocol verdict as an outage, and no message would ever exhaust its
// attempts again — the messages would come back forever, and the counter that
// is supposed to stop them would sit at zero.
func TestProtocolErrorsAreNotMistakenForNetworkFailures(t *testing.T) {
	var nerr net.Error
	if !errors.As(error(kafka.MessageSizeTooLarge), &nerr) {
		t.Skip("kafka.Error no longer satisfies net.Error; the ordering this guards is moot")
	}

	got := classify(kafka.MessageSizeTooLarge, true)

	if core.IsUnavailable(got) {
		t.Error("a protocol rejection was classified as an outage, so it would never spend an attempt")
	}
	if !core.IsPermanent(got) {
		t.Errorf("a payload above the broker's limit is not permanent: %v", got)
	}
}
