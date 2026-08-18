package core

import (
	"errors"
	"fmt"
	"testing"
)

func TestIsPermanentSeesThroughWrapping(t *testing.T) {
	base := errors.New("no such exchange")
	err := fmt.Errorf("publish: %w", Permanent("unroutable", base))

	if !IsPermanent(err) {
		t.Error("IsPermanent should see through wrapping")
	}
	if !errors.Is(err, base) {
		t.Error("the original cause should stay reachable with errors.Is")
	}
}

func TestIsPermanentRejectsOrdinaryErrors(t *testing.T) {
	if IsPermanent(fmt.Errorf("connection reset: %w", errors.New("EOF"))) {
		t.Error("an ordinary error must be retryable")
	}
	if IsPermanent(nil) {
		t.Error("nil is not a permanent failure")
	}
}

func TestPermanentErrorMessageNamesTheReason(t *testing.T) {
	err := Permanent("payload too large", errors.New("limit is 1MiB"))

	want := "permanent (payload too large): limit is 1MiB"
	if err.Error() != want {
		t.Errorf("Error() = %q, want %q", err.Error(), want)
	}
}

func TestStatusString(t *testing.T) {
	for status, want := range map[Status]string{
		StatusPending:    "pending",
		StatusProcessing: "processing",
		StatusSent:       "sent",
		StatusFailed:     "failed",
		Status(9):        "unknown",
	} {
		if got := status.String(); got != want {
			t.Errorf("Status(%d).String() = %q, want %q", status, got, want)
		}
	}
}
