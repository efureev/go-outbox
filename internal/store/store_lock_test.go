package store

import "testing"

// A release function that panics the second time it is called is a trap:
// releasing early and then again through a defer is the ordinary way to write
// the call site, and it is how this was found.
func TestReleaseIsIdempotent(t *testing.T) {
	calls := 0
	release := idempotent(func() { calls++ })

	release()
	release()
	release()

	if calls != 1 {
		t.Errorf("the underlying release ran %d times, want exactly 1", calls)
	}
}
