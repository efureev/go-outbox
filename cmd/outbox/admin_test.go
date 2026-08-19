package main

import "testing"

// A broker error arrives with whatever newlines the driver put in it. Printed
// as-is into a column, one of them turns the table into something no reader —
// and no awk — can follow.
func TestOneLineFlattensBrokerErrors(t *testing.T) {
	got := oneLine("rabbitmq: publish failed\n\tconnection reset\n")

	if want := "rabbitmq: publish failed connection reset"; got != want {
		t.Errorf("oneLine = %q, want %q", got, want)
	}
	if oneLine("") != "" {
		t.Errorf("oneLine on an empty error = %q", oneLine(""))
	}
}

func TestTruncate(t *testing.T) {
	cases := map[string]struct{ in, want string }{
		"short enough is untouched": {"local", "local"},
		"exactly the limit":         {"1234567890", "1234567890"},
		"longer is cut":             {"12345678901", "123456789…"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := truncate(tc.in, 10); got != tc.want {
				t.Errorf("truncate(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
