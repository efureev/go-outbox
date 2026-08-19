package outboxsql_test

import (
	"reflect"
	"testing"

	"github.com/efureev/go-outbox/pkg/outboxclient"
	"github.com/efureev/go-outbox/pkg/outboxsql"
)

// The two clients carry the same message, defined twice.
//
// Sharing one definition would put this package back in the same module graph
// as pgx, which is the whole thing it exists to avoid — so the types are
// duplicated, and duplication drifts. A field added to one and forgotten in the
// other is not a compile error anywhere: it is a producer on database/sql
// quietly unable to set something a producer on pgx can.
//
// This test is the thing that notices. It lives in the outboxsql tests rather
// than in outboxclient's because pgx is then only in this test binary, not in
// anybody's build.
func TestTheTwoMessagesStayIdentical(t *testing.T) {
	compare(t, "Message",
		reflect.TypeOf(outboxsql.Message{}),
		reflect.TypeOf(outboxclient.Message{}))

	compare(t, "Target",
		reflect.TypeOf(outboxsql.Target{}),
		reflect.TypeOf(outboxclient.Target{}))
}

func compare(t *testing.T, name string, got, want reflect.Type) {
	t.Helper()

	if got.NumField() != want.NumField() {
		t.Fatalf("outboxsql.%s has %d fields, outboxclient.%s has %d",
			name, got.NumField(), name, want.NumField())
	}

	for i := range got.NumField() {
		a, b := got.Field(i), want.Field(i)

		if a.Name != b.Name {
			t.Errorf("%s field %d: outboxsql has %q, outboxclient has %q", name, i, a.Name, b.Name)

			continue
		}
		if a.Tag != b.Tag {
			t.Errorf("%s.%s: tag %q against %q", name, a.Name, a.Tag, b.Tag)
		}

		// Target is the one field whose type differs by construction: each
		// package has its own, and those are compared on their own above.
		if a.Name == "Target" {
			continue
		}
		if a.Type != b.Type {
			t.Errorf("%s.%s: outboxsql has %s, outboxclient has %s", name, a.Name, a.Type, b.Type)
		}
	}
}
