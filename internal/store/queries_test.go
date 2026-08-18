package store

import (
	"reflect"
	"strings"
	"testing"
)

// Every query has to be rendered. A field left out of the struct literal is a
// silent empty string, and an empty query reaches the database as a confusing
// "unused argument" rather than as anything that names the missing statement —
// which is exactly how it was found the first time.
func TestNewQueriesRendersEveryStatement(t *testing.T) {
	q := newQueries(`"outbox"`, `"outbox"."messages"`)

	v := reflect.ValueOf(q)
	for i := range v.NumField() {
		name := v.Type().Field(i).Name
		sql := v.Field(i).String()

		if strings.TrimSpace(sql) == "" {
			t.Errorf("query %q is empty: it was left out of the struct literal", name)

			continue
		}
		if strings.Contains(sql, "%!") {
			t.Errorf("query %q has a formatting error: %s", name, sql)
		}
		if strings.Contains(sql, "%[1]s") || strings.Contains(sql, "%s") {
			t.Errorf("query %q still holds an unsubstituted verb: %s", name, sql)
		}
	}
}

// The ownership predicate is the whole reason several replicas are safe. A
// write-back that loses it silently reintroduces the defect that let an expired
// lease overwrite another instance's result.
func TestFinalizingQueriesRequireTheLeaseToken(t *testing.T) {
	q := newQueries(`"outbox"`, `"outbox"."messages"`)

	for name, sql := range map[string]string{
		"ack":          q.ack,
		"nack":         q.nack,
		"releaseLease": q.releaseLease,
	} {
		if !strings.Contains(sql, "lease_token = $") {
			t.Errorf("query %q does not check lease ownership:\n%s", name, sql)
		}
	}
}

// Claiming must skip rows another replica already holds, or two instances
// publish the same message in the same poll cycle.
func TestClaimSkipsLockedRows(t *testing.T) {
	q := newQueries(`"outbox"`, `"outbox"."messages"`)

	for name, sql := range map[string]string{"claim": q.claim, "reclaim": q.reclaim, "purge": q.purge} {
		if !strings.Contains(sql, "FOR UPDATE SKIP LOCKED") {
			t.Errorf("query %q does not use FOR UPDATE SKIP LOCKED:\n%s", name, sql)
		}
	}
}

// Counting delivered rows means scanning them, and there are far more of them
// than of anything else.
func TestStatsNeverCountsDeliveredRows(t *testing.T) {
	q := newQueries(`"outbox"`, `"outbox"."messages"`)

	if strings.Contains(q.stats, "status = 2") {
		t.Errorf("the stats query touches delivered rows:\n%s", q.stats)
	}
	if strings.Contains(q.stats, "GROUP BY") {
		t.Errorf("the stats query groups over the table instead of using the partial indexes:\n%s", q.stats)
	}
}
