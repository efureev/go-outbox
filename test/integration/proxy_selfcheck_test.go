//go:build integration

package integration

import "testing"

// The proxy is test infrastructure, so it is checked before anything is
// concluded from it: a resilience test that passes because the fault never
// happened is worse than no test.
func TestProxyBreaksAndHeals(t *testing.T) {
	// PostgreSQL is a convenient target: it answers a TCP connect and holds it.
	p := newProxy(t, hostOf(t, dsn()))

	if err := dialThrough(p.Addr()); err != nil {
		t.Fatalf("a healthy proxy refused a connection: %v", err)
	}

	p.Break()

	if err := dialThrough(p.Addr()); err == nil {
		t.Error("a broken proxy still relayed a connection")
	}

	p.Heal()

	if err := dialThrough(p.Addr()); err != nil {
		t.Errorf("a healed proxy refuses connections: %v", err)
	}
}
