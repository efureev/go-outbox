package observability

import (
	"encoding/json"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/efureev/go-outbox/internal/config"
)

// The shipped dashboard is checked against the metric set rather than against a
// screenshot. A panel querying a metric that was renamed, or filtering on a
// label that never existed, renders an empty graph — which reads as "nothing is
// happening" and is indistinguishable from good news until somebody needs it.

const dashboardPath = "../../dashboards/outbox.json"

// describeAll reports every metric the set registers, with its variable labels,
// by asking each collector to describe itself. Gathering would miss the ones
// with no series yet — broker errors, for instance, exist precisely so they can
// be zero.
func describeAll(t *testing.T, m *Metrics) map[string]map[string]bool {
	t.Helper()

	desc := regexp.MustCompile(`fqName: "([a-z_]+)".*variableLabels: \{([^}]*)\}`)
	out := map[string]map[string]bool{}

	v := reflect.ValueOf(m).Elem()
	for i := range v.NumField() {
		// The set keeps unexported bookkeeping alongside the collectors, and
		// reflection refuses to hand those over.
		if !v.Field(i).CanInterface() {
			continue
		}

		c, ok := v.Field(i).Interface().(prometheus.Collector)
		if !ok || v.Field(i).IsNil() {
			continue
		}

		ch := make(chan *prometheus.Desc, 8)
		go func() {
			c.Describe(ch)
			close(ch)
		}()

		for d := range ch {
			match := desc.FindStringSubmatch(d.String())
			if match == nil {
				t.Fatalf("could not parse a metric description; the format changed: %s", d)
			}

			labels := map[string]bool{}
			for _, l := range strings.Split(match[2], ",") {
				if l = strings.TrimSpace(l); l != "" {
					labels[l] = true
				}
			}
			out[match[1]] = labels
		}
	}

	return out
}

type target struct {
	Expr string `json:"expr"`
}

type dashPanel struct {
	Title   string      `json:"title"`
	Targets []target    `json:"targets"`
	Panels  []dashPanel `json:"panels"` // a collapsed row keeps its panels inside it
}

type dashboard struct {
	Panels []dashPanel `json:"panels"`
}

// queries flattens every expression a panel would send, unescaped by the JSON
// decoder. Matching against the raw file instead would see \" where a query has
// a quote, which is how a label check can silently pass on everything.
func (d dashboard) queries() []string {
	var out []string

	var walk func([]dashPanel)
	walk = func(panels []dashPanel) {
		for _, p := range panels {
			for _, t := range p.Targets {
				if t.Expr != "" {
					out = append(out, t.Expr)
				}
			}
			walk(p.Panels)
		}
	}
	walk(d.Panels)

	return out
}

func loadDashboard(t *testing.T) (dashboard, string) {
	t.Helper()

	raw, err := os.ReadFile(dashboardPath)
	if err != nil {
		t.Fatalf("read the dashboard: %v", err)
	}

	var d dashboard
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatalf("the dashboard is not valid JSON, so Grafana would refuse the import: %v", err)
	}

	return d, string(raw)
}

func metricSet(t *testing.T) *Metrics {
	t.Helper()

	return New(prometheus.NewRegistry(), config.BrokerConfig{
		Streams: map[string]config.StreamConfig{"local": {Driver: "rmq"}},
		Drivers: map[string]config.DriverConfig{"rmq": nil},
	})
}

func TestDashboardQueriesOnlyMetricsThatExist(t *testing.T) {
	known := describeAll(t, metricSet(t))
	d, raw := loadDashboard(t)

	// Histogram queries name the bucket series, which is not a metric of its
	// own; the suffixes are stripped back to what the code registers.
	suffixes := []string{"_bucket", "_sum", "_count"}
	name := regexp.MustCompile(`outbox_[a-z_]+`)

	seen := 0
	for _, ref := range name.FindAllString(raw, -1) {
		base := ref
		for _, s := range suffixes {
			if strings.HasSuffix(base, s) {
				base = strings.TrimSuffix(base, s)

				break
			}
		}
		// Prose in a panel description may name a configuration variable rather
		// than a metric; only the ones that look like metrics are checked.
		if _, ok := known[base]; !ok && strings.HasPrefix(base, "outbox_") {
			if _, isHist := known[base+"_bucket"]; isHist {
				continue
			}
			t.Errorf("the dashboard references %q, which the metric set does not register", ref)
		}
		seen++
	}

	if seen == 0 {
		t.Fatal("no metric references were found; this test is checking nothing")
	}
	if len(d.Panels) == 0 {
		t.Fatal("the dashboard has no panels")
	}
}

// A filter on a label that does not exist matches nothing, so the panel is
// empty and looks like a quiet system.
func TestDashboardFiltersOnLabelsThatExist(t *testing.T) {
	known := describeAll(t, metricSet(t))
	d, _ := loadDashboard(t)

	selector := regexp.MustCompile(`(outbox_[a-z_]+)\{([^}]*)\}`)
	label := regexp.MustCompile(`([a-z_]+)\s*[=!]~?\s*"`)

	checked := 0
	for _, q := range d.queries() {
		for _, m := range selector.FindAllStringSubmatch(q, -1) {
			base := strings.TrimSuffix(m[1], "_bucket")
			labels, ok := known[base]
			if !ok {
				continue // reported by the test above
			}

			for _, l := range label.FindAllStringSubmatch(m[2], -1) {
				checked++
				if !labels[l[1]] {
					t.Errorf("a panel filters %s on %q, which is not one of its labels %v",
						base, l[1], keysOf(labels))
				}
			}
		}
	}

	if checked == 0 {
		t.Fatal("no label filters were examined; this test is checking nothing")
	}
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	return out
}
