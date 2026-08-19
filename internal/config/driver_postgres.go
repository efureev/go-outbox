package config

import (
	"fmt"
	"strings"
	"time"
)

// Defaults for a table destination. They differ from a broker's because the
// failure they guard against is different: a slow INSERT is a loaded database,
// not a lost connection, so the timeout is generous rather than tight, and the
// pool is small because delivery is a handful of statements per batch and not a
// workload of its own.
const (
	defaultPostgresWriteTimeout = 15 * time.Second
	defaultPostgresMaxConns     = 4
	maxPostgresConns            = 64
)

// PostgresDriver configures delivery into a table — the consumer's inbox.
//
// The dispatcher only ever inserts. It does not create the table, does not read
// it, and does not clean it up: the table belongs to whoever consumes from it,
// the same way a JetStream stream belongs to whoever owns the cluster.
type PostgresDriver struct {
	name   string
	naming Naming

	// DSN is where the inbox lives. Empty means the database the dispatcher
	// already reads its outbox from, which is the modular-monolith case and the
	// only shape where delivery and write-back could ever share a transaction.
	DSN string
	// SameDatabase records that DSN was defaulted rather than given. It is not
	// configuration but a conclusion, and it exists so the driver can say so in
	// its logs and in /api/v1/stats instead of leaving an operator to compare
	// two connection strings by eye.
	SameDatabase bool

	Schema string
	Table  string

	WriteTimeout time.Duration
	MaxConns     int32
}

func (d *PostgresDriver) Name() string   { return d.name }
func (*PostgresDriver) Type() DriverType { return DriverPostgres }
func (d *PostgresDriver) Naming() Naming { return d.naming }

// Endpoint reports where the rows go, with credentials removed and the target
// table named: two drivers pointed at the same database but different tables
// are otherwise indistinguishable in the stats response.
func (d *PostgresDriver) Endpoint() string {
	return fmt.Sprintf("%s#%s.%s", redactDSN(d.DSN), d.Schema, d.Table)
}

// Qualified is the target table, quoted. Both identifiers are validated at
// load time, which is what makes interpolating them into SQL safe — a
// placeholder cannot stand in for an identifier.
func (d *PostgresDriver) Qualified() string {
	return fmt.Sprintf("%q.%q", d.Schema, d.Table)
}

func buildPostgresDriver(name string, naming Naming, get func(string) string, db DBConfig) (*PostgresDriver, error) {
	dsn := get("DSN")
	sameDatabase := dsn == ""
	if sameDatabase {
		dsn = db.ConnString()
	}
	if dsn == "" {
		return nil, fmt.Errorf("driver %q: no DSN, and the dispatcher's own database is not configured either", name)
	}

	schema := get("SCHEMA")
	if schema == "" {
		return nil, fmt.Errorf("driver %q: SCHEMA is not set", name)
	}
	table := get("TABLE")
	if table == "" {
		return nil, fmt.Errorf("driver %q: TABLE is not set", name)
	}

	// The same rule the dispatcher applies to its own schema and table. An
	// identifier that cannot be quoted safely is a configuration error, not
	// something to escape at run time.
	if !identifier.MatchString(schema) {
		return nil, fmt.Errorf("driver %q: SCHEMA %q is not a valid lower-case unquoted identifier", name, schema)
	}
	if !identifier.MatchString(table) {
		return nil, fmt.Errorf("driver %q: TABLE %q is not a valid lower-case unquoted identifier", name, table)
	}

	// Delivering into the outbox table itself would be a loop: every message
	// published would be a new message to publish.
	if sameDatabase && schema == db.Schema && table == db.Table {
		return nil, fmt.Errorf(
			"driver %q: the destination is the dispatcher's own outbox table (%s.%s), which would deliver to itself",
			name, schema, table)
	}

	writeTimeout, err := durationOrDefault(get("WRITE_TIMEOUT"), defaultPostgresWriteTimeout)
	if err != nil {
		return nil, fmt.Errorf("driver %q: WRITE_TIMEOUT: %w", name, err)
	}
	if writeTimeout <= 0 {
		return nil, fmt.Errorf("driver %q: WRITE_TIMEOUT must be positive, got %s", name, writeTimeout)
	}

	maxConns, err := intOrDefault(get("MAX_CONNS"), defaultPostgresMaxConns)
	if err != nil {
		return nil, fmt.Errorf("driver %q: MAX_CONNS: %w", name, err)
	}
	if maxConns < 1 || maxConns > maxPostgresConns {
		return nil, fmt.Errorf("driver %q: MAX_CONNS must be between 1 and %d, got %d",
			name, maxPostgresConns, maxConns)
	}

	return &PostgresDriver{
		name:         name,
		naming:       naming,
		DSN:          strings.TrimSpace(dsn),
		SameDatabase: sameDatabase,
		Schema:       schema,
		Table:        table,
		WriteTimeout: writeTimeout,
		MaxConns:     int32(maxConns),
	}, nil
}
