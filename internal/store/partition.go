package store

import (
	"context"
	"fmt"
	"time"
)

// Partitioning is what a deployment past roughly ten million rows a day needs
// and what everybody else should leave alone.
//
// At that volume the retention sweep stops keeping up: a chunked DELETE creates
// dead tuples faster than autovacuum reclaims them, so the table keeps growing
// while apparently being cleaned. Range partitioning by created_at turns the
// same work into DROP TABLE — a catalog change and an unlink, whatever the row
// count.
//
// It is opt-in by construction. Nothing here creates a partitioned table: an
// operator does that deliberately with migrations/partitioned/messages.sql
// before the ordinary migrations run, and the dispatcher notices afterwards.
// Everything below is a no-op against an ordinary table, and the queries the
// dispatcher runs are the same either way — partitioning is transparent to DML.

// IsPartitioned reports whether the outbox table is range-partitioned.
//
// Asked once at startup rather than per sweep: converting a table to
// partitioned is a deliberate migration with the process stopped, not something
// that happens underneath a running dispatcher.
func (s *Store) IsPartitioned(ctx context.Context) (bool, error) {
	var partitioned bool

	err := s.pool.QueryRow(ctx,
		`SELECT relkind = 'p' FROM pg_class WHERE oid = $1::regclass`, s.table).Scan(&partitioned)
	if err != nil {
		return false, fmt.Errorf("check whether %s is partitioned: %w", s.table, err)
	}

	return partitioned, nil
}

// EnsurePartitions creates the daily partitions covering today and the next
// `ahead` days, and reports the ones it had to create.
//
// Running ahead is not tidiness. A row that fits no partition is a failed
// INSERT, and that INSERT is inside the producer's business transaction — so a
// missing partition does not delay a message, it rolls back whatever the
// application was doing. The default partition shipped with the schema is the
// second line of defence for the same reason.
func (s *Store) EnsurePartitions(ctx context.Context, ahead int) ([]string, error) {
	if ahead < 0 {
		ahead = 0
	}

	// The database's date, not this process's: the partition bounds are
	// evaluated by the server, and a dispatcher in another timezone must not
	// decide which day it is.
	var today time.Time
	if err := s.pool.QueryRow(ctx, `SELECT current_date`).Scan(&today); err != nil {
		return nil, fmt.Errorf("read the database date: %w", err)
	}

	var created []string

	for i := 0; i <= ahead; i++ {
		day := today.AddDate(0, 0, i)
		name := s.partitionName(day)

		// IF NOT EXISTS rather than a catalogue lookup first: several replicas
		// reach this at the same moment, and the janitor's advisory lock covers
		// the sweep, not the startup path that also calls this.
		_, err := s.pool.Exec(ctx, fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')`,
			name, s.table, day.Format(time.DateOnly), day.AddDate(0, 0, 1).Format(time.DateOnly)))
		if err != nil {
			return created, fmt.Errorf("create partition %s: %w", name, err)
		}

		created = append(created, name)
	}

	return created, nil
}

// Partition is one child table of the outbox.
type Partition struct {
	// Name is already schema-qualified and quoted, as the catalogue renders it.
	Name string
	// Default marks the catch-all partition, which is never dropped: it is what
	// keeps a producer's INSERT from failing when a daily partition is missing.
	Default bool
}

// Partitions lists the children of the outbox table.
func (s *Store) Partitions(ctx context.Context) ([]Partition, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT c.oid::regclass::text, pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT'
		  FROM pg_inherits i
		  JOIN pg_class c ON c.oid = i.inhrelid
		 WHERE i.inhparent = $1::regclass
		 ORDER BY 1`, s.table)
	if err != nil {
		return nil, fmt.Errorf("list partitions of %s: %w", s.table, err)
	}
	defer rows.Close()

	var out []Partition
	for rows.Next() {
		var p Partition
		if err := rows.Scan(&p.Name, &p.Default); err != nil {
			return nil, fmt.Errorf("scan partition: %w", err)
		}
		out = append(out, p)
	}

	return out, rows.Err()
}

// DropExpiredPartitions removes the partitions that retention has finished
// with, and reports the ones it dropped.
//
// A partition is only dropped when every row in it has been delivered and the
// most recent delivery is older than the retention window. Both halves matter,
// and neither can be inferred from the partition's own bounds: those are on
// created_at while retention is on dispatched_at, so a partition full of
// messages written last week may still hold one that failed and is waiting for
// somebody to look at it. Dropping by age alone would delete it.
func (s *Store) DropExpiredPartitions(ctx context.Context, retention time.Duration) ([]string, error) {
	if retention <= 0 {
		return nil, nil
	}

	parts, err := s.Partitions(ctx)
	if err != nil {
		return nil, err
	}

	var dropped []string

	for _, p := range parts {
		if p.Default {
			continue
		}

		expired, err := s.partitionExpired(ctx, p.Name, retention)
		if err != nil {
			return dropped, err
		}
		if !expired {
			continue
		}

		// DROP takes a brief exclusive lock on the parent. It is a catalogue
		// change and an unlink, not a scan, so the pause is the same whether the
		// partition holds a thousand rows or a hundred million — which is the
		// whole reason for partitioning in the first place.
		if _, err := s.pool.Exec(ctx, `DROP TABLE IF EXISTS `+p.Name); err != nil {
			return dropped, fmt.Errorf("drop partition %s: %w", p.Name, err)
		}

		dropped = append(dropped, p.Name)
	}

	return dropped, nil
}

func (s *Store) partitionExpired(ctx context.Context, name string, retention time.Duration) (bool, error) {
	var expired bool

	err := s.pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT NOT EXISTS (SELECT 1 FROM %[1]s WHERE status <> 2)
		   AND coalesce(max(dispatched_at), '-infinity'::timestamptz)
		       < now() - make_interval(secs => $1)
		  FROM %[1]s`, name), retention.Seconds()).Scan(&expired)
	if err != nil {
		return false, fmt.Errorf("check whether partition %s is spent: %w", name, err)
	}

	return expired, nil
}

// DefaultPartitionRows counts what landed in the catch-all partition.
//
// It should be zero. Anything else means a row arrived for a day nobody had
// created a partition for, and while the default partition kept the producer's
// transaction from failing, those rows now block creating the proper partition
// for their range until they are moved or deleted.
func (s *Store) DefaultPartitionRows(ctx context.Context) (int64, error) {
	parts, err := s.Partitions(ctx)
	if err != nil {
		return 0, err
	}

	for _, p := range parts {
		if !p.Default {
			continue
		}

		var n int64
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM `+p.Name).Scan(&n); err != nil {
			return 0, fmt.Errorf("count rows in %s: %w", p.Name, err)
		}

		return n, nil
	}

	return 0, nil
}

// partitionName is the child table for one day, schema-qualified and quoted.
// The naming is a convention rather than a lookup, so a partition created by
// hand with a different name is still adopted by the parent and still dropped
// when it expires — only the creation path assumes it.
func (s *Store) partitionName(day time.Time) string {
	return fmt.Sprintf("%s.%q", s.schema, s.tableName+"_"+day.Format("20060102"))
}
