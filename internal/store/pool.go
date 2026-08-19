package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/efureev/go-outbox/internal/config"
)

// NewPool opens the connection pool and verifies it is usable.
//
// The connection string is assembled by config.DBConfig.ConnString, which escapes
// each component. Concatenating a keyword/value string by hand means a password
// containing a space or a quote produces a DSN that parses into something else
// entirely.
func NewPool(ctx context.Context, cfg config.DBConfig, appName string) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.ConnString())
	if err != nil {
		return nil, fmt.Errorf("parse database DSN: %w", err)
	}

	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns
	poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	poolCfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout

	if poolCfg.ConnConfig.RuntimeParams == nil {
		poolCfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	// application_name makes this service identifiable in pg_stat_activity,
	// which is the first thing anyone looks at when the outbox is blamed for
	// database load.
	poolCfg.ConnConfig.RuntimeParams["application_name"] = appName
	if cfg.StatementTimeout > 0 {
		poolCfg.ConnConfig.RuntimeParams["statement_timeout"] =
			strconv.FormatInt(cfg.StatementTimeout.Milliseconds(), 10)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("open database pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout(cfg.ConnectTimeout))
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()

		return nil, fmt.Errorf("database is unreachable: %w", err)
	}

	return pool, nil
}

func pingTimeout(connect time.Duration) time.Duration {
	if connect > 0 {
		return connect
	}

	return 5 * time.Second
}
