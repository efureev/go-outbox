package store

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/efureev/go-outbox/internal/config"
)

// NewPool opens the connection pool and verifies it is usable.
//
// The DSN is either given whole or assembled through net/url, which escapes
// each component. Concatenating a keyword/value string by hand means a password
// containing a space or a quote produces a DSN that parses into something else
// entirely.
func NewPool(ctx context.Context, cfg config.DBConfig, appName string) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn(cfg))
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

func dsn(cfg config.DBConfig) string {
	if cfg.DSN != "" {
		return cfg.DSN
	}

	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(cfg.User, cfg.Password),
		Host:   net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		Path:   "/" + cfg.Name,
	}

	q := url.Values{}
	if cfg.SSLMode != "" {
		q.Set("sslmode", cfg.SSLMode)
	}
	u.RawQuery = q.Encode()

	return u.String()
}
