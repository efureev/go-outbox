package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"
)

// serveOptions configures one listener.
type serveOptions struct {
	name            string
	port            int
	handler         http.Handler
	readTimeout     time.Duration
	writeTimeout    time.Duration
	shutdownTimeout time.Duration
	log             *slog.Logger
}

// serve binds the port, starts the server in the background and returns a
// function that stops it.
//
// The listener is bound synchronously so that a port already in use fails the
// module's start rather than surfacing later as a log line nobody reads —
// which is what happens when a server is started in a bare goroutine.
func serve(ctx context.Context, opts serveOptions) (stop func(context.Context) error, err error) {
	addr := net.JoinHostPort("", strconv.Itoa(opts.port))

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s for %s: %w", addr, opts.name, err)
	}

	srv := &http.Server{
		Handler:           opts.handler,
		ReadHeaderTimeout: opts.readTimeout,
		ReadTimeout:       opts.readTimeout,
		WriteTimeout:      opts.writeTimeout,
	}

	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			opts.log.Error("http server stopped", slog.String("server", opts.name), slog.Any("error", err))
		}
	}()

	opts.log.Info("http server listening", slog.String("server", opts.name), slog.Int("port", opts.port))

	return func(ctx context.Context) error {
		shutdownCtx, cancel := context.WithTimeout(ctx, opts.shutdownTimeout)
		defer cancel()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shut down %s server: %w", opts.name, err)
		}

		return nil
	}, nil
}

// writeJSON writes v as a JSON response with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)

	if err := encodeJSON(w, v); err != nil {
		// The status line is already on the wire, so there is nothing to do
		// but stop writing; the client sees a truncated body and a connection
		// close, which is the honest signal.
		return
	}
}
