// Package logging implements the ONE unified logging setup every binary
// calls once at startup — Controller, Agent, and CLI alike. See
// architecture.md, "Unified logging (locked)".
//
// Rules encoded here:
//   - standard log/slog, no third-party logging framework.
//   - level priority: --debug > --verbose > GROUNDPLANE_LOG_LEVEL env >
//     config-file log.level > default WARN.
//   - two destinations, both always configured for daemons: a stderr text
//     handler at the configured level, and a rotating file handler ALWAYS
//     at DEBUG (10MB x 3 backups) — a production incident never needs a
//     rerun with --debug.
//   - the CLI is console-only (nothing to persist).
//   - never log secret values.
package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/natefinch/lumberjack.v2"
)

// ConsoleConfig configures the stderr text handler.
type ConsoleConfig struct {
	Enabled bool
	NoColor bool
}

// FileConfig configures the rotating file handler. Always DEBUG level,
// per the lock — the level field here does not exist on purpose.
type FileConfig struct {
	Enabled    bool
	Path       string
	MaxSizeMB  int
	MaxBackups int
}

// Options is everything Setup needs. Build it from resolved config +
// flags; ResolveLevel implements the priority order above.
type Options struct {
	Level   slog.Level
	Console ConsoleConfig
	File    FileConfig
}

// ResolveLevel implements the locked priority order:
// --debug > --verbose > GROUNDPLANE_LOG_LEVEL env > config-file log.level > WARN.
func ResolveLevel(debug, verbose bool, envLevel, configLevel string) slog.Level {
	switch {
	case debug:
		return slog.LevelDebug
	case verbose:
		return slog.LevelInfo
	case envLevel != "":
		return parseLevel(envLevel)
	case configLevel != "":
		return parseLevel(configLevel)
	default:
		return slog.LevelWarn
	}
}

func parseLevel(s string) slog.Level {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "DEBUG":
		return slog.LevelDebug
	case "INFO":
		return slog.LevelInfo
	case "WARN", "WARNING":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelWarn
	}
}

// Setup configures the process-wide default slog.Logger per Options and
// returns it. Call once, at each entry point (cmd/groundplane,
// cmd/controller, cmd/agent).
func Setup(opts Options) (*slog.Logger, error) {
	var handlers []slog.Handler

	if opts.Console.Enabled {
		handlers = append(handlers, slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: opts.Level,
		}))
	}

	if opts.File.Enabled {
		if opts.File.Path == "" {
			return nil, fmt.Errorf("logging: file.enabled is true but file.path is empty")
		}
		if err := os.MkdirAll(filepath.Dir(opts.File.Path), 0o755); err != nil {
			return nil, fmt.Errorf("logging: create log dir: %w", err)
		}
		lj := &lumberjack.Logger{
			Filename:   opts.File.Path,
			MaxSize:    orDefault(opts.File.MaxSizeMB, 10),
			MaxBackups: orDefault(opts.File.MaxBackups, 3),
			Compress:   false,
		}
		// Always DEBUG — locked. A production incident never needs a rerun
		// with --debug because the file already has everything.
		handlers = append(handlers, slog.NewTextHandler(lj, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
	}

	var handler slog.Handler
	switch len(handlers) {
	case 0:
		handler = slog.NewTextHandler(io.Discard, nil) // both destinations disabled — quiet boot
	case 1:
		handler = handlers[0]
	default:
		handler = &multiHandler{handlers: handlers}
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger, nil
}

func orDefault(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

// multiHandler fans a record out to every configured destination — this
// is what makes "file AND stdout/stderr, both always available" a single
// Setup() call instead of two loggers to keep in sync.
type multiHandler struct {
	handlers []slog.Handler
}

func (m *multiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, h := range m.handlers {
		if h.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (m *multiHandler) Handle(ctx context.Context, r slog.Record) error {
	var firstErr error
	for _, h := range m.handlers {
		if !h.Enabled(ctx, r.Level) {
			continue
		}
		if err := h.Handle(ctx, r.Clone()); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (m *multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		next[i] = h.WithAttrs(attrs)
	}
	return &multiHandler{handlers: next}
}

func (m *multiHandler) WithGroup(name string) slog.Handler {
	next := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		next[i] = h.WithGroup(name)
	}
	return &multiHandler{handlers: next}
}
