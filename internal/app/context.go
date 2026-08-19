// Package app is where every binary's dependency wiring happens —
// config load, logging setup, adapter registration, store construction,
// server/scheduler/client assembly. cmd/controller, cmd/agent, and
// cmd/groundplane each import ONLY this package (plus internal/cli, for
// the CLI's command tree) — nothing else, so `go list -deps ./cmd/...`
// is the whole story of what a binary depends on. See
// docs/standards.md, section 1's import matrix.
package app

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// RootContext is the one signal-handling context every binary starts
// from — SIGINT/SIGTERM cancel it, which propagates to in-flight task
// steps, the HTTP server, and the agent's worker pool alike. Identical
// logic in all three cmd/* mains would otherwise be copy-pasted three
// times; this is "logic in one place" applied to process lifecycle.
func RootContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
