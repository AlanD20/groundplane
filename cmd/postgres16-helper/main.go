package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/AlanD20/groundplane/internal/postgres16helper"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// The helper protocol intentionally exposes no diagnostics: its stable exit
	// code is the sole process result, so the typed in-process error is suppressed.
	code, _ := postgres16helper.Main(ctx, os.Args[1:])
	os.Exit(int(code))
}
