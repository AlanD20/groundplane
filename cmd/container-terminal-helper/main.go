package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/AlanD20/groundplane/internal/containerterminalhelper"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	code, _ := containerterminalhelper.Run(ctx, os.Stdin, os.Stdout)
	os.Exit(code)
}
