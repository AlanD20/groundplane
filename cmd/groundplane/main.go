// cmd/groundplane is the CLI entry point — thin: init ctx (via
// internal/app), run the Cobra root (via internal/cli). Per
// docs/standards.md's import matrix, this is the one cmd/* file
// allowed two internal imports: internal/app (for the shared
// signal-handling context) and internal/cli (the command tree itself).
// internal/cli.Execute already routes every error through
// internal/cli/common.HandleErrors and returns a plain exit code, so
// this file never needs to import pkg/errs.
package main

import (
	"os"

	"github.com/AlanD20/groundplane/internal/app"
	"github.com/AlanD20/groundplane/internal/cli"
)

func main() {
	ctx, stop := app.RootContext()
	defer stop()

	os.Exit(cli.Execute(ctx, cli.Dependencies{
		RunController: app.RunController,
	}))
}
