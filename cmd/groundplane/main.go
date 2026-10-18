// cmd/groundplane is the CLI entry point — thin: init ctx (via
// internal/app), run the Cobra root (via internal/cli). Per
// docs/standards.md's import matrix, this is the one cmd/* file
// allowed the application layer (for the shared
// signal-handling context) and internal/cli (the command tree itself).
// internal/cli.Execute already routes every error through
// internal/cli/common.HandleErrors and returns a plain exit code, so
// this file never needs to import pkg/errs.
package main

import (
	"os"

	"github.com/AlanD20/groundplane/internal/app"
	"github.com/AlanD20/groundplane/internal/app/hostcommands"
	"github.com/AlanD20/groundplane/internal/cli"
)

func main() {
	ctx, stop := app.RootContext()
	defer stop()
	if len(os.Args) == 2 && os.Args[1] == hostcommands.RunnerConnectionCheckHelperArgument {
		if err := hostcommands.RunRunnerConnectionCheckHelper(ctx, os.Stdin, os.Stdout); err != nil {
			os.Exit(1)
		}
		return
	}

	os.Exit(cli.Execute(ctx, cli.Dependencies{
		RunController:         app.RunController,
		InspectControllerKey:  app.InspectControllerKey,
		InspectControllerEtcd: app.InspectControllerEtcd,
	}))
}
