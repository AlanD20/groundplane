// cmd/controller is the Controller entry point — thin: everything it
// does is call into internal/app. Per docs/standards.md's import
// matrix, this file imports internal/app and NOTHING else internal.
package main

import (
	"fmt"
	"os"

	"github.com/AlanD20/groundplane/internal/app"
)

func main() {
	ctx, stop := app.RootContext()
	defer stop()

	if err := app.RunController(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "controller:", err)
		os.Exit(1)
	}
}
