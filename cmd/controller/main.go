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

	configPath := app.DefaultControllerConfigPath
	if v := os.Getenv("GROUNDPLANE_CONTROLLER_CONFIG"); v != "" {
		configPath = v
	}

	c, err := app.NewController(ctx, configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "controller:", err)
		os.Exit(1)
	}

	if err := c.Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "controller:", err)
		os.Exit(1)
	}
}
