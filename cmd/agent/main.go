// cmd/agent is the Agent entry point — thin: everything it does is call
// into internal/app. Per docs/standards.md's import matrix, this
// file imports internal/app and NOTHING else internal. The native Controller
// owns this executable's OCI container lifecycle through Docker; there is no
// Agent systemd unit or bootstrap Compose path. See ADR 0016.
package main

import (
	"fmt"
	"os"

	"github.com/AlanD20/groundplane/internal/app"
)

func main() {
	ctx, stop := app.RootContext()
	defer stop()

	configPath := app.DefaultAgentConfigPath
	if v := os.Getenv("GROUNDPLANE_AGENT_CONFIG"); v != "" {
		configPath = v
	}

	a, err := app.NewAgent(ctx, configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "agent:", err)
		os.Exit(1)
	}

	if err := a.Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "agent:", err)
		os.Exit(1)
	}
}
