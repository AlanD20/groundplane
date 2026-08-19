// cmd/agent is the Agent entry point — thin: everything it does is call
// into internal/app. Per docs/standards.md's import matrix, this
// file imports internal/app and NOTHING else internal. Seeded once by
// groundplane-agent.service (docker plus the bootstrap compose file, no
// logic). See mvp.md, "Agent (execution plane)".
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
