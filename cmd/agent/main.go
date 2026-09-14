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
	if len(os.Args) == 2 && os.Args[1] == app.ComposeHelperArgument {
		if err := app.RunComposeHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent compose helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == app.EnvironmentDirectoryHelperArgument {
		if err := app.RunEnvironmentDirectoryHelper(
			ctx,
			os.Stdin,
			os.Stdout,
			os.Getenv(app.EnvironmentVolumeRootEnv),
		); err != nil {
			fmt.Fprintln(os.Stderr, "agent Environment directory helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == app.ManagedConfigHelperArgument {
		if err := app.RunManagedConfigHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent managed-config helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 3 && os.Args[1] == app.HostResolutionHelperArgument {
		if err := app.RunHostResolutionHelper(ctx, os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, "agent host-resolution helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == app.EntryMaterializerArgument {
		if err := app.RunEntryMaterializer(ctx, os.Stdin); err != nil {
			fmt.Fprintln(os.Stderr, "agent entry materializer:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == app.EntryMaterializationVerificationArgument {
		proven, err := app.VerifyEntryMaterialization(ctx, os.Stdin)
		if err != nil {
			fmt.Fprintln(os.Stderr, "agent materialization verification:", err)
			os.Exit(1)
		}
		if !proven {
			os.Exit(2)
		}
		return
	}
	if len(os.Args) != 1 {
		fmt.Fprintln(os.Stderr, "agent: unsupported execution mode")
		os.Exit(1)
	}

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
