// cmd/agent is the Agent entry point — thin: everything it does is call
// into internal/app. Per docs/standards.md's import matrix, this
// file imports only the application layer. The native Controller
// owns this executable's OCI container lifecycle through Docker; there is no
// Agent systemd unit or bootstrap Compose path. See ADR 0016.
package main

import (
	"fmt"
	"os"

	"github.com/AlanD20/groundplane/internal/app"
	"github.com/AlanD20/groundplane/internal/app/hostcommands"
)

func main() {
	ctx, stop := app.RootContext()
	defer stop()
	if len(os.Args) == 2 && os.Args[1] == hostcommands.ResolverWorkHelperArgument {
		if err := hostcommands.RunResolverWorkHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent DNS resolver helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == hostcommands.BackingProvisionHelperArgument {
		if err := hostcommands.RunBackingProvisionHelper(ctx, os.Stdin, os.Stdout, os.Getenv(hostcommands.EnvironmentVolumeRootEnv)); err != nil {
			fmt.Fprintln(os.Stderr, "agent Backing creation helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == hostcommands.EnvironmentProvisionHelperArgument {
		if err := hostcommands.RunEnvironmentProvisionHelper(ctx, os.Stdin, os.Stdout, os.Getenv(hostcommands.EnvironmentVolumeRootEnv)); err != nil {
			fmt.Fprintln(os.Stderr, "agent Environment provisioning helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == hostcommands.EntryWorkHelperArgument {
		if err := hostcommands.RunEntryWorkHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent Entry helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == hostcommands.ComponentWorkHelperArgument {
		if err := hostcommands.RunComponentWorkHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent Component helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == hostcommands.AttachConsumerHelperArgument {
		if err := hostcommands.RunAttachConsumerHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent Detach consumer helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == hostcommands.BackupConsumerHelperArgument {
		if err := hostcommands.RunBackupConsumerHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent Backup Volume consumer helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == hostcommands.NetworkWorkHelperArgument {
		if err := hostcommands.RunNetworkWorkHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent Network helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == hostcommands.VolumeWorkHelperArgument {
		if err := hostcommands.RunVolumeWorkHelper(ctx, os.Stdin, os.Stdout, os.Getenv(hostcommands.EnvironmentVolumeRootEnv)); err != nil {
			fmt.Fprintln(os.Stderr, "agent Volume helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == hostcommands.LifecycleWorkHelperArgument {
		if err := hostcommands.RunLifecycleWorkHelper(ctx, os.Stdin, os.Stdout, os.Getenv(hostcommands.EnvironmentVolumeRootEnv)); err != nil {
			fmt.Fprintln(os.Stderr, "agent Service lifecycle helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == hostcommands.RemovalRecoveryHelperArgument {
		if err := hostcommands.RunRemovalRecoveryHelper(ctx, os.Stdin, os.Stdout, os.Getenv(hostcommands.EnvironmentVolumeRootEnv)); err != nil {
			fmt.Fprintln(os.Stderr, "agent Service removal recovery helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == hostcommands.EdgeConnectionCheckHelperArgument {
		if err := hostcommands.RunEdgeConnectionCheckHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent edge connection helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == hostcommands.BackingAccessHelperArgument {
		if err := hostcommands.RunBackingAccessHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent Backing access helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == hostcommands.EnvironmentRemovalHelperArgument {
		if err := hostcommands.RunEnvironmentRemovalHelper(ctx, os.Stdin, os.Stdout, os.Getenv(hostcommands.EnvironmentVolumeRootEnv)); err != nil {
			fmt.Fprintln(os.Stderr, "agent Environment removal helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == hostcommands.ReleaseHelperArgument {
		if err := hostcommands.RunReleaseHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent Release helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == hostcommands.RecoveryHelperArgument {
		if err := hostcommands.RunRecoveryHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent Release recovery helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == hostcommands.ComposeHelperArgument {
		if err := hostcommands.RunComposeHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent compose helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == hostcommands.ManagedConfigHelperArgument {
		if err := hostcommands.RunManagedConfigHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent managed-config helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 3 && os.Args[1] == hostcommands.HostResolutionHelperArgument {
		if err := hostcommands.RunHostResolutionHelper(ctx, os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, "agent host-resolution helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == hostcommands.EntryMaterializerArgument {
		if err := hostcommands.RunEntryMaterializer(ctx, os.Stdin); err != nil {
			fmt.Fprintln(os.Stderr, "agent entry materializer:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == hostcommands.EntryMaterializationVerificationArgument {
		proven, err := hostcommands.VerifyEntryMaterialization(ctx, os.Stdin)
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
