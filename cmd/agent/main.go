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
	if len(os.Args) == 2 && os.Args[1] == app.BackingProvisionHelperArgument {
		if err := app.RunBackingProvisionHelper(ctx, os.Stdin, os.Stdout, os.Getenv(app.EnvironmentVolumeRootEnv)); err != nil {
			fmt.Fprintln(os.Stderr, "agent Backing creation helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == app.EnvironmentProvisionHelperArgument {
		if err := app.RunEnvironmentProvisionHelper(ctx, os.Stdin, os.Stdout, os.Getenv(app.EnvironmentVolumeRootEnv)); err != nil {
			fmt.Fprintln(os.Stderr, "agent Environment provisioning helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == app.EntryWorkHelperArgument {
		if err := app.RunEntryWorkHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent Entry helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == app.ComponentWorkHelperArgument {
		if err := app.RunComponentWorkHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent Component helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == app.AttachConsumerHelperArgument {
		if err := app.RunAttachConsumerHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent Detach consumer helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == app.BackupConsumerHelperArgument {
		if err := app.RunBackupConsumerHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent Backup Volume consumer helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == app.NetworkWorkHelperArgument {
		if err := app.RunNetworkWorkHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent Network helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == app.VolumeWorkHelperArgument {
		if err := app.RunVolumeWorkHelper(ctx, os.Stdin, os.Stdout, os.Getenv(app.EnvironmentVolumeRootEnv)); err != nil {
			fmt.Fprintln(os.Stderr, "agent Volume helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == app.LifecycleWorkHelperArgument {
		if err := app.RunLifecycleWorkHelper(ctx, os.Stdin, os.Stdout, os.Getenv(app.EnvironmentVolumeRootEnv)); err != nil {
			fmt.Fprintln(os.Stderr, "agent Service lifecycle helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == app.RemovalRecoveryHelperArgument {
		if err := app.RunRemovalRecoveryHelper(ctx, os.Stdin, os.Stdout, os.Getenv(app.EnvironmentVolumeRootEnv)); err != nil {
			fmt.Fprintln(os.Stderr, "agent Service removal recovery helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == app.EdgeConnectionCheckHelperArgument {
		if err := app.RunEdgeConnectionCheckHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent edge connection helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == app.BackingAccessHelperArgument {
		if err := app.RunBackingAccessHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent Backing access helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == app.EnvironmentRemovalHelperArgument {
		if err := app.RunEnvironmentRemovalHelper(ctx, os.Stdin, os.Stdout, os.Getenv(app.EnvironmentVolumeRootEnv)); err != nil {
			fmt.Fprintln(os.Stderr, "agent Environment removal helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == app.ReleaseHelperArgument {
		if err := app.RunReleaseHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent Release helper:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == app.RecoveryHelperArgument {
		if err := app.RunRecoveryHelper(ctx, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent Release recovery helper:", err)
			os.Exit(1)
		}
		return
	}
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
