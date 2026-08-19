package cli

import "github.com/spf13/cobra"

// addCommands registers every top-level noun — flat, never nested (see
// api-cli.md, "Flat nouns (locked)"). This is the single place the tree
// is assembled; each noun's own file only builds its own subtree. `dns`
// and `router` are deliberately absent — DNS settings live under `core
// component coredns config` now, and the Router is a read-only
// projection surfaced via `environment show`/the Console, replaced as a
// CLI verb by `addon enable|disable|config` (see api-cli.md's command
// tree — both nouns were dropped from the locked tree).
func addCommands(root *cobra.Command) {
	root.AddCommand(
		newTenantCmd(),
		newProjectCmd(),
		newEnvironmentCmd(),
		newServiceCmd(),
		newZoneCmd(),
		newRouteCmd(),
		newVolumeCmd(),
		newEntryCmd(),
		newScriptCmd(),
		newReleaseGroupCmd(),
		newBackupCmd(),
		newAddonCmd(),
		newBackingServiceCmd(),
		newSecretCmd(),
		newConnectorCmd(),
		newRunnerCmd(),
		newAgentCmd(),
		newTaskCmd(),
		newActivityCmd(),
		newHostCmd(),
		newCoreCmd(),
		newControllerCmd(),
		newAgentRunCmd(),
		newVersionCmd(),
	)
}
