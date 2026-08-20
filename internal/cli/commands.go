package cli

import "github.com/spf13/cobra"

// addCommands registers every top-level noun — flat, never nested (see
// api-cli.md, "Flat nouns (locked)"). This is the single place the tree
// is assembled; each noun's own file only builds its own subtree. `dns`
// is deliberately absent because DNS settings live under `component
// config coredns --platform`. `router` remains the locked read-only
// projection noun; mutations stay on `component`.
func addCommands(root *cobra.Command, deps Dependencies) {
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
		newComponentCmd(),
		newRouterCmd(),
		newBackingServiceCmd(),
		newSecretCmd(),
		newConnectorCmd(),
		newRunnerCmd(),
		newAgentCmd(),
		newTaskCmd(),
		newActivityCmd(),
		newHostCmd(),
		withExecutionClass(newControllerCmd(deps), executionLocal),
		withExecutionClass(newAgentRunCmd(), executionLocal),
		withExecutionClass(newVersionCmd(), executionTool),
		withExecutionClass(newCompletionCmd(root), executionTool),
	)
}
