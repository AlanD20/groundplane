package cli

import (
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/spf13/cobra"
)

func newRunnerScopedMigrationCmd() *cobra.Command {
	command := &cobra.Command{
		Use:   "scoped-migration",
		Short: "Inspect or migrate a registered Runner to scoped access",
	}
	command.AddCommand(newRunnerScopedMigrationShowCmd(), newRunnerScopedMigrationStartCmd())
	return command
}

func newRunnerScopedMigrationShowCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "show <slug>",
		Short: "Show the Runner migration phase and revision",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runnerID, err := resolveRunnerTarget(cmd, args[0])
			if err != nil {
				return err
			}
			app := fromContext(cmd)
			status, err := app.Client.ShowRunnerScopedMigration(cmd.Context(), runnerID)
			if err != nil {
				return err
			}
			fields := map[string]any{
				"runner_id": status.RunnerID, "revision": status.Revision, "available": status.Available, "phase": status.Phase,
			}
			if status.TaskID != "" {
				fields["task_id"] = status.TaskID
			}
			if status.AcceptedAt != nil {
				fields["accepted_at"] = *status.AcceptedAt
			}
			if status.CompletedAt != nil {
				fields["completed_at"] = *status.CompletedAt
			}
			headers, values := fieldsOfVia(fields)
			return app.Out.RenderOne(headers, values, status)
		},
	}
}

func newRunnerScopedMigrationStartCmd() *cobra.Command {
	var input apiTypes.RunnerScopedMigrationRequest
	var revision int64
	var key string
	command := &cobra.Command{
		Use:   "start <slug>",
		Short: "Migrate the Runner to scoped access after its current job completes",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runnerID, err := resolveRunnerTarget(cmd, args[0])
			if err != nil {
				return err
			}
			accepted, err := fromContext(
				cmd,
			).Client.StartRunnerScopedMigration(
				cmd.Context(),
				runnerID,
				revision,
				input,
				key,
			)
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	}
	command.Flags().StringVar(&input.ConnectionID, "connection-id", "", "authorized GitHub connection ID")
	command.Flags().Int64Var(&input.RepositoryID, "repository-id", 0, "GitHub repository ID that can use this Runner")
	command.Flags().Int64Var(&revision, "revision", 0, "exact revision from runner scoped-migration show")
	command.Flags().
		StringVar(&key, "idempotency-key", "", "reuse this key with the same revision to resolve uncertain acceptance")
	for _, name := range []string{"connection-id", "repository-id", "revision"} {
		_ = command.MarkFlagRequired(name)
	}
	return command
}
