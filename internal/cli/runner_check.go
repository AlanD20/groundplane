package cli

import "github.com/spf13/cobra"

func newRunnerCheckCmd() *cobra.Command {
	var key string
	var revision int64
	cmd := &cobra.Command{
		Use:   "check <slug>",
		Short: "Check GitHub, rootless Docker, GP API and registry from an existing Runner",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			runnerID, err := resolveRunnerTarget(cmd, args[0])
			if err != nil {
				return err
			}
			selected := revision
			if selected == 0 {
				runner, err := app.Client.ShowRunner(cmd.Context(), runnerID)
				if err != nil {
					return err
				}
				selected = runner.Revision
			}
			accepted, err := app.Client.CheckRunner(cmd.Context(), runnerID, selected, key)
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	}
	cmd.Flags().StringVar(&key, "idempotency-key", "", "reuse this key and --revision to resolve uncertain acceptance")
	cmd.Flags().Int64Var(&revision, "revision", 0, "exact desired revision; defaults to the current Runner read")
	return cmd
}

func newRunnerCheckResultCmd() *cobra.Command {
	var taskID string
	cmd := &cobra.Command{
		Use:   "check-result <slug>",
		Short: "Show the latest Runner diagnostic or one recorded Task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			runnerID, err := resolveRunnerTarget(cmd, args[0])
			if err != nil {
				return err
			}
			result, err := app.Client.ShowRunnerCheck(cmd.Context(), runnerID, taskID)
			if err != nil {
				return err
			}
			fields, values := fieldsOfVia(map[string]any{"task_id": result.TaskID, "phase": result.Phase,
				"outcome": result.Outcome, "failure": result.Failure, "stale": result.Stale,
				"execution_location": result.ExecutionLocation, "configuration": result.Configuration, "stages": result.Stages,
				"created_at": result.CreatedAt, "started_at": result.StartedAt, "finished_at": result.FinishedAt})
			return app.Out.RenderOne(fields, values, result)
		},
	}
	cmd.Flags().StringVar(&taskID, "task", "", "show this Task instead of the latest retained Runner check")
	return cmd
}
