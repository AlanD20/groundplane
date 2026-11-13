package cli

import "github.com/spf13/cobra"

func newRunnerSoftwareUpdateCmd() *cobra.Command {
	var key, version string
	var revision int64
	cmd := &cobra.Command{
		Use:   "update <slug>",
		Short: "Update the upstream GitHub Runner to an exact version after its job completes",
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
			accepted, err := app.Client.UpdateRunnerSoftware(cmd.Context(), runnerID, selected, version, key)
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	}
	cmd.Flags().StringVar(&version, "version", "", "exact upstream GitHub Runner version")
	_ = cmd.MarkFlagRequired("version")
	cmd.Flags().StringVar(&key, "idempotency-key", "", "reuse this key and --revision to resolve uncertain acceptance")
	cmd.Flags().Int64Var(&revision, "revision", 0, "exact desired revision; defaults to the current Runner read")
	return cmd
}

func newRunnerSoftwareResultCmd() *cobra.Command {
	var taskID string
	cmd := &cobra.Command{
		Use:   "update-result <slug>",
		Short: "Show the latest Runner software update or one recorded Task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			runnerID, err := resolveRunnerTarget(cmd, args[0])
			if err != nil {
				return err
			}
			result, err := app.Client.ShowRunnerUpdate(cmd.Context(), runnerID, taskID)
			if err != nil {
				return err
			}
			fields, values := fieldsOfVia(map[string]any{"task_id": result.TaskID, "version": result.Version, "phase": result.Phase, "outcome": result.Outcome, "failure": result.Failure, "stages": result.Stages, "created_at": result.CreatedAt, "finished_at": result.FinishedAt})
			return app.Out.RenderOne(fields, values, result)
		},
	}
	cmd.Flags().StringVar(&taskID, "task", "", "show this Task instead of the latest retained Runner check")
	return cmd
}

func newRunnerSoftwareVersionsCmd() *cobra.Command {
	return &cobra.Command{Use: "versions <slug>", Short: "List official checksummed GitHub Runner releases for this host", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		app := fromContext(cmd)
		id, err := resolveRunnerTarget(cmd, args[0])
		if err != nil {
			return err
		}
		result, err := app.Client.ListRunnerVersions(cmd.Context(), id)
		if err != nil {
			return err
		}
		rows := []map[string]any{}
		for _, item := range result.Items {
			rows = append(rows, map[string]any{"version": item.Version, "architecture": item.Architecture})
		}
		headers, values := tabulateVia(app, rows)
		return app.Out.Render(headers, values, result)
	}}
}
