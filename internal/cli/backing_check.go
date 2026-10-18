package cli

import (
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func newAttachCheckCmd() *cobra.Command {
	var grant string
	cmd := &cobra.Command{
		Use:   "check <attach>",
		Short: "Check Backing access from the deployed consumer's network",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			environmentID, err := resolveEnvironmentTarget(cmd, fromContext(cmd).Scope.Environment)
			if err != nil {
				return err
			}
			attachID, err := resolveAttachTarget(cmd, args[0])
			if err != nil {
				return err
			}
			grantID := ""
			if grant != "" {
				grantID, err = resolveAttachTarget(cmd, grant)
				if err != nil {
					return err
				}
			}
			accepted, err := fromContext(cmd).Client.CheckBackingAccess(cmd.Context(), environmentID, attachID, grantID)
			if err != nil {
				return err
			}
			fields, values := fieldsOfVia(map[string]any{"task_id": accepted.TaskID})
			return fromContext(cmd).Out.RenderOne(fields, values, accepted)
		},
	}
	cmd.Flags().StringVar(&grant, "grant", "", "check a database granted to this consumer's credential")
	return cmd
}

func newAttachCheckResultCmd() *cobra.Command {
	var latest, grant string
	cmd := &cobra.Command{
		Use:   "check-result [task-id]",
		Short: "Show access stages, captured configuration and helper cleanup",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if (len(args) == 0) != (latest != "") || latest == "" && grant != "" {
				return errs.New(
					errs.KindValidationFailed,
					"check-result requires a Task id or --latest Attach with an optional --grant",
				)
			}
			environmentID, err := resolveEnvironmentTarget(cmd, fromContext(cmd).Scope.Environment)
			if err != nil {
				return err
			}
			taskID, attachID, grantID := "", "", ""
			if latest != "" {
				attachID, err = resolveAttachTarget(cmd, latest)
				if err != nil {
					return err
				}
				if grant != "" {
					grantID, err = resolveAttachTarget(cmd, grant)
					if err != nil {
						return err
					}
				}
			} else {
				taskID = args[0]
			}
			result, err := fromContext(
				cmd,
			).Client.ShowBackingCheck(
				cmd.Context(),
				environmentID,
				taskID,
				attachID,
				grantID,
			)
			if err != nil {
				return err
			}
			fields, values := fieldsOfVia(
				map[string]any{"task_id": result.TaskID, "phase": result.Phase, "outcome": result.Outcome,
					"failure": result.Failure, "stale": result.Stale, "configuration": result.Configuration, "stages": result.Stages,
					"execution_location": result.ExecutionLocation, "cleanup_required": result.CleanupRequired,
					"helper_name": result.HelperName, "created_at": result.CreatedAt,
					"started_at": result.StartedAt, "finished_at": result.FinishedAt},
			)
			return fromContext(cmd).Out.RenderOne(fields, values, result)
		},
	}
	cmd.Flags().StringVar(&latest, "latest", "", "show the latest retained check for this Attach")
	cmd.Flags().StringVar(&grant, "grant", "", "select the granted database for --latest")
	return cmd
}
