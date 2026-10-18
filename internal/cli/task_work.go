package cli

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"
)

func newTaskStagesCmd() *cobra.Command {
	var revision int64
	cmd := &cobra.Command{Use: "stages <task-id> <work-id> <attempt-id>", Short: "Inspect named stages, expected results and observations", Args: cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			stages, err := app.Client.TaskStages(cmd.Context(), args[0], args[1], args[2], revision)
			if err != nil {
				return err
			}
			if stages.Expired {
				if _, err := fmt.Fprintln(cmd.ErrOrStderr(), "Stage details expired under diagnostic retention. The recorded attempt outcome and recovery state are unchanged."); err != nil {
					return err
				}
			} else if stages.Incomplete {
				if _, err := fmt.Fprintln(cmd.ErrOrStderr(), "Some execution details could not be saved. Stages may be incomplete; consult the attempt outcome."); err != nil {
					return err
				}
			}
			if !stages.Available && !stages.Expired {
				if _, err := fmt.Fprintln(cmd.ErrOrStderr(), "Stage diagnostics are unavailable; this does not mean no effects occurred."); err != nil {
					return err
				}
			}
			rows := make([][]string, len(stages.Items))
			for index, stage := range stages.Items {
				rows[index] = []string{stage.Name, string(stage.State), stage.Expected, stage.Observed, stage.FailureCode, nullableTaskTimestamp(stage.StartedAt), nullableTaskTimestamp(stage.FinishedAt)}
			}
			return app.Out.Render([]string{"STAGE", "STATE", "EXPECTED", "OBSERVED", "ERROR", "STARTED", "FINISHED"}, rows, stages)
		}}
	cmd.Flags().Int64Var(&revision, "revision", 0, "snapshot revision from execution attempts")
	return cmd
}

func newTaskAttemptsCmd() *cobra.Command {
	var after uint64
	var limit, revision int64
	cmd := &cobra.Command{Use: "attempts <task-id> <work-id>", Short: "Inspect work execution attempts and their host ownership", Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			page, err := app.Client.TaskAttempts(cmd.Context(), args[0], args[1], after, limit, revision)
			if err != nil {
				return err
			}
			rows := make([][]string, len(page.Items))
			for index, attempt := range page.Items {
				rows[index] = []string{attempt.ID, strconv.FormatUint(attempt.Number, 10), string(attempt.Direction),
					string(attempt.Executor), attempt.HostID, string(attempt.Outcome),
					nullableTaskTimestamp(attempt.StartedAt), nullableTaskTimestamp(attempt.FinishedAt)}
			}
			return app.Out.Render([]string{"ID", "ATTEMPT", "DIRECTION", "EXECUTOR", "HOST", "OUTCOME", "STARTED", "FINISHED"}, rows, page)
		}}
	cmd.Flags().Uint64Var(&after, "after", 0, "last attempt number on the previous page")
	cmd.Flags().Int64Var(&limit, "limit", 25, "page size from 1 through 100")
	cmd.Flags().Int64Var(&revision, "revision", 0, "snapshot revision from the previous page")
	return cmd
}

func newTaskImageCmd() *cobra.Command {
	var revision int64
	cmd := &cobra.Command{Use: "image <task-id> <work-id>", Short: "Inspect image fetch or removal evidence", Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			image, err := app.Client.TaskImage(cmd.Context(), args[0], args[1], revision)
			if err != nil {
				return err
			}
			fields, values := fieldsOfVia(map[string]any{"task_id": image.TaskID, "work_item_id": image.WorkItemID,
				"requested": image.Requested, "image_id": image.ImageID, "manifest": image.Manifest,
				"platform": image.Platform, "phase": image.Phase, "outcome": image.Outcome,
				"summary": image.Summary, "progress": image.Progress, "progress_unavailable": image.ProgressUnavailable,
				"uncertain_phase": image.UncertainPhase, "revision": image.Revision})
			return app.Out.RenderOne(fields, values, image)
		}}
	cmd.Flags().Int64Var(&revision, "revision", 0, "snapshot revision from Task details")
	return cmd
}
