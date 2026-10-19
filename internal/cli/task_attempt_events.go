package cli

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func newTaskAttemptEventsCmd() *cobra.Command {
	var after uint64
	var revision int64
	var follow bool
	cmd := &cobra.Command{
		Use:   "attempt-events <task-id> <work-id> <attempt-id>",
		Short: "Read retained execution events for an attempt",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			if follow && revision != 0 {
				return errs.New(errs.KindValidationFailed, "follow cannot use a fixed historical revision")
			}
			app := fromContext(cmd)
			cursor := after
			first, warned := true, false
			for {
				events, err := app.Client.TaskAttemptEvents(cmd.Context(), args[0], args[1], args[2], cursor, revision)
				if err != nil {
					return err
				}
				if events.Expired || events.First > 1 && cursor == 0 {
					if _, err := fmt.Fprintln(cmd.ErrOrStderr(), "Earlier events are no longer retained; this is not a complete execution history."); err != nil {
						return err
					}
				}
				if events.Incomplete && !warned {
					if _, err := fmt.Fprintln(cmd.ErrOrStderr(), "Some diagnostics could not be recorded; consult the attempt outcome."); err != nil {
						return err
					}
					warned = true
				}
				rows := make([][]string, len(events.Items))
				for index, event := range events.Items {
					rows[index] = []string{
						strconv.FormatUint(event.Sequence, 10),
						event.RecordedAt.Format(time.RFC3339Nano),
						event.Stage.Name,
						string(event.Stage.State),
						event.Stage.Observed,
						event.Stage.FailureCode,
					}
				}
				if first || len(events.Items) > 0 || events.FinishedAt != nil {
					if err := app.Out.Render([]string{"SEQUENCE", "RECORDED", "STAGE", "STATE", "OBSERVED", "ERROR"}, rows, events); err != nil {
						return err
					}
				}
				first = false
				if !follow || events.FinishedAt != nil {
					if follow && events.FinishedAt != nil {
						_, err := fmt.Fprintf(cmd.ErrOrStderr(), "Attempt finished: %s\n", events.Outcome)
						return err
					}
					return nil
				}
				cursor = events.Last
				if err := waitAttemptEvents(cmd.Context()); err != nil {
					return err
				}
			}
		},
	}
	cmd.Flags().BoolVar(&follow, "follow", false, "follow this attempt until its recorded completion")
	cmd.Flags().Uint64Var(&after, "after", 0, "last event sequence already read for this attempt")
	cmd.Flags().Int64Var(&revision, "revision", 0, "fixed snapshot revision; zero reads current retained events")
	return cmd
}

func waitAttemptEvents(ctx context.Context) error {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
