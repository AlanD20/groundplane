package taskcheckpoint

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *ReleaseCheckpointService) recordForwardRevocation(ctx context.Context,
	view workqueue.ResumeView, authorization []keyvalue.Condition, decision releasework.ForwardCheckpointDecision,
) error {
	result, err := service.queue.Commit(ctx, workqueue.Change{Conditions: authorization}, decision.Change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(
			errs.KindStateConflict,
			"Release supersession changed before recording; reload the same attempt",
		)
	}
	service.reportForwardRevocation(ctx, view.Attempt.Attempt, *decision.RevokedAt)
	return errs.New(
		errs.KindStateConflict,
		"newer configuration superseded this Release; no further forward effect is authorized",
	)
}

// This reports a committed permission decision, not container cleanup. Lost
// diagnostics never turn refusal into an effect grant or authorize replay.
func (service *ReleaseCheckpointService) reportForwardRevocation(ctx context.Context,
	attempt workexecution.Attempt, at time.Time,
) {
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	snapshot, err := service.diagnostics.Read(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, 0)
	if err == nil && snapshot.Expired {
		return
	}
	if err == nil {
		sequence := uint64(1)
		if snapshot.Record != nil {
			sequence = snapshot.Record.Sequence + 1
		}
		stage := workexecution.Stage{Key: "forward_superseded", Name: "Stop superseded deployment",
			State: workexecution.StageSucceeded, StartedAt: &at, FinishedAt: &at,
			Expected: "Newer configuration prevents any further forward step",
			Observed: "Forward permission revoked; existing effects remain protected for captured cleanup"}
		err = service.diagnostics.Report(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID,
			attempt.Executor, sequence, stage, at)
	}
	if err != nil {
		service.logger.WarnContext(
			bounded,
			"Release supersession diagnostics incomplete",
			"attempt_id",
			attempt.ID,
			"error",
			err,
		)
		if markErr := service.diagnostics.MarkIncomplete(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor); markErr != nil {
			service.logger.WarnContext(
				bounded,
				"could not record Release diagnostic loss",
				"attempt_id",
				attempt.ID,
				"error",
				markErr,
			)
		}
	}
}
