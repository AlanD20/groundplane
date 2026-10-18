package scriptwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) SettleCompleted(ctx context.Context, view workqueue.ResumeView, authorization []keyvalue.Condition, at time.Time) error {
	return service.repository.SettleCompletedScriptWork(ctx, view, authorization, at)
}

// Complete acknowledges a saved cleanup cursor, including a failed command.
// A repeated request returns the original attempt result, not a new attempt.
func (service *Service) Complete(ctx context.Context, authorization agentregistration.WorkAuthorization,
	claimed workexecution.Attempt, sequence uint64, evidence workexecution.Input,
) (workexecution.Attempt, error) {
	if service == nil || ctx == nil || claimed.Validate() != nil || sequence == 0 || evidence.Validate() != nil ||
		claimed.Direction != workexecution.Forward || claimed.Executor != authorization.Identity() {
		return workexecution.Attempt{}, errs.New(errs.KindValidationFailed, "Script completion identity is invalid")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return workexecution.Attempt{}, err
	}
	view, err := service.queue.ReadResume(ctx, claimed.TaskID, claimed.WorkItemID, claimed.ID, authorization.Identity())
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if view.Work.Item.Item.Operation != workexecution.Script || !workexecution.SameAssignment(claimed, view.Attempt.Attempt) ||
		view.Checkpoint.Record == nil || view.Checkpoint.Record.Sequence != sequence || view.Checkpoint.Record.Evidence != evidence {
		return workexecution.Attempt{}, errs.New(errs.KindStateConflict, "Script completion differs from its saved checkpoint")
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		if err := service.SettleCompleted(ctx, view, guards, time.Now().UTC()); err != nil {
			return workexecution.Attempt{}, err
		}
		view, err = service.queue.ReadResume(ctx, claimed.TaskID, claimed.WorkItemID, claimed.ID, authorization.Identity())
		if err != nil {
			return workexecution.Attempt{}, err
		}
	}
	attempt := view.Attempt.Attempt
	if !workexecution.SameAssignment(claimed, attempt) || attempt.FinishedAt == nil || attempt.Result == nil || *attempt.Result != evidence {
		return workexecution.Attempt{}, errs.New(errs.KindStateConflict, "Script result does not acknowledge this completion")
	}
	return attempt, nil
}
