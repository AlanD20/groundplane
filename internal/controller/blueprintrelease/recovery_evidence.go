package blueprintrelease

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) PrepareRecoveryEvidence(ctx context.Context, view workqueue.ResumeView, input releaseexecution.RecoveryInput, previous releasework.RecoverySnapshot, report releaseexecution.RecoveryReport) (workqueue.Change, error) {
	if service == nil || service.ledger == nil || ctx == nil || !view.ActiveAttempt || !view.CurrentExecutor ||
		!workexecution.SameAssignment(input.Input.Attempt, view.Attempt.Attempt) || input.Validate() != nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint recovery checkpoint does not own this cleanup attempt")
	}
	var prior *releaseexecution.RecoveryProgress
	if previous.Record != nil {
		prior = &previous.Record.Report.Progress
	}
	if err := releaseexecution.ValidateRecoveryTransition(input, prior, report.Progress); err != nil {
		return workqueue.Change{}, err
	}
	// Cleanup is bound to captured work, not current desired intent; superseding
	// the forward action must not revoke permission to restore its saved runtime.
	sources, err := service.ledger.LoadBlueprintReleaseWorkInput(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(sources.Rollout.Record.CandidateArtifact)
	change := workqueue.Change{Conditions: sources.Conditions}
	if !report.Progress.InFlight {
		proof, err := service.ledger.RecoveryEffectConditions(ctx, view, input, previous, report)
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, proof.Conditions...)
	}
	return change, nil
}
