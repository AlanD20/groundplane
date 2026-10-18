package blueprintrelease

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (service *Service) LoadRecoveryInput(ctx context.Context, view workqueue.ResumeView) (releaseexecution.RecoveryInput, error) {
	origin, err := service.ledger.LoadBlueprintReleaseRecoveryOrigin(ctx, view)
	if err != nil {
		return releaseexecution.RecoveryInput{}, err
	}
	delivery, err := service.loadCapturedAssignment(ctx, view)
	if err != nil {
		return releaseexecution.RecoveryInput{}, err
	}
	_, release, err := workexecution.DecodeReleaseDelivery(delivery, view.Attempt.Attempt.Executor)
	if err != nil {
		return releaseexecution.RecoveryInput{}, err
	}
	artifacts, err := executionplan.OpenReleaseWorkArtifacts(view.Attempt.Attempt, release)
	if err != nil {
		return releaseexecution.RecoveryInput{}, err
	}
	input := releaseexecution.RecoveryInput{Input: releaseexecution.Input{Attempt: view.Attempt.Attempt, Release: release, Artifacts: artifacts}, Forward: origin.Forward, Failure: origin.Failure}
	if err := input.Validate(); err != nil {
		return releaseexecution.RecoveryInput{}, err
	}
	return input, nil
}
