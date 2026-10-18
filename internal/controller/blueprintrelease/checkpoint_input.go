package blueprintrelease

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// LoadCheckpointInput uses the assigned input and stored Start receipt, not
// today's Blueprint or Agent-provided candidate bytes. Permission to cross a
// new effect boundary is checked separately in the checkpoint transaction.
func (service *Service) LoadCheckpointInput(ctx context.Context, view workqueue.ResumeView) (releaseexecution.Input, error) {
	if view.Attempt.Attempt.StartedAt == nil || view.Attempt.Attempt.FinishedAt != nil {
		return releaseexecution.Input{}, errs.New(errs.KindStateConflict, "Release checkpoint requires a started unfinished attempt")
	}
	delivery, err := service.loadCapturedAssignment(ctx, view)
	if err != nil {
		return releaseexecution.Input{}, err
	}
	attempt, release, err := workexecution.DecodeReleaseDelivery(delivery, view.Attempt.Attempt.Executor)
	if err != nil {
		return releaseexecution.Input{}, err
	}
	if !workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
		return releaseexecution.Input{}, errs.New(errs.KindStateConflict, "Release checkpoint input changed assignment")
	}
	artifacts, err := executionplan.OpenReleaseWorkArtifacts(view.Attempt.Attempt, release)
	if err != nil {
		return releaseexecution.Input{}, err
	}
	return releaseexecution.Input{Attempt: view.Attempt.Attempt, Release: release, Artifacts: artifacts}, nil
}
