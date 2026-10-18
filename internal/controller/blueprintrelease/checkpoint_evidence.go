package blueprintrelease

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareCheckpointEvidence joins operation authority to the shared cursor
// transaction. New effects require current intent; proof of an already-granted
// effect remains reportable after supersession or Stop. Typed effect/Script
// evidence is independently checked by the checkpoint service in that transaction.
func (service *Service) PrepareCheckpointEvidence(ctx context.Context, view workqueue.ResumeView, input releaseexecution.Input, previous releasework.Snapshot, report releaseexecution.CheckpointReport) (workqueue.Change, error) {
	if service == nil || service.ledger == nil || ctx == nil || !view.ActiveAttempt || !view.CurrentExecutor ||
		!workexecution.SameAssignment(input.Attempt, view.Attempt.Attempt) || input.Attempt.Direction != workexecution.Forward {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Release checkpoint does not own this forward attempt")
	}
	var prior *releaseexecution.Progress
	if previous.Record != nil {
		prior = &previous.Record.Progress
	}
	if err := releaseexecution.ValidateTransition(input, prior, report.Progress); err != nil {
		return workqueue.Change{}, err
	}
	var sources etcd.ReleaseWorkSources
	var err error
	if report.Progress.InFlight {
		sources, err = service.ledger.LoadBlueprintReleaseCheckpointSources(ctx, view)
	} else {
		sources, err = service.ledger.LoadReleaseWorkInput(ctx, view.Work)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(sources.Rollout.Record.CandidateArtifact)
	if !report.Progress.InFlight {
		return workqueue.Change{Conditions: sources.Conditions}, nil
	}
	artifact, err := releaserender.OpenReleaseCandidateArtifact(sources.Rollout.Record)
	if err != nil {
		return workqueue.Change{}, err
	}
	storage, err := service.ledger.PostgresPatchConditions(ctx, artifact, view.Work.Item.Item.TargetID, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: append(sources.Conditions, storage...)}, nil
}
