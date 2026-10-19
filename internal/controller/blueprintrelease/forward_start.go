package blueprintrelease

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareForwardStart is the forward branch of Release start authorization.
// Recovery uses captured restoration authority, never current desired-head
// comparisons. The dispatcher commits these comparisons with the start receipt.
func (service *Service) PrepareForwardStart(ctx context.Context, view workqueue.ResumeView) (workqueue.Change, error) {
	if service == nil || service.ledger == nil {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Blueprint Release start is not configured")
	}
	sources, err := service.ledger.LoadReleaseStartSources(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(sources.Rollout.Record.CandidateArtifact)
	artifact, err := releaserender.OpenReleaseCandidateArtifact(sources.Rollout.Record)
	if err != nil {
		return workqueue.Change{}, err
	}
	storage, err := service.ledger.PostgresPatchConditions(
		ctx,
		artifact,
		view.Work.Item.Item.TargetID,
		view.Work.ReadRevision,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: append(sources.Conditions, storage...)}, nil
}
