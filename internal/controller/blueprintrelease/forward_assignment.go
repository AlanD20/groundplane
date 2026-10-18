package blueprintrelease

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareForwardAssignment owns only initial forward admission. Recovery must
// use its retained checkpoints and captured input, not these current-head checks.
func (service *Service) PrepareForwardAssignment(ctx context.Context,
	request workqueue.Assignment, at time.Time,
) (workdispatch.Decision, error) {
	task, item := request.Task.Task, request.Item.Item
	if service == nil || service.ledger == nil || request.DecisionOnly || request.Direction != workexecution.Forward ||
		item.Placement.Executor != workexecution.Agent || request.Executor.Record.Identity.HostID != item.Placement.HostID ||
		at.IsZero() || !at.Before(task.Deadline) || !at.Before(item.Deadline) {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Blueprint Release cannot obtain this forward assignment")
	}
	view := workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}
	sources, err := service.ledger.LoadReleaseForwardSources(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	defer clear(sources.Rollout.Record.CandidateArtifact)
	artifact, err := releaserender.OpenReleaseCandidateArtifact(sources.Rollout.Record)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	storage, err := service.ledger.PostgresPatchConditions(ctx, artifact, item.TargetID, request.ReadRevision)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	conditions := append(sources.Conditions, storage...)
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: item.Deadline,
		Change: workqueue.Change{Conditions: conditions}}, nil
}
