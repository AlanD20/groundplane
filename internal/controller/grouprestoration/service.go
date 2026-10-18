// Package grouprestoration owns captured switch-back of earlier successful
// Group members. It is not ordinary previous-image Rollback or forward Deploy.
package grouprestoration

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Service struct {
	ledger    *etcd.ReleaseLedger
	groups    *releasegroups.ExecutionRepository
	queue     *workqueue.Repository
	artifacts *taskplanning.ScriptArtifactService
}

func New(ledger *etcd.ReleaseLedger, groups *releasegroups.ExecutionRepository,
	queue *workqueue.Repository, artifacts *taskplanning.ScriptArtifactService,
) (*Service, error) {
	if ledger == nil || groups == nil || queue == nil || artifacts == nil {
		return nil, errs.New(errs.KindInternal, "Group restoration dependencies are not configured")
	}
	return &Service{ledger: ledger, groups: groups, queue: queue, artifacts: artifacts}, nil
}

func (service *Service) Prepare(ctx context.Context, taskID, workID string) error {
	view, err := service.queue.ReadWork(ctx, taskID, workID, 0)
	if err != nil {
		return err
	}
	at := time.Now().UTC()
	decision, err := service.groups.Decide(ctx, view.Task, view.ReadRevision, at)
	if err != nil {
		return err
	}
	if decision.Decision.WorkID != workID {
		return nil
	}
	var change workqueue.Change
	switch decision.Decision.Action {
	case domain.GroupWorkSkip:
		change, err = service.groups.PrepareSkip(view.Task, decision, at)
	case domain.GroupWorkRestore:
		change, err = service.groups.PrepareRestorationInput(ctx, view, at)
	default:
		return nil
	}
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(change.Mutations)
	result, err := service.queue.Commit(ctx, change)
	keyvalue.ClearValues(result.FailureReads)
	return err
}

// LoadRecoveryInput uses the actual restoration attempt and the genuinely
// successful source receipt. No fake failed source or active assignment is made.
func (service *Service) LoadRecoveryInput(ctx context.Context, view workqueue.ResumeView) (releaseexecution.RecoveryInput, error) {
	if !view.ActiveAttempt || view.ExecutorRevision <= 0 || workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil ||
		view.Work.Item.Item.Operation != workexecution.ReleaseRestoration {
		return releaseexecution.RecoveryInput{}, errs.New(errs.KindStateConflict, "Group restoration input requires its retained active assignment")
	}
	sources, err := service.ledger.LoadGroupRestorationSources(ctx, view.Work)
	if err != nil {
		return releaseexecution.RecoveryInput{}, err
	}
	defer sources.Clear()
	payload, err := blueprintrelease.BuildCapturedReleaseInput(sources.Release, view.Attempt.Attempt)
	if err != nil {
		return releaseexecution.RecoveryInput{}, err
	}
	artifacts, err := executionplan.OpenReleaseWorkArtifacts(view.Attempt.Attempt, payload)
	if err != nil {
		return releaseexecution.RecoveryInput{}, err
	}
	stored := sources.Restoration.Input
	input := releaseexecution.RecoveryInput{Input: releaseexecution.Input{Attempt: view.Attempt.Attempt, Release: payload, Artifacts: artifacts},
		Forward: stored.Source, Group: releaseexecution.CloneGroupRestoration(&stored.Authority)}
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		if view.Attempt.Attempt.Number < 2 {
			return releaseexecution.RecoveryInput{}, errs.New(errs.KindStateConflict, "Group restoration Retry has no previous attempt")
		}
		previous, err := service.groups.LoadRestorationFailure(ctx, view.Work, view.Attempt.Attempt.Number-1)
		if err != nil {
			return releaseexecution.RecoveryInput{}, err
		}
		input.Interruption = releaseexecution.CloneRestorationInterruption(&previous.Interruption)
	}
	return input, input.Validate()
}
