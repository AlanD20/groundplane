package releaseoperation

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type GroupPreflightPreparation struct {
	ledger  *etcd.ReleaseLedger
	scripts *etcd.ScriptRepository
	queue   *workqueue.Repository
	groups  *releasegroups.ExecutionRepository
}

func NewGroupPreflightPreparation(ledger *etcd.ReleaseLedger, scripts *etcd.ScriptRepository,
	queue *workqueue.Repository, groups *releasegroups.ExecutionRepository,
) (*GroupPreflightPreparation, error) {
	if ledger == nil || scripts == nil || queue == nil || groups == nil {
		return nil, errs.New(errs.KindInternal, "Group preflight preparation dependencies are not configured")
	}
	return &GroupPreflightPreparation{ledger: ledger, scripts: scripts, queue: queue, groups: groups}, nil
}

func (owner *GroupPreflightPreparation) Prepare(ctx context.Context, taskID, workID string) error {
	view, err := owner.queue.ReadWork(ctx, taskID, workID, 0)
	if err != nil {
		return err
	}
	root, err := owner.groups.Load(ctx, view.Task, view.ReadRevision)
	if err != nil {
		return err
	}
	index := slices.IndexFunc(
		root.Record.Preflight,
		func(hook releasegroups.PreflightHook) bool { return hook.WorkItemID == workID },
	)
	if index < 0 {
		return errs.New(errs.KindStateConflict, "Script has no captured Group preflight membership")
	}
	hook := root.Record.Preflight[index]
	memberID := root.Record.Policy.Members[hook.Ordinal-1].ForwardID
	member, err := owner.queue.ReadWork(ctx, taskID, memberID, view.ReadRevision)
	if err != nil {
		return err
	}
	request, _, err := owner.ledger.ReadDirectReleasePreparation(ctx, member)
	if err != nil {
		return err
	}
	current, guards, err := owner.ledger.MatchDirectReleaseSource(ctx, member, request)
	if err != nil {
		return err
	}
	at := time.Now().UTC()
	var change workqueue.Change
	if !current {
		change, err = workqueue.PrepareInputSupersession(view, guards, at)
	} else {
		change, err = owner.scripts.PrepareGroupPreflightWork(ctx, view, at)
		if err == nil {
			change.Conditions = append(change.Conditions, guards...)
		}
	}
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(change.Mutations)
	result, err := owner.queue.Commit(ctx, change)
	keyvalue.ClearValues(result.FailureReads)
	return err
}
