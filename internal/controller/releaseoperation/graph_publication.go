package releaseoperation

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type GraphPublication struct {
	ledger   *etcd.ReleaseLedger
	queue    *workqueue.Repository
	networks *networkwork.InputRepository
	groups   *releasegroups.ExecutionRepository
}

func NewGraphPublication(ledger *etcd.ReleaseLedger, queue *workqueue.Repository, networks *networkwork.InputRepository, groups *releasegroups.ExecutionRepository) (*GraphPublication, error) {
	if ledger == nil || queue == nil || networks == nil || groups == nil {
		return nil, errs.New(errs.KindInternal, "Direct Release graph publication dependencies are not configured")
	}
	return &GraphPublication{ledger: ledger, queue: queue, networks: networks, groups: groups}, nil
}

func (owner *GraphPublication) PreparePublication(ctx context.Context, task workqueue.TaskSnapshot,
	revision int64, at time.Time,
) (workdispatch.PublicationBatch, error) {
	if ctx == nil || task.Revision <= 0 || revision < task.Revision || task.Task.PublicationComplete ||
		task.Task.FinishedAt != nil || at.Before(task.Task.UpdatedAt) {
		return workdispatch.PublicationBatch{}, errs.New(errs.KindStateConflict, "Direct Release graph publication is no longer open")
	}
	if ids.Validate(ids.KindReleaseGroup, task.Task.TargetID) == nil {
		return owner.prepareGroupPublication(ctx, task, revision, at)
	}
	request, guard, err := owner.ledger.ReadDirectReleaseTaskPreparation(ctx, task, revision)
	if err != nil {
		return workdispatch.PublicationBatch{}, err
	}
	work, err := owner.queue.ReadWork(ctx, task.Task.ID, request.Configuration.WorkItemID, revision)
	if err != nil {
		return workdispatch.PublicationBatch{}, err
	}
	rollout, err := owner.ledger.GetServiceRollout(ctx, work)
	if err != nil {
		return workdispatch.PublicationBatch{}, err
	}
	defer clear(rollout.Record.CandidateArtifact)
	if err := request.Matches(task.Task, work.Item.Item, rollout.Record); err != nil {
		return workdispatch.PublicationBatch{}, err
	}
	if work.Item.Item.Input != nil || work.Item.Item.Phase != workexecution.Waiting || work.Item.Item.AttemptNumber != 0 {
		return workdispatch.PublicationBatch{}, errs.New(errs.KindStateConflict, "Unpublished Release graph contains executable or executed Service work")
	}
	graph, err := request.WorkGraph(task.Task, work.Item.Item)
	if err != nil {
		return workdispatch.PublicationBatch{}, err
	}
	guards := []keyvalue.Condition{guard,
		{Key: workqueue.TaskKey(task.Task.ID), ModRevision: task.Revision},
		{Key: workqueue.ItemKey(work.Item.Item.ID), ModRevision: work.Item.Revision}}
	staged := workqueue.Change{Conditions: guards}
	defer keyvalue.ZeroMutationBytes(staged.Mutations)
	prepared := 0
	for index, network := range request.Networks {
		input, err := network.Open(task.Task.Scope.EnvironmentID)
		if err != nil {
			return workdispatch.PublicationBatch{}, err
		}
		part, ready, err := owner.networks.PrepareReleaseEnsure(ctx, task, graph[index], input,
			rollout.Record.Render.Projection.RevisionID, guards, revision)
		if err != nil {
			return workdispatch.PublicationBatch{}, err
		}
		if ready {
			continue
		}
		staged.Conditions = append(staged.Conditions, part.Conditions...)
		staged.Mutations = append(staged.Mutations, part.Mutations...)
		prepared++
		if prepared == 4 {
			break
		}
	}
	if prepared != 0 {
		// Every new immutable input root advances this Task revision. The later
		// publication-barrier comparison therefore fences all prepared inputs,
		// without placing hundreds of comparisons in a single transaction.
		next := workexecution.CloneTask(task.Task)
		next.UpdatedAt = at
		value, err := workqueue.EncodeTask(next)
		if err != nil {
			return workdispatch.PublicationBatch{}, err
		}
		staged.Mutations = append(staged.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
			Key: workqueue.TaskKey(task.Task.ID), Value: value})
		result, err := owner.queue.Commit(ctx, staged)
		keyvalue.ClearValues(result.FailureReads)
		if err != nil {
			return workdispatch.PublicationBatch{}, err
		}
		if !result.Succeeded {
			return workdispatch.PublicationBatch{}, errs.New(errs.KindStateConflict, "Direct Release graph preparation ownership changed")
		}
		return workdispatch.PublicationBatch{Planning: true}, nil
	}
	return workdispatch.PublicationBatch{Graph: graph, Authority: workqueue.Change{Conditions: guards}}, nil
}
