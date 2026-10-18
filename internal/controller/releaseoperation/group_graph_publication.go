package releaseoperation

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *GraphPublication) prepareGroupPublication(ctx context.Context, task workqueue.TaskSnapshot,
	revision int64, at time.Time,
) (workdispatch.PublicationBatch, error) {
	root, err := owner.groups.Load(ctx, task, revision)
	if err != nil {
		return workdispatch.PublicationBatch{}, err
	}
	guards := []keyvalue.Condition{{Key: workqueue.TaskKey(task.Task.ID), ModRevision: task.Revision},
		{Key: releasegroups.ExecutionCaptureKey(task.Task.ID), ModRevision: root.Revision}}
	staged := workqueue.Change{Conditions: guards}
	defer func() { keyvalue.ZeroMutationBytes(staged.Mutations) }()
	prepared := 0
	for index := range root.Record.Members {
		ready, part, err := owner.ledger.PrepareGroupReleaseMemberPublication(ctx, task, root, index, revision)
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
		return owner.commitGroupPreparation(ctx, task, staged, at)
	}
	networks := make(map[string]etcd.DirectReleaseNetwork)
	projectionID := ""
	for index := range root.Record.Members {
		member, err := owner.ledger.ReadCapturedGroupMember(ctx, task, root, index, revision)
		if err != nil {
			return workdispatch.PublicationBatch{}, err
		}
		currentProjection := member.Rollout.Render.Projection.RevisionID
		clear(member.Rollout.CandidateArtifact)
		if projectionID != "" && projectionID != currentProjection {
			return workdispatch.PublicationBatch{}, errs.New(errs.KindStateConflict, "Group members captured different Environment projections")
		}
		projectionID = currentProjection
		for _, network := range member.Preparation.Networks {
			if previous, found := networks[network.NetworkID]; found &&
				(previous.WorkItemID != network.WorkItemID || previous.Input != network.Input || !bytes.Equal(previous.Data, network.Data)) {
				return workdispatch.PublicationBatch{}, errs.New(errs.KindStateConflict, "Group members captured different definitions for a shared DNS bridge")
			}
			networks[network.NetworkID] = network
		}
	}
	for _, item := range root.Record.Graph {
		if item.Operation != workexecution.Network {
			continue
		}
		network, found := networks[item.TargetID]
		if !found || item.ID != network.WorkItemID || item.Input == nil || *item.Input != network.Input {
			return workdispatch.PublicationBatch{}, errs.New(errs.KindStateConflict, "Group DNS prerequisite differs from its sealed member sources")
		}
		input, err := network.Open(task.Task.Scope.EnvironmentID)
		if err != nil {
			return workdispatch.PublicationBatch{}, err
		}
		part, ready, err := owner.networks.PrepareReleaseEnsure(ctx, task, item, input, projectionID, guards, revision)
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
		return owner.commitGroupPreparation(ctx, task, staged, at)
	}
	return workdispatch.PublicationBatch{Graph: root.Record.Graph, Authority: workqueue.Change{Conditions: guards}}, nil
}

func (owner *GraphPublication) commitGroupPreparation(ctx context.Context, task workqueue.TaskSnapshot,
	staged workqueue.Change, at time.Time,
) (workdispatch.PublicationBatch, error) {
	next := workexecution.CloneTask(task.Task)
	next.UpdatedAt = at
	value, err := workqueue.EncodeTask(next)
	if err != nil {
		return workdispatch.PublicationBatch{}, err
	}
	defer clear(value)
	staged.Mutations = append(staged.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: workqueue.TaskKey(task.Task.ID), Value: value})
	result, err := owner.queue.Commit(ctx, staged)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return workdispatch.PublicationBatch{}, err
	}
	if !result.Succeeded {
		return workdispatch.PublicationBatch{}, errs.New(errs.KindStateConflict, "Group preparation ownership changed before publication")
	}
	return workdispatch.PublicationBatch{Planning: true}, nil
}
