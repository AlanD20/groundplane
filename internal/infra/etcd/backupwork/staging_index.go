package backupwork

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type stagingSourceIndex struct {
	TaskID      string              `json:"task_id"`
	WorkItemID  string              `json:"work_item_id"`
	HostID      string              `json:"host_id"`
	Input       workexecution.Input `json:"input"`
	Ordinal     uint32              `json:"ordinal"`
	ExecutionID string              `json:"execution_id"`
	PointID     string              `json:"point_id"`
}

func sourceStagingIndexKey(digest []byte) string {
	return "/v1/runtime/backup-work-staging/" + hex.EncodeToString(digest)
}

func prepareSourceStagingIndexes(item workexecution.Item, input *agentpb.BackupWorkInput) (workqueue.Change, error) {
	var change workqueue.Change
	seen := make(map[string]struct{})
	for ordinal, step := range input.Steps {
		point := sourcePoint(step)
		if point == "" {
			continue
		} // Prune has no source-file staging.
		digest, err := executionplan.BackupSourceStagingKey(item.TaskID, step.ExecutionId, point)
		if err != nil {
			return workqueue.Change{}, err
		}
		key := sourceStagingIndexKey(digest)
		if _, exists := seen[key]; exists {
			return workqueue.Change{}, conflict("Backup sources share a staging namespace")
		}
		seen[key] = struct{}{}
		encoded, err := recordcodec.Encode("backup-work-staging", stagingSourceIndex{
			TaskID: item.TaskID, WorkItemID: item.ID, HostID: item.Placement.HostID, Input: *item.Input,
			Ordinal: uint32(ordinal), ExecutionID: step.ExecutionId, PointID: point,
		})
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: encoded},
		)
	}
	return change, nil
}

type StagingSource struct {
	View       workqueue.View
	Input      *agentpb.BackupWorkInput
	Sources    []*agentpb.BackupStepResume
	Ordinal    uint32
	Conditions []keyvalue.Condition
}

// ReadStagingSource resolves a private startup namespace without scanning Task
// history or substituting current desired state for captured input.
func (repository *InputRepository) ReadStagingSource(ctx context.Context, queue *workqueue.Repository,
	identity workexecution.Identity, digest []byte,
) (StagingSource, error) {
	var zero StagingSource
	if queue == nil || identity.Validate() != nil || identity.Executor != workexecution.Agent || len(digest) != 32 {
		return zero, conflict("Backup source recovery requires an authenticated host and source key")
	}
	key := sourceStagingIndexKey(digest)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}})
	if err != nil {
		return zero, err
	}
	if read == nil {
		return zero, conflict("Backup staging index snapshot is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision <= 0 || len(read.Values) != 1 || read.Values[0] == nil || read.Values[0].Version != 1 ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > read.ReadRevision {
		return zero, conflict("Backup source staging index is missing or changed")
	}
	index, err := recordcodec.Decode[stagingSourceIndex](read.Values[0].Value, "backup-work-staging")
	if err != nil {
		return zero, err
	}
	expected, err := executionplan.BackupSourceStagingKey(index.TaskID, index.ExecutionID, index.PointID)
	if err != nil || !bytes.Equal(expected, digest) || index.HostID != identity.HostID {
		return zero, conflict("Backup source staging belongs to another input or host")
	}
	view, err := queue.ReadWork(ctx, index.TaskID, index.WorkItemID, read.ReadRevision)
	if err != nil {
		return zero, err
	}
	if view.Item.Item.Input == nil || *view.Item.Item.Input != index.Input ||
		view.Item.Item.Placement.HostID != identity.HostID {
		return zero, conflict("Backup source staging differs from its retained work")
	}
	input, inputGuard, err := repository.Load(ctx, view)
	if err != nil {
		return zero, err
	}
	if uint64(index.Ordinal) >= uint64(len(input.Steps)) {
		return zero, conflict("Backup source ordinal is invalid")
	}
	step := input.Steps[index.Ordinal]
	if step.ExecutionId != index.ExecutionID || sourcePoint(step) != index.PointID {
		return zero, conflict("Backup source differs from its captured staging namespace")
	}
	sources, progressGuard, err := repository.LoadProgress(ctx, view, input)
	if err != nil {
		return zero, err
	}
	return StagingSource{View: view, Input: input, Sources: sources, Ordinal: index.Ordinal,
		Conditions: []keyvalue.Condition{inputGuard, progressGuard,
			{Key: key, ModRevision: read.Values[0].ModRevision},
			{Key: workqueue.TaskKey(index.TaskID), ModRevision: view.Task.Revision},
			{Key: workqueue.ItemKey(index.WorkItemID), ModRevision: view.Item.Revision},
		}}, nil
}

func sourcePoint(step *agentpb.BackupStepAuthority) string {
	if step.GetCapture() != nil {
		return step.GetCapture().PointId
	}
	return step.GetRestore().GetPointId()
}
