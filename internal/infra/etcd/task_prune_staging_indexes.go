package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *TaskRepository) backupStagingPruneIndexes(
	ctx context.Context,
	task TaskRecord,
	sealed *agentpb.ExecutionPlan,
	revision int64,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	keys := make([]string, 0, len(sealed.Steps))
	for _, step := range sealed.Steps {
		pointID := backupStagingStepPoint(step.GetBackupStep())
		if pointID == "" {
			continue
		}
		digest, err := executionplan.BackupStagingRecoveryKey(task.ID, step.StepId, pointID)
		if err != nil {
			return nil, nil, err
		}
		keys = append(keys, backupruntime.BackupStagingIndexKey(digest))
	}
	if len(keys) == 0 {
		return nil, nil, nil
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, nil, backupStagingConflict()
	}
	defer etcdstore.ClearValues(read.Values)
	conditions := make([]etcdstore.Condition, 0, len(keys))
	mutations := make([]etcdstore.Mutation, 0, len(keys))
	for index, value := range read.Values {
		if value == nil {
			if task.TerminalAssignment != nil {
				return nil, nil, backupStagingConflict()
			}
			conditions = append(conditions, etcdstore.Condition{Key: keys[index]})
			continue
		}
		if value.Version != 1 || value.ModRevision <= 0 || task.TerminalAssignment == nil {
			return nil, nil, backupStagingConflict()
		}
		record, err := backupruntime.DecodeBackupStagingIndex(value.Value)
		if err != nil || record.TaskID != task.ID || record.PlanSHA256 != task.PlanHash ||
			record.AgentID != task.TerminalAssignment.AgentID ||
			record.AgentGeneration != task.TerminalAssignment.AgentGeneration {
			return nil, nil, backupStagingConflict()
		}
		digest, err := record.RecoveryKey()
		if err != nil || backupruntime.BackupStagingIndexKey(digest) != keys[index] {
			return nil, nil, backupStagingConflict()
		}
		conditions = append(conditions, etcdstore.Condition{Key: keys[index], ModRevision: value.ModRevision})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: keys[index]})
	}
	return conditions, mutations, nil
}
