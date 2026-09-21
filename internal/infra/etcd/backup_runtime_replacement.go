package etcd

import (
	"bytes"
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupRuntimeRepository) replaceBackupRun(
	ctx context.Context,
	current etcdstore.Versioned[backupruntime.BackupRunRecord],
	next backupruntime.BackupRunRecord,
	extraConditions []etcdstore.Condition,
	extraMutations []etcdstore.Mutation,
	validateExtra func([]*etcdstore.KeyValue) error,
	authority *backupruntime.BackupAssignmentInput,
	checkpoint *backupruntime.BackupCheckpointInput,
) (etcdstore.Versioned[backupruntime.BackupRunRecord], error) {
	if current.Revision <= 0 || current.ReadRevision < current.Revision {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup run version is invalid",
		)
	}
	value, err := backupruntime.EncodeBackupRunRecord(next)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	defer clear(value)
	anchorKeys := make([]string, 1, len(extraConditions)+1)
	anchorKeys[0] = backupruntime.BackupRunKey(current.Record.TaskID)
	for _, condition := range extraConditions {
		anchorKeys = append(anchorKeys, condition.Key)
	}
	anchor, err := repository.ReadCurrentKeys(ctx, anchorKeys)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	defer etcdstore.ClearValues(anchor.Values)
	if anchor.Values[0] == nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindTaskNotFound,
			"backup run was not found",
		)
	}
	stored, err := backupruntime.DecodeBackupRunRecord(anchor.Values[0].Value)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	replay := false
	if anchor.Values[0].ModRevision != current.Revision ||
		!backupruntime.BackupRunRecordsEqual(stored, current.Record) {
		if backupruntime.BackupRunRecordsEqual(stored, next) {
			replay = true
		} else {
			return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
				errs.KindStateConflict,
				"backup run changed",
			)
		}
	}
	if !replay {
		for index, condition := range extraConditions {
			if !etcdstore.ConditionMatchesRead(condition, anchor.Values[index+1]) {
				return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
					errs.KindStateConflict,
					"backup runtime companion state changed",
				)
			}
		}
		if validateExtra != nil {
			if err := validateExtra(anchor.Values[1:]); err != nil {
				return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
			}
		}
	}
	var checkpointPlan backupCheckpointPlan
	var assignmentConditions []etcdstore.Condition
	if authority != nil {
		if authority.TaskID != current.Record.TaskID {
			return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
				errs.KindValidationFailed,
				"backup assignment task does not match its run",
			)
		}
		assignmentConditions, err = repository.loadBackupAssignmentFence(
			ctx,
			*authority,
			anchor.ReadRevision,
		)
		if err != nil {
			return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
		}
	}
	if checkpoint != nil {
		if checkpoint.TaskID != current.Record.TaskID {
			return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
				errs.KindValidationFailed,
				"backup checkpoint task does not match its run",
			)
		}
		ordinal, changed := backupruntime.ChangedBackupSourceOrdinal(current.Record, next)
		if !changed {
			return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
				errs.KindValidationFailed,
				"backup checkpoint source is invalid",
			)
		}
		checkpointPlan, err = repository.loadBackupCheckpointPlan(
			ctx, *checkpoint, anchor.ReadRevision,
			backupRunCheckpointBinding(current.Record, ordinal),
		)
		if err != nil {
			return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
		}
		defer checkpointPlan.clear()
		if checkpointPlan.duplicate && !replay {
			return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
				errs.KindStateConflict,
				"backup checkpoint domain state is incomplete",
			)
		}
	}
	if replay {
		if checkpoint != nil && !checkpointPlan.duplicate {
			return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
				errs.KindStateConflict,
				"backup checkpoint replay evidence is incomplete",
			)
		}
		if checkpoint != nil && anchor.Values[0].ModRevision != checkpointPlan.commitRevision {
			return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
				errs.KindStateConflict,
				"backup checkpoint domain evidence changed",
			)
		}
		if !exactBackupRuntimeReplayCompanions(
			anchor.Values[1:],
			extraConditions,
			extraMutations,
			anchor.Values[0].ModRevision,
		) {
			return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
				errs.KindStateConflict,
				"backup runtime replay companion state changed",
			)
		}
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{
			Record:       stored,
			Revision:     anchor.Values[0].ModRevision,
			ReadRevision: anchor.ReadRevision,
		}, nil
	}
	evidence, err := repository.loadOwnedEvidence(ctx, current.Record, anchor.ReadRevision)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: backupruntime.BackupRunKey(current.Record.TaskID), ModRevision: current.Revision},
	}
	conditions = append(conditions, extraConditions...)
	conditions = append(conditions, evidence.fence.TransactionConditions()...)
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: backupruntime.BackupRunKey(next.TaskID), Value: value}}
	mutations = append(mutations, extraMutations...)
	epoch, err := evidence.fence.EpochRewriteMutation()
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	defer clear(epoch.Value)
	mutations = append(mutations, epoch)
	conditions = append(conditions, checkpointPlan.conditions...)
	conditions = append(conditions, assignmentConditions...)
	for _, mutation := range checkpointPlan.mutations {
		copyOfMutation := mutation
		copyOfMutation.Value = append([]byte(nil), mutation.Value...)
		mutations = append(mutations, copyOfMutation)
	}
	result, err := repository.TransactRuntime(ctx, conditions, mutations)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	if !result.Succeeded {
		defer etcdstore.ClearValues(result.FailureReads)
		if len(result.FailureReads) != len(conditions) {
			return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
				errs.KindInternal,
				"backup run transition compare evidence is incomplete",
			)
		}
		if result.FailureReads[0] == nil || result.FailureReads[0].ModRevision != current.Revision {
			return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
				errs.KindStateConflict,
				"backup run changed",
			)
		}
		fenceStart := 1 + len(extraConditions)
		fenceEnd := fenceStart + evidence.fence.ConditionCount()
		if err := evidence.fence.ClassifyConflict(result.FailureReads[fenceStart:fenceEnd]); err != nil {
			return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
		}
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindStateConflict,
			"backup runtime state changed",
		)
	}
	return etcdstore.Versioned[backupruntime.BackupRunRecord]{
		Record:       next,
		Revision:     result.Revision,
		ReadRevision: result.Revision,
	}, nil
}

func exactBackupRuntimeReplayCompanions(
	values []*etcdstore.KeyValue,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
	resultRevision int64,
) bool {
	if len(values) != len(conditions) || resultRevision <= 0 {
		return false
	}
	byKey := make(map[string]etcdstore.Mutation, len(mutations))
	for _, mutation := range mutations {
		if _, exists := byKey[mutation.Key]; exists {
			return false
		}
		byKey[mutation.Key] = mutation
	}
	for index, condition := range conditions {
		value := values[index]
		mutation, changed := byKey[condition.Key]
		if !changed {
			if !etcdstore.ConditionMatchesRead(condition, value) {
				return false
			}
			continue
		}
		delete(byKey, condition.Key)
		switch mutation.Type {
		case etcdstore.MutationPut:
			if value == nil || value.ModRevision != resultRevision ||
				!bytes.Equal(value.Value, mutation.Value) {
				return false
			}
		case etcdstore.MutationDelete:
			if value != nil {
				return false
			}
		default:
			return false
		}
	}
	return len(byKey) == 0
}
