package etcd

import (
	"context"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"slices"
	"time"
)

func (repository *TaskRepository) closeReleaseOperation(
	ctx context.Context,
	task TaskRecord,
	head ReleaseOperationHead,
	fence ReleaseFenceSet,
	base *etcdstore.GetManyResult,
	terminals *etcdstore.GetManyResult,
	terminalStatus taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	terminalAt time.Time,
	proofConditions ...etcdstore.Condition,
) (bool, error) {
	for _, value := range terminals.Values {
		if value == nil {
			return false, corruptReleaseRecord()
		}
	}
	state := releaseOperationTerminalState(terminalStatus)
	if result.ReconciliationRequired {
		state = domain.StateRecoveryRequired
	}
	if head.Progress != nil {
		progress, err := releaseTerminalGroupProgress(head, task, terminalStatus, result, terminalAt)
		if err != nil {
			return false, err
		}
		head.Progress = &progress
	}
	head.State = state
	if state == domain.StateRecoveryRequired {
		head.RecoveryOutcome = releaseOperationTerminalState(terminalStatus)
		head.FailedMemberOrdinal = releaseFailedOrdinalFromResult(task, result)
	} else {
		head.RecoveryOutcome = ""
		head.FailedMemberOrdinal = 0
	}
	head.UpdatedAt = terminalAt
	headValue, err := encodeReleaseRecord("release-operation", head)
	if err != nil {
		return false, err
	}
	defer clear(headValue)
	epochValue := slices.Clone(base.Values[3].Value)
	defer clear(epochValue)
	conditions := []etcdstore.Condition{
		{Key: base.Values[0].Key, ModRevision: base.Values[0].ModRevision},
		{Key: base.Values[1].Key, ModRevision: base.Values[1].ModRevision},
		{Key: base.Values[2].Key, ModRevision: base.Values[2].ModRevision},
		{Key: base.Values[3].Key, ModRevision: base.Values[3].ModRevision},
	}
	for _, value := range terminals.Values {
		conditions = append(conditions, etcdstore.Condition{Key: value.Key, ModRevision: value.ModRevision})
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: releaseOperationKey(task.OperationID), Value: headValue},
		{Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentMutationEpochKey(task.Owner.EnvironmentID), Value: epochValue},
	}
	if state != domain.StateRecoveryRequired {
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: releaseFenceSetKey(task.Owner.EnvironmentID)})
	} else if fence.OperationID != task.OperationID {
		return false, corruptReleaseRecord()
	}
	transaction, err := repository.store.Transact(ctx, append(conditions, proofConditions...), mutations)
	if err != nil {
		return false, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return false, errs.New(errs.KindStateConflict, "release terminal closure evidence changed")
	}
	return true, nil
}

func (repository *TaskRepository) validateReleaseTerminalMembers(
	ctx context.Context,
	task TaskRecord,
	head ReleaseOperationHead,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) error {
	keys := make([]string, len(head.Members))
	for index, member := range head.Members {
		keys[index] = releaseTerminalKey(member.ReleaseID)
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return err
	}
	if read == nil || len(read.Values) != len(keys) {
		return corruptReleaseRecord()
	}
	for index, value := range read.Values {
		if value == nil {
			return corruptReleaseRecord()
		}
		summary, err := decodeReleaseRecord[domain.TerminalSummary](value.Value, "release-terminal-summary")
		if err != nil || summary.ReleaseID != head.Members[index].ReleaseID ||
			(head.State != domain.StateRecoveryRequired && !slices.Contains(summary.AttemptIDs, task.ID)) {
			return corruptReleaseRecord()
		}
	}
	if head.State != domain.StateRecoveryRequired && task.RetryOf == "" &&
		head.State != releaseOperationTerminalState(terminalStatus) {
		return errs.New(errs.KindStateConflict, "release terminal outcome changed")
	}
	return nil
}
