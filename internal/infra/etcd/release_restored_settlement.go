package etcd

import (
	"context"
	"slices"
	"time"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Restoration settles current runtime facts, not a second forward outcome.
// Existing terminal summaries remain history, including a completion published
// before the Task was interrupted. Each member and the final head are CAS-fenced
// by the exact validated recovery proof; the Task retains its original failure.
func (repository *TaskRepository) finalizeRestoredRelease(
	ctx context.Context, task TaskRecord, assignment taskassignments.TaskAssignmentRecord,
	recovery releaseRecoveryAcknowledgement, at time.Time, revision int64,
	closingGuards ...etcdstore.Condition,
) (bool, error) {
	if !recovery.final || recovery.value == nil || len(recovery.conditions) == 0 ||
		assignment.ExecutionMode != taskassignments.TaskExecutionModeRecoveryOnly ||
		assignment.RestorationAuthority == nil {
		return false, taskassignments.CorruptTaskAssignment()
	}
	publication := task.Params[releaserender.TaskReleasePublicationParam]
	keys := []string{releases.ReleaseOperationKey(task.OperationID),
		releases.ReleaseFenceSetKey(
			task.Owner.EnvironmentID,
		), hierarchyrecord.EnvironmentMutationEpochKey(task.Owner.EnvironmentID)}
	base, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return false, err
	}
	if base == nil || len(base.Values) != len(keys) || base.Values[0] == nil || base.Values[2] == nil {
		return false, releases.CorruptReleaseRecord()
	}
	head, err := releases.DecodeReleaseRecord[releases.ReleaseOperationHead](base.Values[0].Value, "release-operation")
	if err != nil || head.OperationID != task.OperationID || head.PublicationID != publication ||
		head.LatestTaskID != task.ID || head.EnvironmentID != task.Owner.EnvironmentID ||
		len(head.Members) != len(assignment.RestorationAuthority.Candidates) {
		return false, releases.CorruptReleaseRecord()
	}
	if base.Values[1] != nil {
		fence, err := releases.DecodeReleaseRecord[releases.ReleaseFenceSet](base.Values[1].Value, "release-fence-set")
		if err != nil || fence.OperationID != task.OperationID || fence.AttemptTaskID != task.ID {
			return false, releases.CorruptReleaseRecord()
		}
	} else if !head.State.Terminal() {
		return false, releases.CorruptReleaseRecord()
	}
	conditions := append([]etcdstore.Condition{}, recovery.conditions...)
	conditions = append(conditions, closingGuards...)
	for index, key := range keys {
		conditions = append(
			conditions,
			etcdstore.Condition{Key: key, ModRevision: etcdstore.RevisionOf(base.Values[index])},
		)
	}
	for index, member := range head.Members {
		candidate := assignment.RestorationAuthority.Candidates[index]
		if candidate.ServiceID != member.ServiceID || candidate.ReleaseID != member.ReleaseID {
			return false, releases.CorruptReleaseRecord()
		}
		processed, err := repository.settleRestoredReleaseMember(
			ctx,
			task,
			assignment,
			recovery,
			head,
			member,
			at,
			revision,
			conditions,
		)
		if err != nil || processed {
			return processed, err
		}
	}
	state := releaseOperationTerminalState(recovery.status)
	if head.State == state && base.Values[1] == nil {
		return false, nil
	}
	head.State, head.RecoveryOutcome, head.FailedMemberOrdinal, head.UpdatedAt = state, "", 0, at
	encoded, err := releases.EncodeReleaseRecord("release-operation", head)
	if err != nil {
		return false, err
	}
	defer clear(encoded)
	return repository.commitRestoredRelease(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: keys[0], Value: encoded},
		{Type: etcdstore.MutationDelete, Key: keys[1]},
		{Type: etcdstore.MutationPut, Key: keys[2], Value: base.Values[2].Value},
	})
}

func (repository *TaskRepository) settleRestoredReleaseMember(
	ctx context.Context, task TaskRecord, assignment taskassignments.TaskAssignmentRecord,
	recovery releaseRecoveryAcknowledgement, head releases.ReleaseOperationHead, member domain.GroupMember,
	at time.Time, revision int64, guards []etcdstore.Condition,
) (bool, error) {
	keys := []string{releases.ReleaseIntentStagingKey(head.PublicationID, member.ReleaseID),
		releases.ReleaseCheckpointStagingKey(head.PublicationID, member.ReleaseID),
		releases.ReleaseProjectionKey(member.ServiceID), releases.ReleaseTerminalKey(member.ReleaseID),
		releases.ReleaseRetentionKey(member.ReleaseID), serviceruntimerecord.Key(member.ServiceID)}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return false, err
	}
	if read == nil || len(read.Values) != len(keys) || read.Values[0] == nil || read.Values[1] == nil {
		return false, releases.CorruptReleaseRecord()
	}
	intent, err := releases.DecodeReleaseRecord[domain.Intent](read.Values[0].Value, "release-intent")
	if err != nil || domain.ValidateIntent(intent) != nil || intent.ID != member.ReleaseID ||
		intent.ServiceID != member.ServiceID || intent.OperationID != task.OperationID {
		return false, releases.CorruptReleaseRecord()
	}
	checkpoint, err := releases.DecodeReleaseRecord[domain.Checkpoint](read.Values[1].Value, "release-checkpoint")
	if err != nil || checkpoint.ReleaseID != intent.ID || domain.ValidateCheckpoint(checkpoint) != nil {
		return false, releases.CorruptReleaseRecord()
	}
	state := releaseOperationTerminalState(recovery.status)
	if read.Values[3] != nil {
		history, err := releases.DecodeReleaseRecord[domain.TerminalSummary](
			read.Values[3].Value,
			"release-terminal-summary",
		)
		if err != nil || history.ReleaseID != intent.ID || !slices.Contains(history.AttemptIDs, task.ID) {
			return false, releases.CorruptReleaseRecord()
		}
	}
	projection, err := decodeReleaseProjection(read.Values[2], task.Owner.EnvironmentID, member.ServiceID)
	if err != nil {
		return false, err
	}
	if projection.ServingReleaseID != "" && projection.ServingReleaseID != intent.ID &&
		projection.ServingReleaseID != intent.PriorServingReleaseID {
		return false, errs.New(errs.KindStateConflict, "restored release cannot replace another serving operation")
	}
	runtime, err := restoredReleaseRuntimeMutation(task, assignment, recovery, intent, read.Values[5], at)
	if err != nil {
		return false, err
	}
	defer clear(runtime.Value)
	if checkpoint.State == state && read.Values[3] != nil && runtime.Key == "" &&
		projection.ServingReleaseID == intent.PriorServingReleaseID &&
		projection.CurrentSuccessfulReleaseID == intent.PriorSuccessfulReleaseID {
		return false, nil
	}
	checkpoint.State, checkpoint.Evidence, checkpoint.UpdatedAt = state, nil, at
	projection.EnvironmentID, projection.ServiceID = task.Owner.EnvironmentID, member.ServiceID
	projection.ServingReleaseID, projection.CurrentSuccessfulReleaseID = intent.PriorServingReleaseID, intent.PriorSuccessfulReleaseID
	projection.ServingSlot, projection.ActiveOperationID = "", ""
	if proxy, ok := releaseProxyEvidence(recovery.result, member.ServiceID); ok {
		projection.ServingSlot = domain.WorkloadTarget(proxy.Target).Slot()
	}
	projection.Revision++
	retention := releaseRollbackMaterial(intent, state, at)
	summary := releaseTerminalSummary(
		intent,
		state,
		intent.PriorServingReleaseID,
		nil,
		head.Attempts,
		retention.Digest,
		at,
	)
	mutations, err := releaseTerminalRecordMutations(keys, checkpoint, summary, retention, projection, true)
	if err != nil {
		return false, err
	}
	defer func() { etcdstore.ZeroMutationBytes(mutations) }()
	if read.Values[3] != nil {
		// Published history is never rewritten by recovery.
		clear(mutations[1].Value)
		mutations = append(mutations[:1], mutations[2:]...)
	}
	if runtime.Key != "" {
		mutations = append(mutations, runtime)
	}
	conditions := append([]etcdstore.Condition{}, guards...)
	for index, key := range keys {
		conditions = append(
			conditions,
			etcdstore.Condition{Key: key, ModRevision: etcdstore.RevisionOf(read.Values[index])},
		)
	}
	if runtime.Type == etcdstore.MutationPut {
		postgresConditions, postgresMutations, err := repository.postgresRuntimeAcknowledgement(
			ctx,
			runtime.Value,
			revision,
		)
		if err != nil {
			return false, err
		}
		conditions = append(conditions, postgresConditions...)
		mutations = append(mutations, postgresMutations...)
	}
	return repository.commitRestoredRelease(ctx, conditions, mutations)
}

func (repository *TaskRepository) commitRestoredRelease(
	ctx context.Context,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) (bool, error) {
	budget, err := repository.store.MeasureTransaction(ctx, conditions, mutations)
	if err != nil {
		return false, err
	}
	if !budget.Fits() {
		return false, errs.New(errs.KindValidationFailed, "release restoration settlement exceeds transaction budget")
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return false, err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return false, errs.New(errs.KindStateConflict, "release restoration settlement authority changed")
	}
	return true, nil
}
