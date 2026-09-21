package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"slices"
	"time"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) finalizeReleaseRecoveryBatch(
	ctx context.Context,
	task TaskRecord,
	assignment TaskAssignmentRecord,
	terminalStatus taskjournal.TaskStatus,
	result TaskResultRecord,
	agentID string,
	terminalAt time.Time,
	head ReleaseOperationHead,
	fence ReleaseFenceSet,
	base *etcdstore.GetManyResult,
	terminals *etcdstore.GetManyResult,
	proofConditions ...etcdstore.Condition,
) (bool, error) {
	if terminalStatus != taskjournal.TaskStatusCompleted || result.ReconciliationRequired {
		return repository.returnReleaseToRecovery(ctx, task, head, base, terminalAt, proofConditions...)
	}
	pending := make([]int, 0, maximumReleaseTerminalBatchMembers)
	for index, value := range terminals.Values {
		if value == nil {
			return false, corruptReleaseRecord()
		}
		summary, err := decodeReleaseRecord[domain.TerminalSummary](value.Value, "release-terminal-summary")
		if err != nil || summary.ReleaseID != head.Members[index].ReleaseID {
			return false, corruptReleaseRecord()
		}
		if !slices.Contains(summary.AttemptIDs, task.ID) && len(pending) < maximumReleaseTerminalBatchMembers {
			pending = append(pending, index)
		}
	}
	if len(pending) == 0 {
		return repository.closeRecoveredRelease(ctx, task, head, fence, base, terminals, terminalAt, proofConditions...)
	}
	detailKeys := make([]string, 0, len(pending)*3)
	for _, index := range pending {
		member := head.Members[index]
		detailKeys = append(detailKeys,
			releaseIntentStagingKey(head.PublicationID, member.ReleaseID),
			releaseCheckpointStagingKey(head.PublicationID, member.ReleaseID),
			releaseProjectionKey(member.ServiceID),
		)
	}
	details, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: detailKeys, Revision: base.ReadRevision})
	if err != nil {
		return false, err
	}
	if details == nil || details.ReadRevision != base.ReadRevision || len(details.Values) != len(detailKeys) {
		return false, corruptReleaseRecord()
	}
	conditions := []etcdstore.Condition{
		{Key: base.Values[0].Key, ModRevision: base.Values[0].ModRevision},
		{Key: base.Values[1].Key, ModRevision: base.Values[1].ModRevision},
		{Key: base.Values[2].Key, ModRevision: base.Values[2].ModRevision},
		{Key: base.Values[3].Key, ModRevision: base.Values[3].ModRevision},
	}
	mutations := make([]etcdstore.Mutation, 0, len(pending)*3)
	defer clearMutations(mutations)
	for offset, index := range pending {
		member := head.Members[index]
		intentValue, checkpointValue, projectionValue := details.Values[offset*3], details.Values[offset*3+1], details.Values[offset*3+2]
		terminalValue := terminals.Values[index]
		if intentValue == nil || checkpointValue == nil || terminalValue == nil {
			return false, corruptReleaseRecord()
		}
		intent, err := decodeReleaseRecord[domain.Intent](intentValue.Value, "release-intent")
		if err != nil || domain.ValidateIntent(intent) != nil || intent.ID != member.ReleaseID ||
			intent.ServiceID != member.ServiceID {
			return false, corruptReleaseRecord()
		}
		checkpoint, err := decodeReleaseRecord[domain.Checkpoint](checkpointValue.Value, "release-checkpoint")
		if err != nil || checkpoint.ReleaseID != intent.ID || domain.ValidateCheckpoint(checkpoint) != nil {
			return false, corruptReleaseRecord()
		}
		projection, err := decodeReleaseProjection(projectionValue, task.Owner.EnvironmentID, member.ServiceID)
		if err != nil {
			return false, err
		}
		summary, err := decodeReleaseRecord[domain.TerminalSummary](terminalValue.Value, "release-terminal-summary")
		if err != nil || summary.ReleaseID != intent.ID {
			return false, corruptReleaseRecord()
		}
		proxy, hasProxy := releaseProxyEvidence(result, member.ServiceID)
		recreate, hasRecreate := releaseRecreateEvidence(result, member.ServiceID)
		if hasProxy == hasRecreate {
			return false, errs.New(
				errs.KindStateConflict,
				"release recovery omitted its strategy-specific serving evidence",
			)
		}
		observedReleaseID, compensated := proxy.ReleaseID, proxy.Compensated
		observedTarget := proxy.Target
		if hasRecreate {
			observedReleaseID, compensated = recreate.ReleaseID, recreate.Compensated
			observedTarget = recreate.Target
		}
		candidate := !compensated && observedReleaseID == intent.ID
		priorReleaseID := intent.PriorServingReleaseID
		if !candidate && (priorReleaseID == "" || observedReleaseID != priorReleaseID) {
			return false, errs.New(errs.KindStateConflict, "release recovery observed an unsealed proxy lineage")
		}
		if projection.EnvironmentID == "" {
			projection.EnvironmentID, projection.ServiceID = task.Owner.EnvironmentID, member.ServiceID
		}
		projection.ServingSlot = ""
		projection.ServingSlot = domain.WorkloadTarget(observedTarget).Slot()
		projection.ActiveOperationID = ""
		projection.Revision++
		checkpoint.Evidence = nil
		checkpoint.UpdatedAt = terminalAt
		if candidate {
			projection.ServingReleaseID = intent.ID
			projection.CurrentSuccessfulReleaseID = intent.ID
			checkpoint.State = domain.StateCompleted
			effectDigest, err := domain.Digest(struct {
				ReleaseID string           `json:"release_id"`
				Result    TaskResultRecord `json:"result"`
			}{intent.ID, result})
			if err != nil {
				return false, err
			}
			effect := domain.EffectEvidence{
				PlanID: task.PlanID, StepID: task.Steps[index*5+3].ID, AttemptID: task.ID,
				AgentID: agentID, AcknowledgementID: assignment.AssignmentID, ObservedReleaseID: intent.ID,
				ObservedRenderGeneration: uint64(
					task.RenderGeneration,
				), EffectDigest: effectDigest, AcknowledgedAt: terminalAt,
			}
			if hasProxy {
				effect.ObservedSlot, effect.RouterConfigurationDigest, effect.ObservedRouterTarget = domain.WorkloadTarget(proxy.Target).
					Slot(),
					proxy.ConfigSHA256, proxy.Target
			}
			checkpoint.Evidence = []domain.EffectEvidence{effect}
			summary.EffectDigests = append(slices.Clone(summary.EffectDigests), effectDigest)
			summary.Outcome = domain.StateCompleted
			summary.FinalServingReleaseID = intent.ID
		} else {
			projection.ServingReleaseID = intent.PriorServingReleaseID
			projection.CurrentSuccessfulReleaseID = intent.PriorSuccessfulReleaseID
			checkpoint.State = domain.StateFailed
			summary.Outcome = domain.StateFailed
			summary.FinalServingReleaseID = intent.PriorServingReleaseID
		}
		if err := domain.ValidateCheckpoint(checkpoint); err != nil {
			return false, err
		}
		summary.AttemptIDs = append(slices.Clone(summary.AttemptIDs), task.ID)
		summary.CompletedAt = terminalAt
		encodedCheckpoint, err := encodeReleaseRecord("release-checkpoint", checkpoint)
		if err != nil {
			return false, err
		}
		encodedProjection, err := encodeReleaseRecord("service-release-projection", projection)
		if err != nil {
			clear(encodedCheckpoint)
			return false, err
		}
		encodedTerminal, err := encodeReleaseRecord("release-terminal-summary", summary)
		if err != nil {
			clear(encodedCheckpoint)
			clear(encodedProjection)
			return false, err
		}
		conditions = append(conditions,
			etcdstore.Condition{Key: detailKeys[offset*3], ModRevision: intentValue.ModRevision},
			etcdstore.Condition{Key: detailKeys[offset*3+1], ModRevision: checkpointValue.ModRevision},
			etcdstore.Condition{Key: detailKeys[offset*3+2], ModRevision: keyValueRevision(projectionValue)},
			etcdstore.Condition{Key: terminalValue.Key, ModRevision: terminalValue.ModRevision},
		)
		mutations = append(mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: checkpointValue.Key, Value: encodedCheckpoint},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: detailKeys[offset*3+2], Value: encodedProjection},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: terminalValue.Key, Value: encodedTerminal},
		)
	}
	if len(conditions)+len(proofConditions)+len(mutations) > etcdstore.MaximumOperations {
		return false, errs.New(errs.KindInternal, "release recovery batch exceeds the transaction ceiling")
	}
	transaction, err := repository.store.Transact(ctx, append(conditions, proofConditions...), mutations)
	if err != nil {
		return false, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return false, errs.New(errs.KindStateConflict, "release recovery evidence changed")
	}
	return true, nil
}

func (repository *TaskRepository) returnReleaseToRecovery(
	ctx context.Context,
	task TaskRecord,
	head ReleaseOperationHead,
	base *etcdstore.GetManyResult,
	terminalAt time.Time,
	proofConditions ...etcdstore.Condition,
) (bool, error) {
	head.State = domain.StateRecoveryRequired
	head.UpdatedAt = terminalAt
	headValue, err := encodeReleaseRecord("release-operation", head)
	if err != nil {
		return false, err
	}
	defer clear(headValue)
	transaction, err := repository.store.Transact(ctx, append([]etcdstore.Condition{
		{Key: base.Values[0].Key, ModRevision: base.Values[0].ModRevision},
		{Key: base.Values[1].Key, ModRevision: base.Values[1].ModRevision},
		{Key: base.Values[2].Key, ModRevision: base.Values[2].ModRevision},
		{Key: base.Values[3].Key, ModRevision: base.Values[3].ModRevision},
	}, proofConditions...), []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: releaseOperationKey(task.OperationID), Value: headValue},
		{Type: etcdstore.MutationPut, Key: base.Values[3].Key, Value: slices.Clone(base.Values[3].Value)},
	})
	if err != nil {
		return false, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return false, errs.New(errs.KindStateConflict, "release recovery failure evidence changed")
	}
	return true, nil
}

func (repository *TaskRepository) closeRecoveredRelease(
	ctx context.Context,
	task TaskRecord,
	head ReleaseOperationHead,
	fence ReleaseFenceSet,
	base, terminals *etcdstore.GetManyResult,
	terminalAt time.Time,
	proofConditions ...etcdstore.Condition,
) (bool, error) {
	if !head.RecoveryOutcome.Terminal() || fence.AttemptTaskID != task.ID {
		return false, corruptReleaseRecord()
	}
	head.State = head.RecoveryOutcome
	head.RecoveryOutcome = ""
	head.FailedMemberOrdinal = 0
	head.UpdatedAt = terminalAt
	headValue, err := encodeReleaseRecord("release-operation", head)
	if err != nil {
		return false, err
	}
	defer clear(headValue)
	conditions := []etcdstore.Condition{
		{Key: base.Values[0].Key, ModRevision: base.Values[0].ModRevision},
		{Key: base.Values[1].Key, ModRevision: base.Values[1].ModRevision},
		{Key: base.Values[2].Key, ModRevision: base.Values[2].ModRevision},
		{Key: base.Values[3].Key, ModRevision: base.Values[3].ModRevision},
	}
	for _, terminal := range terminals.Values {
		conditions = append(conditions, etcdstore.Condition{Key: terminal.Key, ModRevision: terminal.ModRevision})
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: releaseOperationKey(task.OperationID), Value: headValue},
		{Type: etcdstore.MutationDelete, Key: releaseFenceSetKey(task.Owner.EnvironmentID)},
		{Type: etcdstore.MutationPut, Key: base.Values[3].Key, Value: slices.Clone(base.Values[3].Value)},
	}
	defer clearMutations(mutations)
	transaction, err := repository.store.Transact(ctx, append(conditions, proofConditions...), mutations)
	if err != nil {
		return false, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return false, errs.New(errs.KindStateConflict, "release recovery closure evidence changed")
	}
	return true, nil
}
