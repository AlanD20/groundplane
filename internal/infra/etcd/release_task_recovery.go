package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"slices"
	"time"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) finalizeReleaseRecoveryBatch(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	terminalStatus taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	agentID string,
	terminalAt time.Time,
	head releases.ReleaseOperationHead,
	fence releases.ReleaseFenceSet,
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
			return false, releases.CorruptReleaseRecord()
		}
		summary, err := releases.DecodeReleaseRecord[domain.TerminalSummary](value.Value, "release-terminal-summary")
		if err != nil || summary.ReleaseID != head.Members[index].ReleaseID {
			return false, releases.CorruptReleaseRecord()
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
			releases.ReleaseIntentStagingKey(head.PublicationID, member.ReleaseID),
			releases.ReleaseCheckpointStagingKey(head.PublicationID, member.ReleaseID),
			releases.ReleaseProjectionKey(member.ServiceID),
		)
	}
	details, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: detailKeys, Revision: base.ReadRevision})
	if err != nil {
		return false, err
	}
	if details == nil || details.ReadRevision != base.ReadRevision || len(details.Values) != len(detailKeys) {
		return false, releases.CorruptReleaseRecord()
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
			return false, releases.CorruptReleaseRecord()
		}
		intent, err := releases.DecodeReleaseRecord[domain.Intent](intentValue.Value, "release-intent")
		if err != nil || domain.ValidateIntent(intent) != nil || intent.ID != member.ReleaseID ||
			intent.ServiceID != member.ServiceID {
			return false, releases.CorruptReleaseRecord()
		}
		checkpoint, err := releases.DecodeReleaseRecord[domain.Checkpoint](checkpointValue.Value, "release-checkpoint")
		if err != nil || checkpoint.ReleaseID != intent.ID || domain.ValidateCheckpoint(checkpoint) != nil {
			return false, releases.CorruptReleaseRecord()
		}
		projection, err := decodeReleaseProjection(projectionValue, task.Owner.EnvironmentID, member.ServiceID)
		if err != nil {
			return false, err
		}
		summary, err := releases.DecodeReleaseRecord[domain.TerminalSummary](terminalValue.Value, "release-terminal-summary")
		if err != nil || summary.ReleaseID != intent.ID {
			return false, releases.CorruptReleaseRecord()
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
				ReleaseID string                       `json:"release_id"`
				Result    taskjournal.TaskResultRecord `json:"result"`
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
		encodedCheckpoint, err := releases.EncodeReleaseRecord("release-checkpoint", checkpoint)
		if err != nil {
			return false, err
		}
		encodedProjection, err := releases.EncodeReleaseRecord("service-release-projection", projection)
		if err != nil {
			clear(encodedCheckpoint)
			return false, err
		}
		encodedTerminal, err := releases.EncodeReleaseRecord("release-terminal-summary", summary)
		if err != nil {
			clear(encodedCheckpoint)
			clear(encodedProjection)
			return false, err
		}
		conditions = append(conditions,
			etcdstore.Condition{Key: detailKeys[offset*3], ModRevision: intentValue.ModRevision},
			etcdstore.Condition{Key: detailKeys[offset*3+1], ModRevision: checkpointValue.ModRevision},
			etcdstore.Condition{Key: detailKeys[offset*3+2], ModRevision: etcdstore.RevisionOf(projectionValue)},
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
	etcdstore.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return false, errs.New(errs.KindStateConflict, "release recovery evidence changed")
	}
	return true, nil
}

func (repository *TaskRepository) returnReleaseToRecovery(
	ctx context.Context,
	task TaskRecord,
	head releases.ReleaseOperationHead,
	base *etcdstore.GetManyResult,
	terminalAt time.Time,
	proofConditions ...etcdstore.Condition,
) (bool, error) {
	head.State = domain.StateRecoveryRequired
	head.UpdatedAt = terminalAt
	headValue, err := releases.EncodeReleaseRecord("release-operation", head)
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
		{Type: etcdstore.MutationPut, Key: releases.ReleaseOperationKey(task.OperationID), Value: headValue},
		{Type: etcdstore.MutationPut, Key: base.Values[3].Key, Value: slices.Clone(base.Values[3].Value)},
	})
	if err != nil {
		return false, err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return false, errs.New(errs.KindStateConflict, "release recovery failure evidence changed")
	}
	return true, nil
}

func (repository *TaskRepository) closeRecoveredRelease(
	ctx context.Context,
	task TaskRecord,
	head releases.ReleaseOperationHead,
	fence releases.ReleaseFenceSet,
	base, terminals *etcdstore.GetManyResult,
	terminalAt time.Time,
	proofConditions ...etcdstore.Condition,
) (bool, error) {
	if !head.RecoveryOutcome.Terminal() || fence.AttemptTaskID != task.ID {
		return false, releases.CorruptReleaseRecord()
	}
	head.State = head.RecoveryOutcome
	head.RecoveryOutcome = ""
	head.FailedMemberOrdinal = 0
	head.UpdatedAt = terminalAt
	headValue, err := releases.EncodeReleaseRecord("release-operation", head)
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
		{Type: etcdstore.MutationPut, Key: releases.ReleaseOperationKey(task.OperationID), Value: headValue},
		{Type: etcdstore.MutationDelete, Key: releases.ReleaseFenceSetKey(task.Owner.EnvironmentID)},
		{Type: etcdstore.MutationPut, Key: base.Values[3].Key, Value: slices.Clone(base.Values[3].Value)},
	}
	defer clearMutations(mutations)
	transaction, err := repository.store.Transact(ctx, append(conditions, proofConditions...), mutations)
	if err != nil {
		return false, err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return false, errs.New(errs.KindStateConflict, "release recovery closure evidence changed")
	}
	return true, nil
}
