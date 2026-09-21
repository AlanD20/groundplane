package etcd

import (
	"context"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"slices"
	"time"
)

const maximumReleaseTerminalBatchMembers = 10

// finalizeReleaseTaskBatch considers at most ten Release members. The Task
// primary is the retry cursor: it remains running until every immutable member
// summary exists and the operation head and Environment fence are closed.
// Each member's complete write set is measured before joining the batch,
// including proof guards and the store's physical key prefix.
func (repository *TaskRepository) finalizeReleaseTaskBatch(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	terminalStatus taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	agentID string,
	terminalAt time.Time,
	readRevision int64,
	proofConditions ...etcdstore.Condition,
) (bool, error) {
	publicationID := task.Params[TaskReleasePublicationParam]
	if publicationID == "" {
		return false, nil
	}
	if task.Executor != taskjournal.TaskExecutorAgent ||
		(task.Type != taskjournal.TaskDeploy && task.Type != taskjournal.TaskRollback && task.Type != taskjournal.TaskUpdate) ||
		releases.ValidatePublicationID(publicationID) != nil || task.OperationID == "" || task.RenderGeneration <= 0 {
		return false, releases.CorruptReleaseRecord()
	}
	if terminalStatus != taskjournal.TaskStatusCompleted && result.FailedStepID == "" &&
		!unassignedReleaseAbort(task, terminalStatus) &&
		result.Diagnostic != taskjournal.TaskResultDiagnosticTimeoutBeforeEffect {
		return false, errs.New(errs.KindStateConflict, "release failure is missing its failed step identity")
	}
	if task.Type == taskjournal.TaskUpdate {
		if !result.ReconciliationRequired {
			processed, terminalErr := repository.finalizeReleaseHookExecutionBatch(ctx, task, terminalAt, readRevision)
			if terminalErr != nil || processed {
				return processed, terminalErr
			}
		}
		return repository.finalizeBlueprintReleaseTaskBatch(
			ctx, task, assignment, terminalStatus, result, agentID, terminalAt, readRevision,
		)
	}
	baseKeys := []string{
		releases.ReleasePublicationKey(publicationID), releases.ReleaseOperationKey(task.OperationID),
		releases.ReleaseFenceSetKey(task.Owner.EnvironmentID), hierarchyrecord.EnvironmentMutationEpochKey(task.Owner.EnvironmentID),
	}
	base, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: baseKeys, Revision: readRevision})
	if err != nil {
		return false, err
	}
	if base == nil || len(base.Values) != len(baseKeys) || base.Values[0] == nil || base.Values[1] == nil ||
		base.Values[3] == nil {
		return false, releases.CorruptReleaseRecord()
	}
	marker, err := releases.DecodeReleaseRecord[releases.ReleasePublicationMarker](base.Values[0].Value, "release-publication")
	if err != nil || marker.PublicationID != publicationID || marker.OperationID != task.OperationID {
		return false, releases.CorruptReleaseRecord()
	}
	head, err := releases.DecodeReleaseRecord[releases.ReleaseOperationHead](base.Values[1].Value, "release-operation")
	if err != nil || head.OperationID != task.OperationID || head.PublicationID != publicationID ||
		head.EnvironmentID != task.Owner.EnvironmentID || head.LatestTaskID != task.ID || len(head.Members) == 0 {
		return false, releases.CorruptReleaseRecord()
	}
	if head.State.Terminal() || head.State == domain.StateRecoveryRequired {
		if base.Values[2] != nil && head.State != domain.StateRecoveryRequired {
			return false, releases.CorruptReleaseRecord()
		}
		return false, repository.validateReleaseTerminalMembers(ctx, task, head, terminalStatus, readRevision)
	}
	if base.Values[2] == nil {
		return false, releases.CorruptReleaseRecord()
	}
	fence, err := releases.DecodeReleaseRecord[releases.ReleaseFenceSet](base.Values[2].Value, "release-fence-set")
	if err != nil || fence.OperationID != task.OperationID || fence.AttemptTaskID != task.ID ||
		fence.EnvironmentID != task.Owner.EnvironmentID || len(fence.Members) != len(head.Members) {
		return false, releases.CorruptReleaseRecord()
	}
	if !result.ReconciliationRequired {
		processed, terminalErr := repository.finalizeReleaseHookExecutionBatch(ctx, task, terminalAt, readRevision)
		if terminalErr != nil || processed {
			return processed, terminalErr
		}
	}
	terminalKeys := make([]string, len(head.Members))
	for index, member := range head.Members {
		terminalKeys[index] = releases.ReleaseTerminalKey(member.ReleaseID)
	}
	terminalRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: terminalKeys, Revision: readRevision})
	if err != nil {
		return false, err
	}
	if terminalRead == nil || len(terminalRead.Values) != len(terminalKeys) {
		return false, releases.CorruptReleaseRecord()
	}
	if task.RetryOf != "" && head.State == domain.StateRecovering {
		return repository.finalizeReleaseRecoveryBatch(
			ctx,
			task,
			assignment,
			terminalStatus,
			result,
			agentID,
			terminalAt,
			head,
			fence,
			base,
			terminalRead,
			proofConditions...)
	}
	pending := make([]int, 0, maximumReleaseTerminalBatchMembers)
	for index, value := range terminalRead.Values {
		if value == nil && len(pending) < maximumReleaseTerminalBatchMembers {
			pending = append(pending, index)
		}
	}
	if len(pending) == 0 {
		return repository.closeReleaseOperation(
			ctx,
			task,
			head,
			fence,
			base,
			terminalRead,
			terminalStatus,
			result,
			terminalAt,
			proofConditions...)
	}

	failedOrdinal, err := releaseFailedMemberOrdinal(task, terminalStatus, result)
	if err != nil {
		return false, err
	}
	detailKeys := make([]string, 0, len(pending)*6)
	for _, index := range pending {
		member := head.Members[index]
		detailKeys = append(detailKeys,
			releases.ReleaseIntentStagingKey(publicationID, member.ReleaseID),
			releases.ReleaseCheckpointStagingKey(publicationID, member.ReleaseID),
			releases.ReleaseProjectionKey(member.ServiceID),
			releases.ReleaseTerminalKey(member.ReleaseID),
			releases.ReleaseRetentionKey(member.ReleaseID),
			serviceruntimerecord.Key(member.ServiceID),
		)
	}
	details, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: detailKeys, Revision: readRevision})
	if err != nil {
		return false, err
	}
	if details == nil || len(details.Values) != len(detailKeys) {
		return false, releases.CorruptReleaseRecord()
	}
	conditions := []etcdstore.Condition{
		{Key: baseKeys[0], ModRevision: base.Values[0].ModRevision},
		{Key: baseKeys[1], ModRevision: base.Values[1].ModRevision},
		{Key: baseKeys[2], ModRevision: base.Values[2].ModRevision},
		{Key: baseKeys[3], ModRevision: base.Values[3].ModRevision},
	}
	mutations := make([]etcdstore.Mutation, 0, len(pending)*4)
	defer func() { clearMutations(mutations) }()
	for offset, index := range pending {
		values := details.Values[offset*6 : offset*6+6]
		keys := detailKeys[offset*6 : offset*6+6]
		if values[0] == nil || values[1] == nil || values[3] != nil || values[4] != nil {
			return false, releases.CorruptReleaseRecord()
		}
		member := head.Members[index]
		intent, err := releases.DecodeReleaseRecord[domain.Intent](values[0].Value, "release-intent")
		if err != nil || intent.ID != member.ReleaseID || intent.ServiceID != member.ServiceID ||
			intent.OperationID != task.OperationID || domain.ValidateIntent(intent) != nil {
			return false, releases.CorruptReleaseRecord()
		}
		checkpoint, err := releases.DecodeReleaseRecord[domain.Checkpoint](values[1].Value, "release-checkpoint")
		if err != nil || checkpoint.ReleaseID != intent.ID || domain.ValidateCheckpoint(checkpoint) != nil ||
			checkpoint.State != domain.StatePending {
			return false, releases.CorruptReleaseRecord()
		}
		projection, err := decodeReleaseProjection(values[2], task.Owner.EnvironmentID, member.ServiceID)
		if err != nil {
			return false, err
		}
		proxy, hasProxy := releaseProxyEvidence(result, member.ServiceID)
		recreate, hasRecreate := releaseRecreateEvidence(result, member.ServiceID)
		if hasProxy && hasRecreate {
			return false, errs.New(errs.KindStateConflict, "release serving evidence uses the wrong strategy authority")
		}
		observedReleaseID := proxy.ReleaseID
		observedTarget := proxy.Target
		compensated := hasProxy && proxy.Compensated
		if hasRecreate {
			observedReleaseID, compensated = recreate.ReleaseID, hasRecreate && recreate.Compensated
			observedTarget = recreate.Target
		}
		state, serving, recovery := releaseMemberTerminalState(
			head,
			member.Ordinal,
			terminalStatus,
			failedOrdinal,
			result,
			compensated,
		)
		var evidence []domain.EffectEvidence
		if serving {
			if hasProxy && (proxy.Compensated || proxy.ReleaseID != intent.ID) ||
				hasRecreate && (recreate.Compensated || recreate.ReleaseID != intent.ID) ||
				!hasProxy && !hasRecreate {
				return false, errs.New(errs.KindStateConflict, "release serving evidence does not match its candidate")
			}
			stepID := task.Steps[index*5+2].ID
			if member.Ordinal == failedOrdinal && terminalStatus != taskjournal.TaskStatusCompleted {
				stepID = result.FailedStepID
			}
			effectDigest, digestErr := domain.Digest(struct {
				ReleaseID string                       `json:"release_id"`
				Result    taskjournal.TaskResultRecord `json:"result"`
			}{intent.ID, result})
			if digestErr != nil {
				return false, digestErr
			}
			effect := domain.EffectEvidence{
				PlanID: task.PlanID, StepID: stepID, AttemptID: task.ID, AgentID: agentID,
				AcknowledgementID: assignment.AssignmentID, ObservedReleaseID: intent.ID,
				ObservedRenderGeneration: uint64(
					task.RenderGeneration,
				), EffectDigest: effectDigest, AcknowledgedAt: terminalAt,
			}
			if hasProxy {
				effect.ObservedSlot, effect.RouterConfigurationDigest, effect.ObservedRouterTarget = domain.WorkloadTarget(proxy.Target).
					Slot(),
					proxy.ConfigSHA256, proxy.Target
			}
			evidence = []domain.EffectEvidence{effect}
		} else if compensated {
			priorReleaseID := intent.PriorServingReleaseID
			if priorReleaseID == "" || observedReleaseID != priorReleaseID {
				return false, errs.New(errs.KindStateConflict, "release compensation evidence does not match prior serving state")
			}
		}
		checkpoint.State = state
		checkpoint.Evidence = evidence
		checkpoint.UpdatedAt = terminalAt
		if err := domain.ValidateCheckpoint(checkpoint); err != nil {
			return false, err
		}
		if serving {
			if projection.EnvironmentID == "" {
				projection.EnvironmentID = task.Owner.EnvironmentID
				projection.ServiceID = member.ServiceID
			}
			projection.ServingReleaseID = intent.ID
			projection.ServingSlot = domain.WorkloadTarget(observedTarget).Slot()
			if !recovery {
				projection.CurrentSuccessfulReleaseID = intent.ID
				projection.ActiveOperationID = ""
			} else {
				projection.ActiveOperationID = task.OperationID
			}
			projection.Revision++
		} else if compensated {
			if projection.EnvironmentID == "" {
				projection.EnvironmentID = task.Owner.EnvironmentID
				projection.ServiceID = member.ServiceID
			}
			projection.ServingReleaseID = intent.PriorServingReleaseID
			projection.CurrentSuccessfulReleaseID = intent.PriorSuccessfulReleaseID
			projection.ServingSlot = domain.WorkloadTarget(observedTarget).Slot()
			projection.ActiveOperationID = ""
			projection.Revision++
		}
		retention := releaseRollbackMaterial(intent, state, terminalAt)
		terminal := releaseTerminalSummary(
			intent,
			state,
			projection.ServingReleaseID,
			evidence,
			head.Attempts,
			retention.Digest,
			terminalAt,
		)
		memberMutations, err := releaseTerminalRecordMutations(keys, checkpoint, terminal, retention,
			projection, serving || compensated)
		if err != nil {
			return false, err
		}
		priorConditions, priorMutations := len(conditions), len(mutations)
		conditions = append(conditions,
			etcdstore.Condition{Key: keys[0], ModRevision: values[0].ModRevision},
			etcdstore.Condition{Key: keys[1], ModRevision: values[1].ModRevision},
			etcdstore.Condition{Key: keys[2], ModRevision: keyValueRevision(values[2])},
			etcdstore.Condition{Key: keys[3]}, etcdstore.Condition{Key: keys[4]},
		)
		mutations = append(mutations, memberMutations...)
		if serving && !recovery && state == domain.StateCompleted {
			runtimeValue, err := releaseAcknowledgedRuntime(
				marker,
				member.ServiceID,
				task,
				assignment,
				evidence[0],
				result,
			)
			if err != nil {
				return false, err
			}
			conditions = append(conditions, etcdstore.Condition{Key: keys[5], ModRevision: keyValueRevision(values[5])})
			mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: keys[5], Value: runtimeValue})
		}
		budget, err := repository.store.MeasureTransaction(
			ctx,
			append(slices.Clone(conditions), proofConditions...),
			mutations,
		)
		if err != nil {
			return false, err
		}
		if !budget.Fits() {
			clearMutations(mutations[priorMutations:])
			conditions, mutations = conditions[:priorConditions], mutations[:priorMutations]
			if priorMutations == 0 {
				return false, errs.New(errs.KindValidationFailed, "release terminal member exceeds transaction budget")
			}
			break
		}
	}
	if len(conditions)+len(proofConditions)+len(mutations) > etcdstore.MaximumOperations {
		return false, errs.New(errs.KindInternal, "release terminal batch exceeds the transaction ceiling")
	}
	transaction, err := repository.store.Transact(ctx, append(conditions, proofConditions...), mutations)
	if err != nil {
		return false, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return false, errs.New(errs.KindStateConflict, "release terminal batch evidence changed")
	}
	return true, nil
}
