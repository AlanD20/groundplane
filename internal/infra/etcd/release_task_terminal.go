package etcd

import (
	"context"
	"slices"
	"strconv"
	"time"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
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
	assignment TaskAssignmentRecord,
	terminalStatus TaskStatus,
	result TaskResultRecord,
	agentID string,
	terminalAt time.Time,
	readRevision int64,
	proofConditions ...Condition,
) (bool, error) {
	publicationID := task.Params[TaskReleasePublicationParam]
	if publicationID == "" {
		return false, nil
	}
	if task.Executor != TaskExecutorAgent ||
		(task.Type != TaskDeploy && task.Type != TaskRollback && task.Type != TaskUpdate) ||
		validatePublicationID(publicationID) != nil || task.OperationID == "" || task.RenderGeneration <= 0 {
		return false, corruptReleaseRecord()
	}
	if terminalStatus != TaskStatusCompleted && result.FailedStepID == "" &&
		!unassignedReleaseAbort(task, terminalStatus) &&
		result.Diagnostic != TaskResultDiagnosticTimeoutBeforeEffect {
		return false, errs.New(errs.KindStateConflict, "release failure is missing its failed step identity")
	}
	if task.Type == TaskUpdate {
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
		releasePublicationKey(publicationID), releaseOperationKey(task.OperationID),
		releaseFenceSetKey(task.Owner.EnvironmentID), environmentMutationEpochKey(task.Owner.EnvironmentID),
	}
	base, err := repository.store.GetMany(ctx, GetManyRequest{Keys: baseKeys, Revision: readRevision})
	if err != nil {
		return false, err
	}
	if base == nil || len(base.Values) != len(baseKeys) || base.Values[0] == nil || base.Values[1] == nil ||
		base.Values[3] == nil {
		return false, corruptReleaseRecord()
	}
	marker, err := decodeReleaseRecord[ReleasePublicationMarker](base.Values[0].Value, "release-publication")
	if err != nil || marker.PublicationID != publicationID || marker.OperationID != task.OperationID {
		return false, corruptReleaseRecord()
	}
	head, err := decodeReleaseRecord[ReleaseOperationHead](base.Values[1].Value, "release-operation")
	if err != nil || head.OperationID != task.OperationID || head.PublicationID != publicationID ||
		head.EnvironmentID != task.Owner.EnvironmentID || head.LatestTaskID != task.ID || len(head.Members) == 0 {
		return false, corruptReleaseRecord()
	}
	if head.State.Terminal() || head.State == domain.StateRecoveryRequired {
		if base.Values[2] != nil && head.State != domain.StateRecoveryRequired {
			return false, corruptReleaseRecord()
		}
		return false, repository.validateReleaseTerminalMembers(ctx, task, head, terminalStatus, readRevision)
	}
	if base.Values[2] == nil {
		return false, corruptReleaseRecord()
	}
	fence, err := decodeReleaseRecord[ReleaseFenceSet](base.Values[2].Value, "release-fence-set")
	if err != nil || fence.OperationID != task.OperationID || fence.AttemptTaskID != task.ID ||
		fence.EnvironmentID != task.Owner.EnvironmentID || len(fence.Members) != len(head.Members) {
		return false, corruptReleaseRecord()
	}
	if !result.ReconciliationRequired {
		processed, terminalErr := repository.finalizeReleaseHookExecutionBatch(ctx, task, terminalAt, readRevision)
		if terminalErr != nil || processed {
			return processed, terminalErr
		}
	}
	terminalKeys := make([]string, len(head.Members))
	for index, member := range head.Members {
		terminalKeys[index] = releaseTerminalKey(member.ReleaseID)
	}
	terminalRead, err := repository.store.GetMany(ctx, GetManyRequest{Keys: terminalKeys, Revision: readRevision})
	if err != nil {
		return false, err
	}
	if terminalRead == nil || len(terminalRead.Values) != len(terminalKeys) {
		return false, corruptReleaseRecord()
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
			releaseIntentStagingKey(publicationID, member.ReleaseID),
			releaseCheckpointStagingKey(publicationID, member.ReleaseID),
			releaseProjectionKey(member.ServiceID),
			releaseTerminalKey(member.ReleaseID),
			releaseRetentionKey(member.ReleaseID),
			serviceruntimerecord.Key(member.ServiceID),
		)
	}
	details, err := repository.store.GetMany(ctx, GetManyRequest{Keys: detailKeys, Revision: readRevision})
	if err != nil {
		return false, err
	}
	if details == nil || len(details.Values) != len(detailKeys) {
		return false, corruptReleaseRecord()
	}
	conditions := []Condition{
		{Key: baseKeys[0], ModRevision: base.Values[0].ModRevision},
		{Key: baseKeys[1], ModRevision: base.Values[1].ModRevision},
		{Key: baseKeys[2], ModRevision: base.Values[2].ModRevision},
		{Key: baseKeys[3], ModRevision: base.Values[3].ModRevision},
	}
	mutations := make([]Mutation, 0, len(pending)*4)
	defer func() { clearMutations(mutations) }()
	for offset, index := range pending {
		values := details.Values[offset*6 : offset*6+6]
		keys := detailKeys[offset*6 : offset*6+6]
		if values[0] == nil || values[1] == nil || values[3] != nil || values[4] != nil {
			return false, corruptReleaseRecord()
		}
		member := head.Members[index]
		intent, err := decodeReleaseRecord[domain.Intent](values[0].Value, "release-intent")
		if err != nil || intent.ID != member.ReleaseID || intent.ServiceID != member.ServiceID ||
			intent.OperationID != task.OperationID || domain.ValidateIntent(intent) != nil {
			return false, corruptReleaseRecord()
		}
		checkpoint, err := decodeReleaseRecord[domain.Checkpoint](values[1].Value, "release-checkpoint")
		if err != nil || checkpoint.ReleaseID != intent.ID || domain.ValidateCheckpoint(checkpoint) != nil ||
			checkpoint.State != domain.StatePending {
			return false, corruptReleaseRecord()
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
			if member.Ordinal == failedOrdinal && terminalStatus != TaskStatusCompleted {
				stepID = result.FailedStepID
			}
			effectDigest, digestErr := domain.Digest(struct {
				ReleaseID string           `json:"release_id"`
				Result    TaskResultRecord `json:"result"`
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
			Condition{Key: keys[0], ModRevision: values[0].ModRevision},
			Condition{Key: keys[1], ModRevision: values[1].ModRevision},
			Condition{Key: keys[2], ModRevision: keyValueRevision(values[2])},
			Condition{Key: keys[3]}, Condition{Key: keys[4]},
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
			conditions = append(conditions, Condition{Key: keys[5], ModRevision: keyValueRevision(values[5])})
			mutations = append(mutations, Mutation{Type: MutationPut, Key: keys[5], Value: runtimeValue})
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
	if len(conditions)+len(proofConditions)+len(mutations) > maximumTransactionOperations {
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

func (repository *TaskRepository) closeReleaseOperation(
	ctx context.Context,
	task TaskRecord,
	head ReleaseOperationHead,
	fence ReleaseFenceSet,
	base *GetManyResult,
	terminals *GetManyResult,
	terminalStatus TaskStatus,
	result TaskResultRecord,
	terminalAt time.Time,
	proofConditions ...Condition,
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
	conditions := []Condition{
		{Key: base.Values[0].Key, ModRevision: base.Values[0].ModRevision},
		{Key: base.Values[1].Key, ModRevision: base.Values[1].ModRevision},
		{Key: base.Values[2].Key, ModRevision: base.Values[2].ModRevision},
		{Key: base.Values[3].Key, ModRevision: base.Values[3].ModRevision},
	}
	for _, value := range terminals.Values {
		conditions = append(conditions, Condition{Key: value.Key, ModRevision: value.ModRevision})
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: releaseOperationKey(task.OperationID), Value: headValue},
		{Type: MutationPut, Key: environmentMutationEpochKey(task.Owner.EnvironmentID), Value: epochValue},
	}
	if state != domain.StateRecoveryRequired {
		mutations = append(mutations, Mutation{Type: MutationDelete, Key: releaseFenceSetKey(task.Owner.EnvironmentID)})
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
	terminalStatus TaskStatus,
	revision int64,
) error {
	keys := make([]string, len(head.Members))
	for index, member := range head.Members {
		keys[index] = releaseTerminalKey(member.ReleaseID)
	}
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
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

func releaseFailedMemberOrdinal(task TaskRecord, terminalStatus TaskStatus, result TaskResultRecord) (uint32, error) {
	if terminalStatus == TaskStatusCompleted {
		return 0, nil
	}
	ordinal := releaseFailedOrdinalFromResult(task, result)
	if ordinal == 0 &&
		(result.Diagnostic == TaskResultDiagnosticTimeoutBeforeEffect || unassignedReleaseAbort(task, terminalStatus)) {
		return 1, nil
	}
	if ordinal == 0 {
		return 0, errs.New(errs.KindStateConflict, "release failed step is outside the frozen procedure")
	}
	return ordinal, nil
}

func releaseFailedOrdinalFromResult(task TaskRecord, result TaskResultRecord) uint32 {
	for index, step := range task.Steps {
		if step.ID == result.FailedStepID {
			if value := task.Params[ReleaseHookStepMemberParam(step.ID)]; value != "" {
				ordinal, err := strconv.ParseUint(value, 10, 32)
				if err != nil || ordinal == 0 {
					return 0
				}
				return uint32(ordinal)
			}
			if task.Params[ReleaseHookStepExecutionParam(step.ID)] != "" {
				return 0
			}
			return uint32(index/5 + 1)
		}
	}
	return 0
}

func releaseMemberTerminalState(
	head ReleaseOperationHead,
	ordinal uint32,
	terminalStatus TaskStatus,
	failedOrdinal uint32,
	result TaskResultRecord,
	compensated bool,
) (domain.State, bool, bool) {
	if terminalStatus == TaskStatusCompleted {
		return domain.StateCompleted, true, false
	}
	if compensated {
		return domain.StateFailed, false, false
	}
	if ordinal < failedOrdinal {
		if head.FailurePolicy == domain.OnFailureSwitchBack {
			return domain.StateRecoveryRequired, true, true
		}
		return domain.StateCompleted, true, false
	}
	if ordinal > failedOrdinal {
		return domain.StateAborted, false, false
	}
	if result.ReconciliationRequired {
		return domain.StateRecoveryRequired, false, true
	}
	switch terminalStatus {
	case TaskStatusTimedOut:
		return domain.StateTimedOut, false, false
	case TaskStatusAborted:
		return domain.StateAborted, false, false
	default:
		return domain.StateFailed, false, false
	}
}

func releaseProxyEvidence(result TaskResultRecord, serviceID string) (TaskProxyEvidence, bool) {
	for _, evidence := range result.ProxyEvidence {
		if evidence.ServiceID == serviceID {
			return evidence, true
		}
	}
	return TaskProxyEvidence{}, false
}

func releaseRecreateEvidence(result TaskResultRecord, serviceID string) (TaskRecreateEvidence, bool) {
	for _, evidence := range result.RecreateEvidence {
		if evidence.ServiceID == serviceID {
			return evidence, true
		}
	}
	return TaskRecreateEvidence{}, false
}

func releaseOperationTerminalState(status TaskStatus) domain.State {
	switch status {
	case TaskStatusCompleted:
		return domain.StateCompleted
	case TaskStatusTimedOut:
		return domain.StateTimedOut
	case TaskStatusAborted:
		return domain.StateAborted
	default:
		return domain.StateFailed
	}
}

func releaseRollbackMaterial(intent domain.Intent, state domain.State, terminalAt time.Time) domain.RollbackMaterial {
	references := []string{intent.RenderInputID, intent.CandidateWorkload.LocalImageID}
	digest, _ := domain.Digest(references)
	material := domain.RollbackMaterial{
		ReleaseID: intent.ID, Status: domain.RetentionAvailable,
		References: references, Digest: digest, Revision: 1,
	}
	if state != domain.StateCompleted {
		material.Status = domain.RetentionExpired
		expired := terminalAt
		material.ExpiredAt = &expired
	}
	return material
}

func releaseTerminalSummary(
	intent domain.Intent,
	state domain.State,
	servingReleaseID string,
	evidence []domain.EffectEvidence,
	attempts []domain.Attempt,
	materialDigest string,
	terminalAt time.Time,
) domain.TerminalSummary {
	effectDigests := make([]string, len(evidence))
	for index := range evidence {
		effectDigests[index] = evidence[index].EffectDigest
	}
	attemptIDs := make([]string, len(attempts))
	for index := range attempts {
		attemptIDs[index] = attempts[index].TaskID
	}
	return domain.TerminalSummary{
		ReleaseID: intent.ID, Outcome: state, FinalServingReleaseID: servingReleaseID,
		EffectDigests: effectDigests, AttemptIDs: attemptIDs,
		RollbackMaterialDigest: materialDigest, CompletedAt: terminalAt,
	}
}

func releaseTerminalGroupProgress(
	head ReleaseOperationHead,
	task TaskRecord,
	terminalStatus TaskStatus,
	result TaskResultRecord,
	terminalAt time.Time,
) (domain.GroupProgress, error) {
	executor, err := domain.NewGroupExecutor(domain.GroupManifest{
		OperationID: head.OperationID, ReleaseGroupID: head.ReleaseGroupID, EnvironmentID: head.EnvironmentID,
		FailurePolicy: head.FailurePolicy, Members: head.Members,
		ConfiguredTimeoutSeconds: head.ConfiguredTimeoutSeconds, ComputedBudgetSeconds: head.ComputedBudgetSeconds,
	})
	if err != nil || head.Progress == nil {
		return domain.GroupProgress{}, corruptReleaseRecord()
	}
	progress := *head.Progress
	progress.Results = nil
	progress.NextMemberOrdinal = 1
	progress.Compensating = false
	progress.NextCompensationOrdinal = 0
	failedOrdinal, err := releaseFailedMemberOrdinal(task, terminalStatus, result)
	if err != nil {
		return domain.GroupProgress{}, err
	}
	limit := uint32(len(head.Members))
	if failedOrdinal > 0 {
		limit = failedOrdinal - 1
	}
	for ordinal := uint32(1); ordinal <= limit; ordinal++ {
		member := head.Members[ordinal-1]
		progress, err = executor.RecordServing(progress, domain.MemberResult{
			Ordinal: ordinal, ServiceID: member.ServiceID, ReleaseID: member.ReleaseID,
			Outcome: domain.MemberServing, ServingReleaseID: member.ReleaseID,
		}, terminalAt)
		if err != nil {
			return domain.GroupProgress{}, err
		}
	}
	if terminalStatus == TaskStatusCompleted {
		return executor.Complete(progress, terminalAt)
	}
	member := head.Members[failedOrdinal-1]
	progress, err = executor.RecordFailure(progress, domain.MemberResult{
		Ordinal: failedOrdinal, ServiceID: member.ServiceID, ReleaseID: member.ReleaseID,
		Outcome: domain.MemberFailed, FailureCode: string(terminalStatus), FailureDetail: string(result.Diagnostic),
	}, terminalAt)
	if err != nil || head.FailurePolicy != domain.OnFailureSwitchBack || result.ReconciliationRequired {
		return progress, err
	}
	for ordinal := failedOrdinal - 1; ordinal > 0; ordinal-- {
		member := head.Members[ordinal-1]
		progress, err = executor.RecordCompensation(progress, domain.MemberResult{
			Ordinal: ordinal, ServiceID: member.ServiceID, ReleaseID: member.ReleaseID,
			Outcome: domain.MemberCompensated, CompensationReleaseID: releasePriorServingEvidence(result, member.ServiceID),
		}, terminalAt)
		if err != nil {
			return domain.GroupProgress{}, err
		}
	}
	return progress, nil
}

func releasePriorServingEvidence(result TaskResultRecord, serviceID string) string {
	evidence, found := releaseProxyEvidence(result, serviceID)
	if found {
		return evidence.ReleaseID
	}
	recreate, found := releaseRecreateEvidence(result, serviceID)
	if found {
		return recreate.ReleaseID
	}
	return ""
}
