package etcd

import (
	"context"
	"encoding/hex"
	"time"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumBlueprintReleaseTerminalBatchMembers = 9

type blueprintReleaseResolvedRecord struct {
	record   ExecutionStepResultRecord
	key      string
	revision int64
}

func (repository *TaskRepository) finalizeBlueprintReleaseTaskBatch(
	ctx context.Context,
	task TaskRecord,
	assignment TaskAssignmentRecord,
	terminalStatus TaskStatus,
	result TaskResultRecord,
	agentID string,
	terminalAt time.Time,
	readRevision int64,
) (bool, error) {
	publicationID := task.Params[TaskReleasePublicationParam]
	baseKeys := []string{
		releasePublicationKey(publicationID), releaseManifestStagingKey(publicationID),
		environmentMutationEpochKey(task.Owner.EnvironmentID),
	}
	base, err := repository.store.GetMany(ctx, GetManyRequest{Keys: baseKeys, Revision: readRevision})
	if err != nil {
		return false, err
	}
	if base == nil || base.ReadRevision != readRevision || len(base.Values) != len(baseKeys) ||
		base.Values[0] == nil || base.Values[1] == nil || base.Values[2] == nil {
		return false, corruptReleaseRecord()
	}
	marker, err := decodeReleaseRecord[ReleasePublicationMarker](base.Values[0].Value, "release-publication")
	if err != nil || marker.PublicationID != publicationID || marker.OperationID != task.OperationID {
		return false, corruptReleaseRecord()
	}
	manifest, err := decodeReleaseRecord[ReleaseStagedManifest](base.Values[1].Value, "release-staged-manifest")
	if err != nil || manifest.PublicationID != publicationID || manifest.OperationID != task.OperationID ||
		manifest.Digest != marker.ManifestDigest || len(manifest.Members) == 0 {
		return false, corruptReleaseRecord()
	}
	terminalKeys := make([]string, len(manifest.Members))
	for index, member := range manifest.Members {
		terminalKeys[index] = releaseTerminalKey(member.ReleaseID)
	}
	terminals, err := repository.store.GetMany(ctx, GetManyRequest{Keys: terminalKeys, Revision: readRevision})
	if err != nil {
		return false, err
	}
	if terminals == nil || terminals.ReadRevision != readRevision || len(terminals.Values) != len(terminalKeys) {
		return false, corruptReleaseRecord()
	}
	pending := make([]int, 0, maximumBlueprintReleaseTerminalBatchMembers)
	for index, value := range terminals.Values {
		if value == nil && len(pending) < maximumBlueprintReleaseTerminalBatchMembers {
			pending = append(pending, index)
		}
	}
	if len(pending) == 0 {
		return false, nil
	}
	resolved, err := repository.blueprintReleaseResolvedRecords(ctx, task.OperationID, task.PlanHash, readRevision)
	if err != nil {
		return false, err
	}
	memberReleaseIDs := make(map[string]struct{}, len(manifest.Members))
	for _, member := range manifest.Members {
		memberReleaseIDs[member.ReleaseID] = struct{}{}
	}
	for releaseID := range resolved {
		if _, ok := memberReleaseIDs[releaseID]; !ok {
			return false, errs.New(errs.KindStateConflict, "Blueprint resolved image does not belong to a candidate Release")
		}
	}
	detailKeys := make([]string, 0, len(pending)*5)
	for _, index := range pending {
		member := manifest.Members[index]
		detailKeys = append(detailKeys,
			releaseIntentStagingKey(publicationID, member.ReleaseID),
			releaseCheckpointStagingKey(publicationID, member.ReleaseID),
			releaseProjectionKey(member.ServiceID),
			releaseTerminalKey(member.ReleaseID),
			releaseRetentionKey(member.ReleaseID),
		)
	}
	details, err := repository.store.GetMany(ctx, GetManyRequest{Keys: detailKeys, Revision: readRevision})
	if err != nil {
		return false, err
	}
	if details == nil || details.ReadRevision != readRevision || len(details.Values) != len(detailKeys) {
		return false, corruptReleaseRecord()
	}
	conditions := []Condition{
		{Key: base.Values[0].Key, ModRevision: base.Values[0].ModRevision},
		{Key: base.Values[1].Key, ModRevision: base.Values[1].ModRevision},
		{Key: base.Values[2].Key, ModRevision: base.Values[2].ModRevision},
	}
	mutations := make([]Mutation, 0, len(pending)*4)
	defer clearMutations(mutations)
	attemptStartedAt := terminalAt
	if task.StartedAt != nil {
		attemptStartedAt = *task.StartedAt
	}
	attempts := []domain.Attempt{{ID: task.ID, TaskID: task.ID, StartedAt: attemptStartedAt}}
	for offset, index := range pending {
		values := details.Values[offset*5 : offset*5+5]
		if values[0] == nil || values[1] == nil || values[3] != nil || values[4] != nil {
			return false, corruptReleaseRecord()
		}
		member := manifest.Members[index]
		intent, decodeErr := decodeReleaseRecord[domain.Intent](values[0].Value, "release-intent")
		intentDigest, _ := domain.Digest(intent)
		if decodeErr != nil || domain.ValidateIntent(intent) != nil || intentDigest != member.IntentDigest ||
			intent.OperationKind != domain.OperationBlueprintApply || intent.ID != member.ReleaseID ||
			intent.ServiceID != member.ServiceID || intent.EnvironmentID != task.Owner.EnvironmentID ||
			intent.OperationID != task.OperationID || intent.OriginatingTaskID != task.ID {
			return false, corruptReleaseRecord()
		}
		checkpoint, decodeErr := decodeReleaseRecord[domain.Checkpoint](values[1].Value, "release-checkpoint")
		checkpointDigest, _ := domain.Digest(checkpoint)
		if decodeErr != nil || domain.ValidateCheckpoint(checkpoint) != nil || checkpointDigest != member.CheckpointDigest ||
			checkpoint.ReleaseID != intent.ID || checkpoint.State != domain.StatePending {
			return false, corruptReleaseRecord()
		}
		projection, decodeErr := decodeReleaseProjection(values[2], task.Owner.EnvironmentID, member.ServiceID)
		if decodeErr != nil {
			return false, decodeErr
		}
		resolvedRecord, hasResolved := resolved[intent.ID]
		var resolvedImage *domain.ResolvedImageEvidence
		if hasResolved {
			wire, wireErr := resolvedRecord.record.Proto()
			if wireErr != nil || wire.GetOperationId() != task.OperationID ||
				hex.EncodeToString(wire.GetPlanHash()) != task.PlanHash || !taskContainsStep(task, wire.GetStepId()) {
				return false, corruptReleaseRecord()
			}
			evidence := wire.GetProcedureServiceImage()
			if evidence.GetReleaseId() != intent.ID || evidence.GetServiceId() != intent.ServiceID ||
				evidence.GetRequestedReference() != intent.Image {
				return false, errs.New(errs.KindStateConflict, "Blueprint resolved image differs from candidate Release")
			}
			value := domain.ResolvedImageEvidence{
				RequestedReference: evidence.GetRequestedReference(), ImmutableReference: evidence.GetImmutableReference(),
				Digest: hex.EncodeToString(evidence.GetImageDigest()), LocalImageID: evidence.GetLocalImageId(),
				ComposeApplyStepID: wire.GetStepId(), ControlPayloadDigest: hex.EncodeToString(wire.GetControlPayloadSha256()),
			}
			if domain.ValidateResolvedImageEvidence(value, intent) != nil {
				return false, corruptReleaseRecord()
			}
			resolvedImage = &value
		}
		successful := terminalStatus == TaskStatusCompleted && !result.ReconciliationRequired
		if successful && intent.Digest == "" && resolvedImage == nil {
			return false, errs.New(errs.KindStateConflict, "Blueprint tagged candidate lacks acknowledged resolved image evidence")
		}
		state := releaseOperationTerminalState(terminalStatus)
		if result.ReconciliationRequired {
			state = domain.StateRecoveryRequired
		}
		checkpoint.State = state
		checkpoint.Evidence = nil
		checkpoint.UpdatedAt = terminalAt
		if successful && resolvedImage != nil {
			effectDigest, digestErr := domain.Digest(*resolvedImage)
			if digestErr != nil {
				return false, digestErr
			}
			checkpoint.Evidence = []domain.EffectEvidence{{
				PlanID: task.PlanID, StepID: resolvedImage.ComposeApplyStepID, AttemptID: task.ID,
				AgentID: agentID, AcknowledgementID: assignment.AssignmentID, ObservedReleaseID: intent.ID,
				ObservedRenderGeneration: uint64(task.RenderGeneration), EffectDigest: effectDigest, AcknowledgedAt: terminalAt,
			}}
		}
		if err := domain.ValidateCheckpoint(checkpoint); err != nil {
			return false, err
		}
		if successful {
			if projection.EnvironmentID == "" {
				projection.EnvironmentID, projection.ServiceID = task.Owner.EnvironmentID, member.ServiceID
			}
			projection.ServingReleaseID = intent.ID
			projection.CurrentSuccessfulReleaseID = intent.ID
			projection.ServingSlot = intent.Slot
			projection.ActiveOperationID = ""
			projection.Revision++
		}
		retention := releaseRollbackMaterial(intent, state, terminalAt)
		if resolvedImage != nil {
			replaced := false
			for index, reference := range retention.References {
				if reference == intent.Image {
					retention.References[index] = resolvedImage.ImmutableReference
					replaced = true
				}
			}
			if !replaced {
				retention.References = append(retention.References, resolvedImage.ImmutableReference)
			}
			retention.Digest, _ = domain.Digest(retention.References)
		}
		terminal := releaseTerminalSummary(intent, state, projection.ServingReleaseID, checkpoint.Evidence, attempts, retention.Digest, terminalAt)
		terminal.ResolvedImage = resolvedImage
		checkpointValue, encodeErr := encodeReleaseRecord("release-checkpoint", checkpoint)
		if encodeErr != nil {
			return false, encodeErr
		}
		terminalValue, encodeErr := encodeReleaseRecord("release-terminal-summary", terminal)
		if encodeErr != nil {
			clear(checkpointValue)
			return false, encodeErr
		}
		retentionValue, encodeErr := encodeReleaseRecord("release-retention", retention)
		if encodeErr != nil {
			clear(checkpointValue)
			clear(terminalValue)
			return false, encodeErr
		}
		conditions = append(conditions,
			Condition{Key: values[0].Key, ModRevision: values[0].ModRevision},
			Condition{Key: values[1].Key, ModRevision: values[1].ModRevision},
			Condition{Key: detailKeys[offset*5+2], ModRevision: keyValueRevision(values[2])},
			Condition{Key: detailKeys[offset*5+3]}, Condition{Key: detailKeys[offset*5+4]},
		)
		if hasResolved {
			conditions = append(conditions, Condition{Key: resolvedRecord.key, ModRevision: resolvedRecord.revision})
		}
		mutations = append(mutations,
			Mutation{Type: MutationPut, Key: values[1].Key, Value: checkpointValue},
			Mutation{Type: MutationPut, Key: detailKeys[offset*5+3], Value: terminalValue},
			Mutation{Type: MutationPut, Key: detailKeys[offset*5+4], Value: retentionValue},
		)
		if successful {
			projectionValue, encodeErr := encodeReleaseRecord("service-release-projection", projection)
			if encodeErr != nil {
				return false, encodeErr
			}
			mutations = append(mutations, Mutation{Type: MutationPut, Key: detailKeys[offset*5+2], Value: projectionValue})
		}
	}
	if len(conditions)+len(mutations) > maximumTransactionOperations {
		return false, errs.New(errs.KindInternal, "Blueprint release terminal batch exceeds the transaction ceiling")
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return false, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return false, errs.New(errs.KindStateConflict, "Blueprint release terminal evidence changed")
	}
	return true, nil
}

func (repository *TaskRepository) blueprintReleaseResolvedRecords(
	ctx context.Context,
	operationID string,
	planHash string,
	revision int64,
) (map[string]blueprintReleaseResolvedRecord, error) {
	read, err := repository.store.Range(ctx, RangeRequest{
		Prefix: executionStepResultPlanPrefix(operationID, planHash), Limit: 257, Revision: revision,
	})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || read.More || len(read.Values) > 256 {
		clearExecutionStepResultValues(read.Values)
		return nil, corruptReleaseRecord()
	}
	defer clearExecutionStepResultValues(read.Values)
	result := make(map[string]blueprintReleaseResolvedRecord, len(read.Values))
	for _, value := range read.Values {
		record, decodeErr := decodeExecutionStepResultRecord(value.Value)
		if decodeErr != nil || record.OperationID != operationID || record.PlanHash != planHash ||
			value.Key != executionStepResultKey(operationID, planHash, record.StepID) || record.ProcedureServiceImage == nil {
			return nil, corruptReleaseRecord()
		}
		releaseID := record.ProcedureServiceImage.ReleaseID
		if _, duplicate := result[releaseID]; duplicate {
			return nil, errs.New(errs.KindStateConflict, "Blueprint Release has duplicate resolved image evidence")
		}
		result[releaseID] = blueprintReleaseResolvedRecord{record: record, key: value.Key, revision: value.ModRevision}
	}
	return result, nil
}
