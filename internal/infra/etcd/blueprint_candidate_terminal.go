package etcd

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"slices"
	"time"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type blueprintCandidateTerminalChange struct {
	applies    bool
	conditions []Condition
	mutations  []Mutation
}

func (change *blueprintCandidateTerminalChange) clear() {
	if change == nil {
		return
	}
	clearMutations(change.mutations)
	change.conditions = nil
	change.mutations = nil
}

type blueprintCandidateTerminalCaptureStore struct {
	taskRepositoryStore
	revision   int64
	called     bool
	conditions []Condition
	mutations  []Mutation
}

func (store *blueprintCandidateTerminalCaptureStore) Transact(
	_ context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	if store.called {
		return TransactionResult{}, errs.New(errs.KindInternal, "Blueprint candidate terminal contribution split across transactions")
	}
	store.called = true
	store.conditions = slices.Clone(conditions)
	store.mutations = cloneBlueprintCandidateMutations(mutations)
	return TransactionResult{Succeeded: true, Revision: store.revision}, nil
}

func cloneBlueprintCandidateMutations(input []Mutation) []Mutation {
	cloned := make([]Mutation, len(input))
	for index, mutation := range input {
		cloned[index] = mutation
		cloned[index].Value = slices.Clone(mutation.Value)
	}
	return cloned
}

func mergeBlueprintCandidateTerminalChange(
	conditions []Condition,
	mutations []Mutation,
	change blueprintCandidateTerminalChange,
) ([]Condition, []Mutation, error) {
	if !change.applies {
		return conditions, mutations, nil
	}
	byKey := make(map[string]Condition, len(conditions)+len(change.conditions))
	for _, condition := range conditions {
		byKey[condition.Key] = condition
	}
	for _, condition := range change.conditions {
		if existing, duplicate := byKey[condition.Key]; duplicate {
			if existing != condition {
				return nil, nil, errs.New(
					errs.KindInternal,
					"Blueprint candidate terminal compare disagrees with another Task contribution",
				)
			}
			continue
		}
		byKey[condition.Key] = condition
		conditions = append(conditions, condition)
	}
	mutations = append(mutations, change.mutations...)
	return conditions, mutations, nil
}

func (repository *TaskRepository) prepareBlueprintCandidateTerminalAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	assignment TaskAssignmentRecord,
	terminalStatus TaskStatus,
	result TaskResultRecord,
	agentID string,
	terminalAt time.Time,
	revision int64,
) (blueprintCandidateTerminalChange, error) {
	publicationID := task.Params[TaskReleasePublicationParam]
	if publicationID == "" || task.Type != TaskUpdate {
		return blueprintCandidateTerminalChange{}, nil
	}
	if task.Executor != TaskExecutorAgent || task.Owner.EnvironmentID == "" ||
		task.Params[TaskMaterializationEnvironmentParam] != task.Owner.EnvironmentID ||
		task.Params[EnvironmentDesiredRevisionParam] == "" {
		return blueprintCandidateTerminalChange{}, corruptReleaseRecord()
	}
	if terminalStatus != TaskStatusCompleted && result.ReconciliationRequired {
		return blueprintCandidateTerminalChange{}, errs.New(
			errs.KindReleaseRecoveryRequired,
			"Blueprint candidate restoration is not yet proven",
		)
	}
	baseConditions, manifest, err := repository.blueprintCandidateAuthority(
		ctx, task, publicationID, revision,
	)
	if err != nil {
		return blueprintCandidateTerminalChange{}, err
	}
	var compensationResult *TaskResultRecord
	if terminalStatus != TaskStatusCompleted {
		if result.FailedStepID == "" || !taskContainsStep(task, result.FailedStepID) {
			return blueprintCandidateTerminalChange{}, errs.New(
				errs.KindReleaseRecoveryRequired,
				"Blueprint failure lacks an exact failed step",
			)
		}
		compensationResult = &result
	}
	candidateConditions, err := repository.validateBlueprintCandidateUnpublished(
		ctx, task, publicationID, manifest, compensationResult, revision,
	)
	if err != nil {
		return blueprintCandidateTerminalChange{}, err
	}
	if terminalStatus != TaskStatusCompleted {
		return blueprintCandidateTerminalChange{
			applies:    true,
			conditions: append(baseConditions, candidateConditions...),
		}, nil
	}
	if result.ReconciliationRequired {
		return blueprintCandidateTerminalChange{}, corruptReleaseRecord()
	}
	capture := &blueprintCandidateTerminalCaptureStore{
		taskRepositoryStore: repository.store,
		revision:            revision,
	}
	capturedRepository := *repository
	capturedRepository.store = capture
	processed, err := capturedRepository.finalizeBlueprintReleaseTaskBatch(
		ctx, task, assignment, terminalStatus, result, agentID, terminalAt, revision,
	)
	if err != nil {
		clearMutations(capture.mutations)
		return blueprintCandidateTerminalChange{}, err
	}
	if !processed || !capture.called {
		clearMutations(capture.mutations)
		return blueprintCandidateTerminalChange{}, corruptReleaseRecord()
	}
	combinedConditions, combinedMutations, err := mergeBlueprintCandidateTerminalChange(
		capture.conditions,
		capture.mutations,
		blueprintCandidateTerminalChange{
			applies:    true,
			conditions: append(baseConditions, candidateConditions...),
		},
	)
	if err != nil {
		clearMutations(capture.mutations)
		return blueprintCandidateTerminalChange{}, err
	}
	return blueprintCandidateTerminalChange{
		applies:    true,
		conditions: combinedConditions,
		mutations:  combinedMutations,
	}, nil
}

func (repository *TaskRepository) blueprintCandidateAuthority(
	ctx context.Context,
	task TaskRecord,
	publicationID string,
	revision int64,
) ([]Condition, ReleaseStagedManifest, error) {
	desiredRevisionID := task.Params[EnvironmentDesiredRevisionParam]
	keys := []string{
		releasePublicationKey(publicationID),
		releaseManifestStagingKey(publicationID),
		environmentMutationEpochKey(task.Owner.EnvironmentID),
		environmentBlueprintHeadKey(task.Owner.EnvironmentID),
		environmentBlueprintRootKey(task.Owner.EnvironmentID, desiredRevisionID),
		environmentComposeProjectionKey(task.Owner.EnvironmentID),
	}
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, ReleaseStagedManifest{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil ||
		read.Values[3] == nil || read.Values[4] == nil {
		return nil, ReleaseStagedManifest{}, corruptReleaseRecord()
	}
	marker, err := decodeReleaseRecord[ReleasePublicationMarker](read.Values[0].Value, "release-publication")
	if err != nil {
		return nil, ReleaseStagedManifest{}, corruptReleaseRecord()
	}
	manifest, err := decodeReleaseRecord[ReleaseStagedManifest](read.Values[1].Value, "release-staged-manifest")
	if err != nil || validateBlueprintCandidateManifest(task, marker, manifest) != nil {
		return nil, ReleaseStagedManifest{}, corruptReleaseRecord()
	}
	headRevisionID, err := decodeTaskReference(read.Values[3].Value)
	if err != nil || headRevisionID != desiredRevisionID {
		return nil, ReleaseStagedManifest{}, errs.New(errs.KindStateConflict, "Blueprint desired head changed")
	}
	seal, err := decodeEnvironmentBlueprintSeal(read.Values[4].Value)
	if err != nil || seal.EnvironmentID != task.Owner.EnvironmentID ||
		seal.RevisionID != desiredRevisionID || seal.SourceKind != EnvironmentBlueprintSourceApply ||
		seal.RenderGeneration != uint64(task.RenderGeneration) {
		return nil, ReleaseStagedManifest{}, corruptReleaseRecord()
	}
	if seal.BaselineHeadRevision == 0 {
		if read.Values[5] != nil {
			return nil, ReleaseStagedManifest{}, errs.New(
				errs.KindStateConflict,
				"Blueprint first-candidate applied absence changed",
			)
		}
	} else {
		if read.Values[5] == nil || read.Values[5].ModRevision != seal.BaselineHeadRevision {
			return nil, ReleaseStagedManifest{}, errs.New(
				errs.KindStateConflict,
				"Blueprint sealed predecessor applied projection changed",
			)
		}
		applied, decodeErr := decodeEnvironmentComposeProjection(read.Values[5].Value)
		if decodeErr != nil || applied.EnvironmentID != task.Owner.EnvironmentID ||
			applied.RevisionID == desiredRevisionID ||
			applied.RenderGeneration >= uint64(task.RenderGeneration) {
			return nil, ReleaseStagedManifest{}, errs.New(
				errs.KindStateConflict,
				"Blueprint candidate is not based on the sealed predecessor projection",
			)
		}
	}
	var attemptConditions []Condition
	if task.RetryOf == "" {
		if read.Values[2].ModRevision != read.Values[0].ModRevision {
			return nil, ReleaseStagedManifest{}, errs.New(
				errs.KindStateConflict,
				"Blueprint publication mutation epoch changed",
			)
		}
	} else {
		_, attemptConditions, err = repository.blueprintCandidateAttempts(ctx, task, seal, revision)
		if err != nil {
			return nil, ReleaseStagedManifest{}, err
		}
		if len(attemptConditions) != 1 || attemptConditions[0].ModRevision != read.Values[2].ModRevision {
			return nil, ReleaseStagedManifest{}, errs.New(
				errs.KindStateConflict,
				"Blueprint retry mutation epoch changed",
			)
		}
	}
	conditions := make([]Condition, len(keys))
	for index, key := range keys {
		conditions[index] = Condition{Key: key, ModRevision: keyValueRevision(read.Values[index])}
	}
	conditions = append(conditions, attemptConditions...)
	return conditions, manifest, nil
}

func (repository *TaskRepository) validateBlueprintCandidateUnpublished(
	ctx context.Context,
	task TaskRecord,
	publicationID string,
	manifest ReleaseStagedManifest,
	compensationResult *TaskResultRecord,
	revision int64,
) ([]Condition, error) {
	keys := make([]string, 0, len(manifest.Members)*6)
	for _, member := range manifest.Members {
		keys = append(keys,
			releaseIntentStagingKey(publicationID, member.ReleaseID),
			releaseRenderInputStagingKey(publicationID, member.ReleaseID),
			releaseCheckpointStagingKey(publicationID, member.ReleaseID),
			releaseProjectionKey(member.ServiceID),
			releaseTerminalKey(member.ReleaseID),
			releaseRetentionKey(member.ReleaseID),
		)
	}
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, corruptReleaseRecord()
	}
	conditions := make([]Condition, 0, len(keys))
	for index, member := range manifest.Members {
		values := read.Values[index*6 : index*6+6]
		if values[0] == nil || values[1] == nil || values[2] == nil || values[4] != nil || values[5] != nil {
			return nil, corruptReleaseRecord()
		}
		intent, decodeErr := decodeReleaseRecord[domain.Intent](values[0].Value, "release-intent")
		intentDigest, _ := domain.Digest(intent)
		if decodeErr != nil || domain.ValidateIntent(intent) != nil ||
			intentDigest != member.IntentDigest || intent.ID != member.ReleaseID ||
			intent.ServiceID != member.ServiceID || intent.EnvironmentID != task.Owner.EnvironmentID ||
			intent.OperationID != task.OperationID || intent.OperationKind != domain.OperationBlueprintApply {
			return nil, corruptReleaseRecord()
		}
		rawRender, decodeErr := decodeReleaseRecord[json.RawMessage](values[1].Value, "release-render-input")
		if decodeErr != nil {
			return nil, decodeErr
		}
		render, decodeErr := decodeReleaseRenderInput(rawRender)
		renderDigest, _ := domain.Digest(rawRender)
		if decodeErr != nil || renderDigest != member.RenderDigest ||
			renderDigest != intent.RenderInputDigest || render.ReleaseID != intent.ID ||
			render.ServiceID != intent.ServiceID || render.PlanID != task.PlanID ||
			render.EnvironmentID != task.Owner.EnvironmentID || render.Strategy != intent.Strategy {
			return nil, corruptReleaseRecord()
		}
		checkpoint, decodeErr := decodeReleaseRecord[domain.Checkpoint](values[2].Value, "release-checkpoint")
		checkpointDigest, _ := domain.Digest(checkpoint)
		if decodeErr != nil || domain.ValidateCheckpoint(checkpoint) != nil ||
			checkpointDigest != member.CheckpointDigest || checkpoint.ReleaseID != member.ReleaseID ||
			checkpoint.State != domain.StatePending {
			return nil, corruptReleaseRecord()
		}
		projection, decodeErr := decodeReleaseProjection(values[3], task.Owner.EnvironmentID, member.ServiceID)
		if decodeErr != nil || projection.ServingReleaseID != intent.PriorServingReleaseID ||
			projection.CurrentSuccessfulReleaseID != intent.PriorSuccessfulReleaseID ||
			projection.ServingReleaseID == member.ReleaseID ||
			projection.CurrentSuccessfulReleaseID == member.ReleaseID {
			return nil, errs.New(errs.KindStateConflict, "Blueprint candidate predecessor Release projection changed")
		}
		if compensationResult != nil {
			if proofErr := validateBlueprintCandidateCompensation(intent, render, *compensationResult); proofErr != nil {
				return nil, proofErr
			}
		}
		for offset, value := range values {
			condition := Condition{Key: keys[index*6+offset], ModRevision: keyValueRevision(value)}
			conditions = append(conditions, condition)
		}
	}
	return conditions, nil
}

func (repository *TaskRepository) validateBlueprintCandidateTerminalReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) error {
	publicationID := task.Params[TaskReleasePublicationParam]
	if publicationID == "" || task.Type != TaskUpdate {
		return nil
	}
	desiredRevisionID := task.Params[EnvironmentDesiredRevisionParam]
	keys := []string{
		releasePublicationKey(publicationID),
		releaseManifestStagingKey(publicationID),
		environmentBlueprintRootKey(task.Owner.EnvironmentID, desiredRevisionID),
	}
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil {
		return corruptReleaseRecord()
	}
	marker, err := decodeReleaseRecord[ReleasePublicationMarker](read.Values[0].Value, "release-publication")
	if err != nil {
		return corruptReleaseRecord()
	}
	manifest, err := decodeReleaseRecord[ReleaseStagedManifest](read.Values[1].Value, "release-staged-manifest")
	if err != nil || validateBlueprintCandidateManifest(task, marker, manifest) != nil {
		return corruptReleaseRecord()
	}
	seal, err := decodeEnvironmentBlueprintSeal(read.Values[2].Value)
	if err != nil || seal.EnvironmentID != task.Owner.EnvironmentID ||
		seal.RevisionID != desiredRevisionID || seal.SourceKind != EnvironmentBlueprintSourceApply ||
		seal.RenderGeneration != uint64(task.RenderGeneration) {
		return corruptReleaseRecord()
	}
	attempts, _, err := repository.blueprintCandidateAttempts(ctx, task, seal, revision)
	if err != nil {
		return err
	}
	expectedAttemptIDs := blueprintAttemptIDs(attempts)
	resolved := map[string]blueprintReleaseResolvedRecord{}
	if terminalStatus == TaskStatusCompleted {
		resolved, err = repository.blueprintReleaseResolvedRecords(ctx, task.OperationID, task.PlanHash, revision)
		if err != nil {
			return err
		}
	}
	detailWidth := 2
	if terminalStatus == TaskStatusCompleted {
		detailWidth = 5
	}
	detailKeys := make([]string, 0, len(manifest.Members)*detailWidth)
	for _, member := range manifest.Members {
		detailKeys = append(detailKeys,
			releaseIntentStagingKey(publicationID, member.ReleaseID),
			releaseRenderInputStagingKey(publicationID, member.ReleaseID),
		)
		if terminalStatus == TaskStatusCompleted {
			detailKeys = append(detailKeys,
				releaseCheckpointStagingKey(publicationID, member.ReleaseID),
				releaseTerminalKey(member.ReleaseID),
				releaseRetentionKey(member.ReleaseID),
			)
		}
	}
	details, err := repository.store.GetMany(ctx, GetManyRequest{Keys: detailKeys, Revision: revision})
	if err != nil {
		return err
	}
	if details == nil || details.ReadRevision != revision || len(details.Values) != len(detailKeys) {
		return corruptReleaseRecord()
	}
	memberReleaseIDs := make(map[string]struct{}, len(manifest.Members))
	for index, member := range manifest.Members {
		memberReleaseIDs[member.ReleaseID] = struct{}{}
		values := details.Values[index*detailWidth : index*detailWidth+detailWidth]
		if values[0] == nil || values[1] == nil {
			return corruptReleaseRecord()
		}
		intent, decodeErr := decodeReleaseRecord[domain.Intent](values[0].Value, "release-intent")
		intentDigest, _ := domain.Digest(intent)
		if decodeErr != nil || domain.ValidateIntent(intent) != nil ||
			intentDigest != member.IntentDigest || intent.ID != member.ReleaseID ||
			intent.ServiceID != member.ServiceID || intent.EnvironmentID != task.Owner.EnvironmentID ||
			intent.OperationID != task.OperationID || intent.OperationKind != domain.OperationBlueprintApply {
			return corruptReleaseRecord()
		}
		rawRender, decodeErr := decodeReleaseRecord[json.RawMessage](values[1].Value, "release-render-input")
		render, renderErr := decodeReleaseRenderInput(rawRender)
		renderDigest, _ := domain.Digest(rawRender)
		if decodeErr != nil || renderErr != nil || renderDigest != member.RenderDigest ||
			renderDigest != intent.RenderInputDigest || render.ReleaseID != intent.ID ||
			render.ServiceID != intent.ServiceID || render.PlanID != task.PlanID ||
			render.EnvironmentID != task.Owner.EnvironmentID || render.Strategy != intent.Strategy {
			return corruptReleaseRecord()
		}
		if terminalStatus != TaskStatusCompleted {
			if task.Result == nil || task.Result.ReconciliationRequired ||
				task.Result.Kind != TaskResultCompose {
				return corruptReleaseRecord()
			}
			if proofErr := validateBlueprintCandidateCompensation(intent, render, *task.Result); proofErr != nil {
				return proofErr
			}
			continue
		}
		if values[2] == nil || values[3] == nil || values[4] == nil {
			return corruptReleaseRecord()
		}
		checkpoint, decodeErr := decodeReleaseRecord[domain.Checkpoint](values[2].Value, "release-checkpoint")
		if decodeErr != nil || domain.ValidateCheckpoint(checkpoint) != nil || checkpoint.ReleaseID != intent.ID {
			return corruptReleaseRecord()
		}
		if task.FinishedAt == nil || checkpoint.State != domain.StateCompleted ||
			task.Result == nil || task.Result.Kind != TaskResultCompose || task.Result.ReconciliationRequired {
			return corruptReleaseRecord()
		}
		terminal, decodeErr := decodeReleaseRecord[domain.TerminalSummary](
			values[3].Value, "release-terminal-summary",
		)
		retention, retentionErr := decodeReleaseRecord[domain.RollbackMaterial](
			values[4].Value, "release-retention",
		)
		expectedEffectDigests := make([]string, len(checkpoint.Evidence))
		for evidenceIndex, evidence := range checkpoint.Evidence {
			expectedEffectDigests[evidenceIndex] = evidence.EffectDigest
		}
		if decodeErr != nil || retentionErr != nil || terminal.ReleaseID != intent.ID ||
			terminal.Outcome != domain.StateCompleted || terminal.FinalServingReleaseID != intent.ID ||
			!reflect.DeepEqual(terminal.EffectDigests, expectedEffectDigests) ||
			!reflect.DeepEqual(terminal.AttemptIDs, expectedAttemptIDs) ||
			terminal.RollbackMaterialDigest != retention.Digest ||
			!terminal.CompletedAt.Equal(*task.FinishedAt) {
			return corruptReleaseRecord()
		}
		resolvedRecord, hasResolved := resolved[intent.ID]
		if !hasResolved {
			if intent.Digest == "" || terminal.ResolvedImage != nil {
				return corruptReleaseRecord()
			}
		} else {
			if terminal.ResolvedImage == nil ||
				domain.ValidateResolvedImageEvidence(*terminal.ResolvedImage, intent) != nil {
				return corruptReleaseRecord()
			}
			wire, wireErr := resolvedRecord.record.Proto()
			evidence := wire.GetProcedureServiceImage()
			if wireErr != nil || wire.GetOperationId() != task.OperationID ||
				hex.EncodeToString(wire.GetPlanHash()) != task.PlanHash ||
				evidence.GetReleaseId() != intent.ID || evidence.GetServiceId() != intent.ServiceID ||
				terminal.ResolvedImage.RequestedReference != evidence.GetRequestedReference() ||
				terminal.ResolvedImage.ImmutableReference != evidence.GetImmutableReference() ||
				terminal.ResolvedImage.Digest != hex.EncodeToString(evidence.GetImageDigest()) ||
				terminal.ResolvedImage.LocalImageID != evidence.GetLocalImageId() ||
				terminal.ResolvedImage.ComposeApplyStepID != wire.GetStepId() ||
				terminal.ResolvedImage.ControlPayloadDigest != hex.EncodeToString(wire.GetControlPayloadSha256()) {
				return corruptReleaseRecord()
			}
		}
		expectedRetention := releaseRollbackMaterial(intent, domain.StateCompleted, *task.FinishedAt)
		if terminal.ResolvedImage != nil {
			replaced := false
			for referenceIndex, reference := range expectedRetention.References {
				if reference == intent.Image {
					expectedRetention.References[referenceIndex] = terminal.ResolvedImage.ImmutableReference
					replaced = true
				}
			}
			if !replaced {
				expectedRetention.References = append(
					expectedRetention.References, terminal.ResolvedImage.ImmutableReference,
				)
			}
			expectedRetention.Digest, _ = domain.Digest(expectedRetention.References)
		}
		if !reflect.DeepEqual(retention, expectedRetention) {
			return corruptReleaseRecord()
		}
	}
	for releaseID := range resolved {
		if _, present := memberReleaseIDs[releaseID]; !present {
			return corruptReleaseRecord()
		}
	}
	return nil
}

func (repository *TaskRepository) prepareBlueprintCandidateRetry(
	ctx context.Context,
	source TaskRecord,
	retry TaskRecord,
	revision int64,
) (releaseTaskRetryChange, error) {
	publicationID := source.Params[TaskReleasePublicationParam]
	if source.Type != TaskUpdate || retry.Type != TaskUpdate ||
		source.Executor != TaskExecutorAgent || retry.Executor != source.Executor ||
		retry.OperationID != source.OperationID || retry.RetryOf != source.ID ||
		retry.Params[TaskReleasePublicationParam] != publicationID ||
		source.Result == nil || source.Result.ReconciliationRequired ||
		(source.Status != TaskStatusFailed && source.Status != TaskStatusAborted && source.Status != TaskStatusTimedOut) {
		return releaseTaskRetryChange{}, errs.New(errs.KindTaskNotRetryable, "Blueprint Task does not own a retryable candidate")
	}
	baseConditions, manifest, err := repository.blueprintCandidateAuthority(ctx, source, publicationID, revision)
	if err != nil {
		return releaseTaskRetryChange{}, err
	}
	candidateConditions, err := repository.validateBlueprintCandidateUnpublished(
		ctx, source, publicationID, manifest, nil, revision,
	)
	if err != nil {
		return releaseTaskRetryChange{}, err
	}
	desiredRevisionID := source.Params[EnvironmentDesiredRevisionParam]
	rootRead, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys:     []string{environmentBlueprintRootKey(source.Owner.EnvironmentID, desiredRevisionID)},
		Revision: revision,
	})
	if err != nil || rootRead == nil || len(rootRead.Values) != 1 || rootRead.Values[0] == nil {
		if err != nil {
			return releaseTaskRetryChange{}, err
		}
		return releaseTaskRetryChange{}, corruptReleaseRecord()
	}
	seal, err := decodeEnvironmentBlueprintSeal(rootRead.Values[0].Value)
	if err != nil {
		return releaseTaskRetryChange{}, err
	}
	attempts, _, err := repository.blueprintCandidateAttempts(ctx, source, seal, revision)
	if err != nil {
		return releaseTaskRetryChange{}, err
	}
	attempts = append(attempts, domain.Attempt{
		ID: retry.ID, TaskID: retry.ID, RetryOf: source.ID, StartedAt: retry.CreatedAt,
	})
	attemptAuthority := blueprintCandidateAttemptAuthorityRecord{
		Schema: 1, TaskID: retry.ID, RetryOf: source.ID,
		OperationID: source.OperationID, PublicationID: publicationID,
		EnvironmentID:           source.Owner.EnvironmentID,
		BaselineAppliedRevision: seal.BaselineHeadRevision,
		Attempts:                attempts,
	}
	if validateBlueprintCandidateAttempts(attemptAuthority, retry, seal) != nil {
		return releaseTaskRetryChange{}, corruptReleaseRecord()
	}
	attemptAuthorityValue, err := encodeEnvelope(
		"blueprint-candidate-attempt-authority", attemptAuthority,
	)
	if err != nil {
		return releaseTaskRetryChange{}, err
	}
	hookTransfer, err := repository.prepareReleaseHookExecutionRetryTransfer(ctx, source, retry, revision)
	if err != nil {
		return releaseTaskRetryChange{}, err
	}
	defer hookTransfer.clear()
	defer clear(attemptAuthorityValue)
	conditions := append(baseConditions, candidateConditions...)
	conditions = append(conditions, hookTransfer.conditions...)
	conditions = append(conditions, Condition{Key: blueprintCandidateAttemptAuthorityKey(retry.ID)})
	steps, err := releaseHookExecutionSteps(source)
	if err != nil {
		return releaseTaskRetryChange{}, err
	}
	if len(steps) != 0 {
		rootKey := scriptSourceRootKey(source.OperationID)
		rootRead, readErr := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{rootKey}, Revision: revision})
		if readErr != nil {
			return releaseTaskRetryChange{}, readErr
		}
		if rootRead == nil || len(rootRead.Values) != 1 || rootRead.Values[0] == nil {
			return releaseTaskRetryChange{}, errs.New(errs.KindScriptRetryUnsafe, "Blueprint Script source authority is unknown")
		}
		root, decodeErr := decodeScriptOperationSourceRoot(rootRead.Values[0].Value)
		if decodeErr != nil || root.OperationID != source.OperationID || root.Phase != "active" ||
			root.ReleasePath != "absent" || root.MembershipCount == 0 || root.MembershipSHA256 == "" {
			return releaseTaskRetryChange{}, errs.New(errs.KindScriptRetryUnsafe, "Blueprint Script source authority is not active")
		}
		conditions = append(conditions, Condition{Key: rootKey, ModRevision: rootRead.Values[0].ModRevision})
	}
	mutations := cloneBlueprintCandidateMutations(hookTransfer.mutations)
	mutations = append(mutations, Mutation{
		Type: MutationPut, Key: blueprintCandidateAttemptAuthorityKey(retry.ID),
		Value: slices.Clone(attemptAuthorityValue),
	})
	epochKey := environmentMutationEpochKey(source.Owner.EnvironmentID)
	for _, condition := range baseConditions {
		if condition.Key == epochKey {
			epochRead, readErr := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{epochKey}, Revision: revision})
			if readErr != nil || epochRead == nil || len(epochRead.Values) != 1 || epochRead.Values[0] == nil {
				clearMutations(mutations)
				if readErr != nil {
					return releaseTaskRetryChange{}, readErr
				}
				return releaseTaskRetryChange{}, corruptReleaseRecord()
			}
			mutations = append(mutations, Mutation{
				Type: MutationPut, Key: epochKey, Value: slices.Clone(epochRead.Values[0].Value),
			})
			break
		}
	}
	return releaseTaskRetryChange{applies: true, conditions: conditions, mutations: mutations}, nil
}
