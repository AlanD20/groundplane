package etcd

import (
	"context"
	"encoding/json"
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
		return TransactionResult{}, errs.New(
			errs.KindInternal,
			"Blueprint candidate terminal contribution split across transactions",
		)
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
	writer taskMaterializationWriterRecord,
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
	if validateTaskMaterializationWriterForTask(writer, task, task.Owner.EnvironmentID) != nil {
		return blueprintCandidateTerminalChange{}, corruptTaskMaterializationWriter()
	}
	if terminalStatus != TaskStatusCompleted && result.ReconciliationRequired {
		return blueprintCandidateTerminalChange{}, errs.New(errs.KindReleaseRecoveryRequired,
			"Blueprint candidate restoration is not yet proven")
	}
	writerCondition, err := repository.blueprintCandidateLiveWriterAuthority(ctx, task, writer, revision)
	if err != nil {
		return blueprintCandidateTerminalChange{}, err
	}
	authority, err := repository.readBlueprintCandidateAuthority(
		ctx, task, publicationID, *writer.BlueprintAppliedPredecessor, revision,
	)
	if err != nil {
		return blueprintCandidateTerminalChange{}, err
	}
	defer clear(authority.epochValue)
	baseConditions, manifest, epochValue := authority.conditions, authority.manifest, authority.epochValue
	_, procedure, err := repository.candidateReleaseDescriptorAtRevision(ctx, task, revision)
	if err != nil || validateAssignmentRestorationDescriptor(task, assignment, procedure) != nil {
		return blueprintCandidateTerminalChange{}, corruptTaskAssignment()
	}
	baseConditions, err = appendBlueprintCandidateCondition(baseConditions, writerCondition)
	if err != nil {
		return blueprintCandidateTerminalChange{}, err
	}
	var compensationResult *TaskResultRecord
	if terminalStatus != TaskStatusCompleted {
		compensationResult, err = blueprintCandidateCompensationResult(task, result)
		if err != nil {
			return blueprintCandidateTerminalChange{}, err
		}
	}
	candidateConditions, err := repository.validateBlueprintCandidateUnpublished(
		ctx, task, publicationID, manifest, compensationResult, revision,
	)
	if err != nil {
		return blueprintCandidateTerminalChange{}, err
	}
	authorityConditions, authorityMutations, err := repository.prepareBlueprintCandidateTerminalAuthority(
		ctx, task, writer, epochValue, revision,
	)
	if err != nil {
		return blueprintCandidateTerminalChange{}, err
	}
	baseConditions = append(baseConditions, authorityConditions...)
	if terminalStatus != TaskStatusCompleted {
		return blueprintCandidateTerminalChange{
			applies:    true,
			conditions: append(baseConditions, candidateConditions...),
			mutations:  authorityMutations,
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
		append(capture.mutations, authorityMutations...),
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
	if _, err := validateReleaseCandidateMarker(task, marker, manifest); err != nil {
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
	if task.RetryOf == "" {
		authority, _, authorityErr := repository.blueprintCandidateAttemptAuthority(ctx, task, revision)
		if authorityErr != nil || validateBlueprintCandidateAttempts(authority, task, seal) != nil {
			return corruptReleaseRecord()
		}
		attempts = slices.Clone(authority.Attempts)
	}
	expectedAttemptIDs := blueprintAttemptIDs(attempts)
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
	for index, member := range manifest.Members {
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
			compensationResult, proofErr := blueprintCandidateCompensationResult(task, *task.Result)
			if proofErr != nil {
				return proofErr
			}
			if compensationResult != nil {
				if proofErr := validateBlueprintCandidateCompensation(intent, render, *compensationResult); proofErr != nil {
					return proofErr
				}
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
			!sameBlueprintAttachStrings(terminal.EffectDigests, expectedEffectDigests) ||
			!sameBlueprintAttachStrings(terminal.AttemptIDs, expectedAttemptIDs) ||
			terminal.RollbackMaterialDigest != retention.Digest ||
			!terminal.CompletedAt.Equal(*task.FinishedAt) {
			return corruptReleaseRecord()
		}
		expectedRetention := releaseRollbackMaterial(intent, domain.StateCompleted, *task.FinishedAt)
		if !blueprintCompletedRollbackMaterialEqual(retention, expectedRetention) {
			return corruptReleaseRecord()
		}
	}
	return nil
}
func blueprintCompletedRollbackMaterialEqual(left, right domain.RollbackMaterial) bool {
	return left.ReleaseID == right.ReleaseID && left.Status == right.Status && left.Digest == right.Digest &&
		left.Revision == right.Revision && sameBlueprintAttachStrings(left.References, right.References) &&
		left.ExpiredAt == nil && right.ExpiredAt == nil
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
		return releaseTaskRetryChange{}, errs.New(
			errs.KindTaskNotRetryable,
			"Blueprint Task does not own a retryable candidate",
		)
	}
	sourceAuthority, sourceAuthorityCondition, err := repository.blueprintCandidateAttemptAuthority(
		ctx,
		source,
		revision,
	)
	if err != nil {
		return releaseTaskRetryChange{}, err
	}
	authority, err := repository.readBlueprintCandidateAuthority(
		ctx, source, publicationID, sourceAuthority.AppliedPredecessor, revision,
	)
	if err != nil {
		return releaseTaskRetryChange{}, err
	}
	defer clear(authority.epochValue)
	if err := authority.validateRetry(source, sourceAuthorityCondition.ModRevision); err != nil {
		return releaseTaskRetryChange{}, err
	}
	baseConditions, manifest, epochValue := authority.conditions, authority.manifest, authority.epochValue
	descriptor, procedure, err := repository.candidateReleaseDescriptorAtRevision(ctx, source, revision)
	if err != nil {
		return releaseTaskRetryChange{}, corruptReleaseRecord()
	}
	for _, member := range procedure.GetMembers() {
		if member.GetServingPredecessor() == nil || member.GetCandidateAbsence() == nil {
			return releaseTaskRetryChange{}, corruptReleaseRecord()
		}
	}
	if _, err := validateReleaseCandidateDescriptor(descriptor, retry, manifest); err != nil {
		return releaseTaskRetryChange{}, err
	}
	baseConditions, err = appendBlueprintCandidateCondition(baseConditions, sourceAuthorityCondition)
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
	if validateBlueprintCandidateAttempts(sourceAuthority, source, seal) != nil {
		return releaseTaskRetryChange{}, corruptReleaseRecord()
	}
	attempts := slices.Clone(sourceAuthority.Attempts)
	attempts = append(attempts, domain.Attempt{
		ID: retry.ID, TaskID: retry.ID, RetryOf: source.ID, StartedAt: retry.CreatedAt,
	})
	attemptAuthority := blueprintCandidateAttemptAuthorityRecord{
		Schema: 2, TaskID: retry.ID, RetryOf: source.ID,
		OperationID: source.OperationID, PublicationID: publicationID,
		EnvironmentID:      source.Owner.EnvironmentID,
		AppliedPredecessor: sourceAuthority.AppliedPredecessor,
		Attempts:           attempts,
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
			return releaseTaskRetryChange{}, errs.New(
				errs.KindScriptRetryUnsafe,
				"Blueprint Script source authority is unknown",
			)
		}
		root, decodeErr := decodeScriptOperationSourceRoot(rootRead.Values[0].Value)
		if decodeErr != nil || root.OperationID != source.OperationID || root.Phase != "active" ||
			root.ReleasePath != "absent" || root.MembershipCount == 0 || root.MembershipSHA256 == "" {
			return releaseTaskRetryChange{}, errs.New(
				errs.KindScriptRetryUnsafe,
				"Blueprint Script source authority is not active",
			)
		}
		conditions = append(conditions, Condition{Key: rootKey, ModRevision: rootRead.Values[0].ModRevision})
	}
	mutations := cloneBlueprintCandidateMutations(hookTransfer.mutations)
	mutations = append(mutations, Mutation{
		Type: MutationPut, Key: blueprintCandidateAttemptAuthorityKey(retry.ID),
		Value: slices.Clone(attemptAuthorityValue),
	})
	mutations = append(mutations, Mutation{
		Type: MutationPut, Key: environmentMutationEpochKey(source.Owner.EnvironmentID),
		Value: slices.Clone(epochValue),
	})
	return releaseTaskRetryChange{applies: true, conditions: conditions, mutations: mutations}, nil
}
