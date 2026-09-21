package etcd

import (
	"context"
	"encoding/json"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	scriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"slices"
	"time"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type blueprintCandidateTerminalChange struct {
	applies    bool
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
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
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
}

func (store *blueprintCandidateTerminalCaptureStore) Transact(
	_ context.Context,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) (etcdstore.TransactionResult, error) {
	if store.called {
		return etcdstore.TransactionResult{}, errs.New(
			errs.KindInternal,
			"Blueprint candidate terminal contribution split across transactions",
		)
	}
	store.called = true
	store.conditions = slices.Clone(conditions)
	store.mutations = cloneBlueprintCandidateMutations(mutations)
	return etcdstore.TransactionResult{Succeeded: true, Revision: store.revision}, nil
}

func cloneBlueprintCandidateMutations(input []etcdstore.Mutation) []etcdstore.Mutation {
	cloned := make([]etcdstore.Mutation, len(input))
	for index, mutation := range input {
		cloned[index] = mutation
		cloned[index].Value = slices.Clone(mutation.Value)
	}
	return cloned
}

func mergeBlueprintCandidateTerminalChange(
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
	change blueprintCandidateTerminalChange,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	if !change.applies {
		return conditions, mutations, nil
	}
	byKey := make(map[string]etcdstore.Condition, len(conditions)+len(change.conditions))
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
	assignment taskassignments.TaskAssignmentRecord,
	terminalStatus taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	agentID string,
	terminalAt time.Time,
	revision int64,
) (blueprintCandidateTerminalChange, error) {
	publicationID := task.Params[TaskReleasePublicationParam]
	if publicationID == "" || task.Type != taskjournal.TaskUpdate {
		return blueprintCandidateTerminalChange{}, nil
	}
	if task.Executor != taskjournal.TaskExecutorAgent || task.Owner.EnvironmentID == "" ||
		task.Params[taskjournal.TaskMaterializationEnvironmentParam] != task.Owner.EnvironmentID ||
		task.Params[blueprints.EnvironmentDesiredRevisionParam] == "" {
		return blueprintCandidateTerminalChange{}, releases.CorruptReleaseRecord()
	}
	if validateTaskMaterializationWriterForTask(writer, task, task.Owner.EnvironmentID) != nil {
		return blueprintCandidateTerminalChange{}, corruptTaskMaterializationWriter()
	}
	if terminalStatus != taskjournal.TaskStatusCompleted && result.ReconciliationRequired {
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
		return blueprintCandidateTerminalChange{}, taskassignments.CorruptTaskAssignment()
	}
	baseConditions, err = appendBlueprintCandidateCondition(baseConditions, writerCondition)
	if err != nil {
		return blueprintCandidateTerminalChange{}, err
	}
	var compensationResult *taskjournal.TaskResultRecord
	if terminalStatus != taskjournal.TaskStatusCompleted {
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
	if terminalStatus != taskjournal.TaskStatusCompleted {
		return blueprintCandidateTerminalChange{
			applies:    true,
			conditions: append(baseConditions, candidateConditions...),
			mutations:  authorityMutations,
		}, nil
	}
	if result.ReconciliationRequired {
		return blueprintCandidateTerminalChange{}, releases.CorruptReleaseRecord()
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
		return blueprintCandidateTerminalChange{}, releases.CorruptReleaseRecord()
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
	manifest releases.ReleaseStagedManifest,
	compensationResult *taskjournal.TaskResultRecord,
	revision int64,
) ([]etcdstore.Condition, error) {
	keys := make([]string, 0, len(manifest.Members)*6)
	for _, member := range manifest.Members {
		keys = append(keys,
			releases.ReleaseIntentStagingKey(publicationID, member.ReleaseID),
			releases.ReleaseRenderInputStagingKey(publicationID, member.ReleaseID),
			releases.ReleaseCheckpointStagingKey(publicationID, member.ReleaseID),
			releases.ReleaseProjectionKey(member.ServiceID),
			releases.ReleaseTerminalKey(member.ReleaseID),
			releases.ReleaseRetentionKey(member.ReleaseID),
		)
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, releases.CorruptReleaseRecord()
	}
	conditions := make([]etcdstore.Condition, 0, len(keys))
	for index, member := range manifest.Members {
		values := read.Values[index*6 : index*6+6]
		if values[0] == nil || values[1] == nil || values[2] == nil || values[4] != nil || values[5] != nil {
			return nil, releases.CorruptReleaseRecord()
		}
		intent, decodeErr := releases.DecodeReleaseRecord[domain.Intent](values[0].Value, "release-intent")
		intentDigest, _ := domain.Digest(intent)
		if decodeErr != nil || domain.ValidateIntent(intent) != nil ||
			intentDigest != member.IntentDigest || intent.ID != member.ReleaseID ||
			intent.ServiceID != member.ServiceID || intent.EnvironmentID != task.Owner.EnvironmentID ||
			intent.OperationID != task.OperationID || intent.OperationKind != domain.OperationBlueprintApply {
			return nil, releases.CorruptReleaseRecord()
		}
		rawRender, decodeErr := releases.DecodeReleaseRecord[json.RawMessage](values[1].Value, "release-render-input")
		if decodeErr != nil {
			return nil, decodeErr
		}
		render, decodeErr := decodeReleaseRenderInput(rawRender)
		renderDigest, _ := domain.Digest(rawRender)
		if decodeErr != nil || renderDigest != member.RenderDigest ||
			renderDigest != intent.RenderInputDigest || render.ReleaseID != intent.ID ||
			render.ServiceID != intent.ServiceID || render.PlanID != task.PlanID ||
			render.EnvironmentID != task.Owner.EnvironmentID || render.Strategy != intent.Strategy {
			return nil, releases.CorruptReleaseRecord()
		}
		checkpoint, decodeErr := releases.DecodeReleaseRecord[domain.Checkpoint](values[2].Value, "release-checkpoint")
		checkpointDigest, _ := domain.Digest(checkpoint)
		if decodeErr != nil || domain.ValidateCheckpoint(checkpoint) != nil ||
			checkpointDigest != member.CheckpointDigest || checkpoint.ReleaseID != member.ReleaseID ||
			checkpoint.State != domain.StatePending {
			return nil, releases.CorruptReleaseRecord()
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
			condition := etcdstore.Condition{Key: keys[index*6+offset], ModRevision: keyValueRevision(value)}
			conditions = append(conditions, condition)
		}
	}
	return conditions, nil
}

func (repository *TaskRepository) validateBlueprintCandidateTerminalReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) error {
	publicationID := task.Params[TaskReleasePublicationParam]
	if publicationID == "" || task.Type != taskjournal.TaskUpdate {
		return nil
	}
	desiredRevisionID := task.Params[blueprints.EnvironmentDesiredRevisionParam]
	keys := []string{
		releases.ReleasePublicationKey(publicationID),
		releases.ReleaseManifestStagingKey(publicationID),
		blueprints.EnvironmentBlueprintRootKey(task.Owner.EnvironmentID, desiredRevisionID),
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil {
		return releases.CorruptReleaseRecord()
	}
	marker, err := releases.DecodeReleaseRecord[releases.ReleasePublicationMarker](read.Values[0].Value, "release-publication")
	if err != nil {
		return releases.CorruptReleaseRecord()
	}
	manifest, err := releases.DecodeReleaseRecord[releases.ReleaseStagedManifest](read.Values[1].Value, "release-staged-manifest")
	if err != nil || validateBlueprintCandidateManifest(task, marker, manifest) != nil {
		return releases.CorruptReleaseRecord()
	}
	if _, err := validateReleaseCandidateMarker(task, marker, manifest); err != nil {
		return releases.CorruptReleaseRecord()
	}
	seal, err := blueprints.DecodeEnvironmentBlueprintSeal(read.Values[2].Value)
	if err != nil || seal.EnvironmentID != task.Owner.EnvironmentID ||
		seal.RevisionID != desiredRevisionID || seal.SourceKind != blueprints.EnvironmentBlueprintSourceApply ||
		seal.RenderGeneration != uint64(task.RenderGeneration) {
		return releases.CorruptReleaseRecord()
	}
	attempts, _, err := repository.blueprintCandidateAttempts(ctx, task, seal, revision)
	if err != nil {
		return err
	}
	if task.RetryOf == "" {
		authority, _, authorityErr := repository.blueprintCandidateAttemptAuthority(ctx, task, revision)
		if authorityErr != nil || validateBlueprintCandidateAttempts(authority, task, seal) != nil {
			return releases.CorruptReleaseRecord()
		}
		attempts = slices.Clone(authority.Attempts)
	}
	expectedAttemptIDs := blueprintAttemptIDs(attempts)
	detailWidth := 2
	if terminalStatus == taskjournal.TaskStatusCompleted {
		detailWidth = 5
	}
	detailKeys := make([]string, 0, len(manifest.Members)*detailWidth)
	for _, member := range manifest.Members {
		detailKeys = append(detailKeys,
			releases.ReleaseIntentStagingKey(publicationID, member.ReleaseID),
			releases.ReleaseRenderInputStagingKey(publicationID, member.ReleaseID),
		)
		if terminalStatus == taskjournal.TaskStatusCompleted {
			detailKeys = append(detailKeys,
				releases.ReleaseCheckpointStagingKey(publicationID, member.ReleaseID),
				releases.ReleaseTerminalKey(member.ReleaseID),
				releases.ReleaseRetentionKey(member.ReleaseID),
			)
		}
	}
	details, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: detailKeys, Revision: revision})
	if err != nil {
		return err
	}
	if details == nil || details.ReadRevision != revision || len(details.Values) != len(detailKeys) {
		return releases.CorruptReleaseRecord()
	}
	for index, member := range manifest.Members {
		values := details.Values[index*detailWidth : index*detailWidth+detailWidth]
		if values[0] == nil || values[1] == nil {
			return releases.CorruptReleaseRecord()
		}
		intent, decodeErr := releases.DecodeReleaseRecord[domain.Intent](values[0].Value, "release-intent")
		intentDigest, _ := domain.Digest(intent)
		if decodeErr != nil || domain.ValidateIntent(intent) != nil ||
			intentDigest != member.IntentDigest || intent.ID != member.ReleaseID ||
			intent.ServiceID != member.ServiceID || intent.EnvironmentID != task.Owner.EnvironmentID ||
			intent.OperationID != task.OperationID || intent.OperationKind != domain.OperationBlueprintApply {
			return releases.CorruptReleaseRecord()
		}
		rawRender, decodeErr := releases.DecodeReleaseRecord[json.RawMessage](values[1].Value, "release-render-input")
		render, renderErr := decodeReleaseRenderInput(rawRender)
		renderDigest, _ := domain.Digest(rawRender)
		if decodeErr != nil || renderErr != nil || renderDigest != member.RenderDigest ||
			renderDigest != intent.RenderInputDigest || render.ReleaseID != intent.ID ||
			render.ServiceID != intent.ServiceID || render.PlanID != task.PlanID ||
			render.EnvironmentID != task.Owner.EnvironmentID || render.Strategy != intent.Strategy {
			return releases.CorruptReleaseRecord()
		}
		if terminalStatus != taskjournal.TaskStatusCompleted {
			if task.Result == nil || task.Result.ReconciliationRequired ||
				task.Result.Kind != taskjournal.TaskResultCompose {
				return releases.CorruptReleaseRecord()
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
			return releases.CorruptReleaseRecord()
		}
		checkpoint, decodeErr := releases.DecodeReleaseRecord[domain.Checkpoint](values[2].Value, "release-checkpoint")
		if decodeErr != nil || domain.ValidateCheckpoint(checkpoint) != nil || checkpoint.ReleaseID != intent.ID {
			return releases.CorruptReleaseRecord()
		}
		if task.FinishedAt == nil || checkpoint.State != domain.StateCompleted ||
			task.Result == nil || task.Result.Kind != taskjournal.TaskResultCompose || task.Result.ReconciliationRequired {
			return releases.CorruptReleaseRecord()
		}
		terminal, decodeErr := releases.DecodeReleaseRecord[domain.TerminalSummary](
			values[3].Value, "release-terminal-summary",
		)
		retention, retentionErr := releases.DecodeReleaseRecord[domain.RollbackMaterial](
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
			return releases.CorruptReleaseRecord()
		}
		expectedRetention := releaseRollbackMaterial(intent, domain.StateCompleted, *task.FinishedAt)
		if !blueprintCompletedRollbackMaterialEqual(retention, expectedRetention) {
			return releases.CorruptReleaseRecord()
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
	if source.Type != taskjournal.TaskUpdate || retry.Type != taskjournal.TaskUpdate ||
		source.Executor != taskjournal.TaskExecutorAgent || retry.Executor != source.Executor ||
		retry.OperationID != source.OperationID || retry.RetryOf != source.ID ||
		retry.Params[TaskReleasePublicationParam] != publicationID ||
		source.Result == nil || source.Result.ReconciliationRequired ||
		(source.Status != taskjournal.TaskStatusFailed && source.Status != taskjournal.TaskStatusAborted && source.Status != taskjournal.TaskStatusTimedOut) {
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
		return releaseTaskRetryChange{}, releases.CorruptReleaseRecord()
	}
	for _, member := range procedure.GetMembers() {
		if member.GetServingPredecessor() == nil || member.GetCandidateAbsence() == nil {
			return releaseTaskRetryChange{}, releases.CorruptReleaseRecord()
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
	desiredRevisionID := source.Params[blueprints.EnvironmentDesiredRevisionParam]
	rootRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{blueprints.EnvironmentBlueprintRootKey(source.Owner.EnvironmentID, desiredRevisionID)},
		Revision: revision,
	})
	if err != nil || rootRead == nil || len(rootRead.Values) != 1 || rootRead.Values[0] == nil {
		if err != nil {
			return releaseTaskRetryChange{}, err
		}
		return releaseTaskRetryChange{}, releases.CorruptReleaseRecord()
	}
	seal, err := blueprints.DecodeEnvironmentBlueprintSeal(rootRead.Values[0].Value)
	if err != nil {
		return releaseTaskRetryChange{}, err
	}
	if validateBlueprintCandidateAttempts(sourceAuthority, source, seal) != nil {
		return releaseTaskRetryChange{}, releases.CorruptReleaseRecord()
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
		return releaseTaskRetryChange{}, releases.CorruptReleaseRecord()
	}
	attemptAuthorityValue, err := recordcodec.Encode(
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
	conditions = append(conditions, etcdstore.Condition{Key: blueprintCandidateAttemptAuthorityKey(retry.ID)})
	steps, err := releaseHookExecutionSteps(source)
	if err != nil {
		return releaseTaskRetryChange{}, err
	}
	if len(steps) != 0 {
		rootKey := scriptsourceevidence.ScriptSourceRootKey(source.OperationID)
		rootRead, readErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{rootKey}, Revision: revision})
		if readErr != nil {
			return releaseTaskRetryChange{}, readErr
		}
		if rootRead == nil || len(rootRead.Values) != 1 || rootRead.Values[0] == nil {
			return releaseTaskRetryChange{}, errs.New(
				errs.KindScriptRetryUnsafe,
				"Blueprint Script source authority is unknown",
			)
		}
		root, decodeErr := scriptsourceevidence.DecodeScriptOperationSourceRoot(rootRead.Values[0].Value)
		if decodeErr != nil || root.OperationID != source.OperationID || root.Phase != "active" ||
			root.ReleasePath != "absent" || root.MembershipCount == 0 || root.MembershipSHA256 == "" {
			return releaseTaskRetryChange{}, errs.New(
				errs.KindScriptRetryUnsafe,
				"Blueprint Script source authority is not active",
			)
		}
		conditions = append(conditions, etcdstore.Condition{Key: rootKey, ModRevision: rootRead.Values[0].ModRevision})
	}
	mutations := cloneBlueprintCandidateMutations(hookTransfer.mutations)
	mutations = append(mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: blueprintCandidateAttemptAuthorityKey(retry.ID),
		Value: slices.Clone(attemptAuthorityValue),
	})
	mutations = append(mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentMutationEpochKey(source.Owner.EnvironmentID),
		Value: slices.Clone(epochValue),
	})
	return releaseTaskRetryChange{applies: true, conditions: conditions, mutations: mutations}, nil
}
