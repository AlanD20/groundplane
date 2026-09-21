package etcd

import (
	"context"

	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	scriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"slices"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareBlueprintCandidateRetry(
	ctx context.Context,
	source TaskRecord,
	retry TaskRecord,
	revision int64,
) (releaseTaskRetryChange, error) {
	publicationID := source.Params[releaserender.TaskReleasePublicationParam]
	if source.Type != taskjournal.TaskUpdate || retry.Type != taskjournal.TaskUpdate ||
		source.Executor != taskjournal.TaskExecutorAgent || retry.Executor != source.Executor ||
		retry.OperationID != source.OperationID || retry.RetryOf != source.ID ||
		retry.Params[releaserender.TaskReleasePublicationParam] != publicationID ||
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
		rootRead, readErr := repository.store.GetMany(
			ctx,
			etcdstore.GetManyRequest{Keys: []string{rootKey}, Revision: revision},
		)
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
