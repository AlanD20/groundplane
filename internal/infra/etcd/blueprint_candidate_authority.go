package etcd

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const blueprintCandidateAttemptAuthorityPrefix = "/v1/runtime/blueprint-candidate-attempts/"

type blueprintCandidateAttemptAuthorityRecord struct {
	Schema             int                                   `json:"schema"`
	TaskID             string                                `json:"task_id"`
	RetryOf            string                                `json:"retry_of,omitempty"`
	OperationID        string                                `json:"operation_id"`
	PublicationID      string                                `json:"publication_id"`
	EnvironmentID      string                                `json:"environment_id"`
	AppliedPredecessor taskMaterializationAppliedPredecessor `json:"applied_predecessor"`
	Attempts           []domain.Attempt                      `json:"attempts"`
}

func blueprintCandidateAttemptAuthorityKey(taskID string) string {
	return blueprintCandidateAttemptAuthorityPrefix + taskID
}

func blueprintCandidateManifestDigest(manifest ReleaseStagedManifest) (string, error) {
	return domain.Digest(struct {
		PublicationID string                   `json:"publication_id"`
		OperationID   string                   `json:"operation_id"`
		Members       []ReleaseStagedMemberRef `json:"members"`
	}{manifest.PublicationID, manifest.OperationID, manifest.Members})
}

func validateBlueprintCandidateManifest(
	task TaskRecord,
	marker ReleasePublicationMarker,
	manifest ReleaseStagedManifest,
) error {
	publicationID := task.Params[TaskReleasePublicationParam]
	if validatePublicationID(publicationID) != nil || marker.PublicationID != publicationID ||
		marker.OperationID != task.OperationID || manifest.PublicationID != publicationID ||
		manifest.OperationID != task.OperationID || marker.ManifestDigest != manifest.Digest ||
		marker.PublishedAt.IsZero() || marker.PublishedAt.Location() != time.UTC ||
		manifest.CreatedAt.IsZero() || manifest.CreatedAt.Location() != time.UTC ||
		len(manifest.Members) == 0 || len(manifest.Members) > maximumReleasePublicationMembers {
		return corruptReleaseRecord()
	}
	digest, err := blueprintCandidateManifestDigest(manifest)
	if err != nil || digest != manifest.Digest {
		return corruptReleaseRecord()
	}
	releases := make(map[string]struct{}, len(manifest.Members))
	services := make(map[string]struct{}, len(manifest.Members))
	for _, member := range manifest.Members {
		if ids.Validate(ids.KindDeployment, member.ReleaseID) != nil ||
			ids.Validate(ids.KindService, member.ServiceID) != nil ||
			!validSHA256(member.IntentDigest) || !validSHA256(member.RenderDigest) ||
			!validSHA256(member.CheckpointDigest) {
			return corruptReleaseRecord()
		}
		if _, duplicate := releases[member.ReleaseID]; duplicate {
			return corruptReleaseRecord()
		}
		if _, duplicate := services[member.ServiceID]; duplicate {
			return corruptReleaseRecord()
		}
		releases[member.ReleaseID] = struct{}{}
		services[member.ServiceID] = struct{}{}
	}
	return nil
}

func validateBlueprintCandidateCompensation(
	intent domain.Intent,
	render ReleaseRenderInput,
	result TaskResultRecord,
) error {
	proxy, hasProxy := releaseProxyEvidence(result, intent.ServiceID)
	recreate, hasRecreate := releaseRecreateEvidence(result, intent.ServiceID)
	priorReleaseID := intent.PriorServingReleaseID
	if priorReleaseID == "" {
		priorReleaseID = "baseline"
	}
	switch intent.Strategy {
	case domain.StrategyBlueGreen:
		if !hasProxy || hasRecreate || !proxy.Compensated ||
			proxy.ReleaseID != priorReleaseID || proxy.Target != string(render.PriorTarget) ||
			proxy.ProxyGeneration != render.PriorProxyGeneration ||
			proxy.ConfigSHA256 != render.PriorProxyDigest {
			return errs.New(errs.KindReleaseRecoveryRequired, "Blueprint proxy predecessor restoration is unproven")
		}
	case domain.StrategyRecreate:
		if hasProxy || !hasRecreate || !recreate.Compensated ||
			recreate.ReleaseID != priorReleaseID || recreate.Target != string(render.PriorTarget) ||
			recreate.ArtifactID != render.PriorArtifactID {
			return errs.New(errs.KindReleaseRecoveryRequired, "Blueprint recreate predecessor restoration is unproven")
		}
	default:
		return corruptReleaseRecord()
	}
	return nil
}

func validateBlueprintCandidateAttempts(
	record blueprintCandidateAttemptAuthorityRecord,
	task TaskRecord,
	seal EnvironmentBlueprintSeal,
) error {
	if seal.EnvironmentID != task.Owner.EnvironmentID ||
		seal.RevisionID != task.Params[EnvironmentDesiredRevisionParam] ||
		seal.RenderGeneration != uint64(task.RenderGeneration) ||
		validateBlueprintCandidateAttemptAuthorityRecord(record, task) != nil {
		return corruptReleaseRecord()
	}
	return nil
}

func validateBlueprintCandidateAttemptAuthorityRecord(
	record blueprintCandidateAttemptAuthorityRecord,
	task TaskRecord,
) error {
	if record.Schema != 2 || record.TaskID != task.ID || record.RetryOf != task.RetryOf ||
		record.OperationID != task.OperationID ||
		record.PublicationID != task.Params[TaskReleasePublicationParam] ||
		record.EnvironmentID != task.Owner.EnvironmentID ||
		len(record.Attempts) == 0 || len(record.Attempts) > 33 ||
		(task.RetryOf == "" && len(record.Attempts) != 1) ||
		(task.RetryOf != "" && len(record.Attempts) < 2) ||
		validateTaskMaterializationWriter(taskMaterializationWriter(
			task, task.Owner.EnvironmentID, &record.AppliedPredecessor,
		)) != nil {
		return corruptReleaseRecord()
	}
	for index, attempt := range record.Attempts {
		if ids.Validate(ids.KindTask, attempt.ID) != nil || attempt.TaskID != attempt.ID ||
			attempt.StartedAt.IsZero() || attempt.StartedAt.Location() != time.UTC ||
			(index == 0 && attempt.RetryOf != "") ||
			(index > 0 && attempt.RetryOf != record.Attempts[index-1].TaskID) {
			return corruptReleaseRecord()
		}
	}
	last := record.Attempts[len(record.Attempts)-1]
	if last.TaskID != task.ID || last.RetryOf != task.RetryOf {
		return corruptReleaseRecord()
	}
	return nil
}

func (repository *TaskRepository) blueprintCandidateAttemptAuthority(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (blueprintCandidateAttemptAuthorityRecord, Condition, error) {
	key := blueprintCandidateAttemptAuthorityKey(task.ID)
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return blueprintCandidateAttemptAuthorityRecord{}, Condition{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return blueprintCandidateAttemptAuthorityRecord{}, Condition{}, corruptReleaseRecord()
	}
	record, err := decodeEnvelope[blueprintCandidateAttemptAuthorityRecord](
		read.Values[0].Value, "blueprint-candidate-attempt-authority",
	)
	if err != nil || validateBlueprintCandidateAttemptAuthorityRecord(record, task) != nil {
		return blueprintCandidateAttemptAuthorityRecord{}, Condition{}, corruptReleaseRecord()
	}
	return record, Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}

func blueprintCandidateShouldAdvanceEpoch(
	task TaskRecord,
	environmentID string,
	mutations []Mutation,
) (bool, error) {
	if !taskHasBlueprintCandidateAppliedAuthority(task) {
		return true, nil
	}
	epochKey := environmentMutationEpochKey(environmentID)
	found := false
	for _, mutation := range mutations {
		if mutation.Key != epochKey {
			continue
		}
		if found || mutation.Type != MutationPut {
			return false, errs.New(errs.KindInternal, "Blueprint terminal epoch mutation is invalid")
		}
		found = true
	}
	return !found, nil
}

func (repository *TaskRepository) prepareBlueprintCandidateTerminalAuthority(
	ctx context.Context,
	task TaskRecord,
	writer taskMaterializationWriterRecord,
	epochValue []byte,
	revision int64,
) ([]Condition, []Mutation, error) {
	var authority blueprintCandidateAttemptAuthorityRecord
	var conditions []Condition
	if task.RetryOf != "" {
		stored, condition, err := repository.blueprintCandidateAttemptAuthority(ctx, task, revision)
		if err != nil || writer.BlueprintAppliedPredecessor == nil ||
			stored.AppliedPredecessor != *writer.BlueprintAppliedPredecessor {
			if err != nil {
				return nil, nil, err
			}
			return nil, nil, corruptReleaseRecord()
		}
		authority = stored
		conditions = []Condition{condition}
	} else {
		startedAt := task.CreatedAt
		if task.StartedAt != nil {
			startedAt = *task.StartedAt
		}
		authority = blueprintCandidateAttemptAuthorityRecord{
			Schema: 2, TaskID: task.ID, OperationID: task.OperationID,
			PublicationID: task.Params[TaskReleasePublicationParam], EnvironmentID: task.Owner.EnvironmentID,
			AppliedPredecessor: *writer.BlueprintAppliedPredecessor,
			Attempts:           []domain.Attempt{{ID: task.ID, TaskID: task.ID, StartedAt: startedAt}},
		}
		conditions = []Condition{{Key: blueprintCandidateAttemptAuthorityKey(task.ID)}}
	}
	if validateBlueprintCandidateAttemptAuthorityRecord(authority, task) != nil {
		return nil, nil, corruptReleaseRecord()
	}
	value, err := encodeEnvelope("blueprint-candidate-attempt-authority", authority)
	if err != nil {
		return nil, nil, err
	}
	key := blueprintCandidateAttemptAuthorityKey(task.ID)
	return conditions, []Mutation{
		{Type: MutationPut, Key: key, Value: value},
		{
			Type: MutationPut, Key: environmentMutationEpochKey(task.Owner.EnvironmentID),
			Value: slices.Clone(epochValue),
		},
	}, nil
}

func (repository *TaskRepository) prepareBlueprintCandidateClaimEpoch(
	ctx context.Context,
	task TaskRecord,
	writer taskMaterializationWriterRecord,
	revision int64,
	readyGateRevision int64,
) (Condition, Mutation, error) {
	publicationID := task.Params[TaskReleasePublicationParam]
	if !taskHasBlueprintCandidateAppliedAuthority(task) || validatePublicationID(publicationID) != nil {
		return Condition{}, Mutation{}, corruptReleaseRecord()
	}
	authorityKey := releasePublicationKey(publicationID)
	if task.RetryOf != "" {
		authorityKey = blueprintCandidateAttemptAuthorityKey(task.ID)
	}
	epochKey := environmentMutationEpochKey(task.Owner.EnvironmentID)
	read, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{authorityKey, epochKey}, Revision: revision,
	})
	if err != nil {
		return Condition{}, Mutation{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 ||
		read.Values[0] == nil || read.Values[1] == nil {
		return Condition{}, Mutation{}, corruptReleaseRecord()
	}
	if task.RetryOf == "" {
		marker, decodeErr := decodeReleaseRecord[ReleasePublicationMarker](
			read.Values[0].Value, "release-publication",
		)
		if decodeErr != nil || marker.PublicationID != publicationID || marker.OperationID != task.OperationID {
			return Condition{}, Mutation{}, corruptReleaseRecord()
		}
	} else {
		authority, decodeErr := decodeEnvelope[blueprintCandidateAttemptAuthorityRecord](
			read.Values[0].Value, "blueprint-candidate-attempt-authority",
		)
		if decodeErr != nil || validateBlueprintCandidateAttemptAuthorityRecord(authority, task) != nil {
			return Condition{}, Mutation{}, corruptReleaseRecord()
		}
	}
	epochRevision := read.Values[1].ModRevision
	markerOrAttemptMatches := epochRevision == read.Values[0].ModRevision
	readyGateMatches := readyGateRevision > 0 && epochRevision == readyGateRevision
	appliedPredecessorMatches := writer.BlueprintAppliedPredecessor != nil &&
		writer.BlueprintAppliedPredecessor.Present &&
		epochRevision == writer.BlueprintAppliedPredecessor.KeyRevision
	if !markerOrAttemptMatches && !readyGateMatches && !appliedPredecessorMatches {
		return Condition{}, Mutation{}, errs.New(errs.KindStateConflict, "Blueprint claim mutation epoch changed")
	}
	return Condition{Key: epochKey, ModRevision: epochRevision},
		Mutation{Type: MutationPut, Key: epochKey, Value: slices.Clone(read.Values[1].Value)}, nil
}

func (repository *TaskRepository) blueprintCandidateLiveWriterAuthority(
	ctx context.Context,
	task TaskRecord,
	writer taskMaterializationWriterRecord,
	revision int64,
) (Condition, error) {
	key := taskMaterializationWriterKey(task.Owner.EnvironmentID)
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return Condition{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return Condition{}, corruptTaskMaterializationWriter()
	}
	stored, err := decodeTaskMaterializationWriter(read.Values[0].Value)
	if err != nil || validateTaskMaterializationWriterForTask(stored, task, task.Owner.EnvironmentID) != nil ||
		stored.EnvironmentID != writer.EnvironmentID || stored.TaskID != writer.TaskID ||
		stored.RenderGeneration != writer.RenderGeneration ||
		(stored.BlueprintAppliedPredecessor == nil) != (writer.BlueprintAppliedPredecessor == nil) ||
		(stored.BlueprintAppliedPredecessor != nil &&
			*stored.BlueprintAppliedPredecessor != *writer.BlueprintAppliedPredecessor) {
		return Condition{}, corruptTaskMaterializationWriter()
	}
	return Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}

func (repository *TaskRepository) blueprintCandidateAuthority(
	ctx context.Context,
	task TaskRecord,
	publicationID string,
	appliedPredecessor taskMaterializationAppliedPredecessor,
	epochAuthorityRevision int64,
	revision int64,
) ([]Condition, ReleaseStagedManifest, []byte, error) {
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
		return nil, ReleaseStagedManifest{}, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil ||
		read.Values[3] == nil || read.Values[4] == nil {
		return nil, ReleaseStagedManifest{}, nil, corruptReleaseRecord()
	}
	marker, err := decodeReleaseRecord[ReleasePublicationMarker](read.Values[0].Value, "release-publication")
	if err != nil {
		return nil, ReleaseStagedManifest{}, nil, corruptReleaseRecord()
	}
	manifest, err := decodeReleaseRecord[ReleaseStagedManifest](read.Values[1].Value, "release-staged-manifest")
	if err != nil || validateBlueprintCandidateManifest(task, marker, manifest) != nil {
		return nil, ReleaseStagedManifest{}, nil, corruptReleaseRecord()
	}
	headRevisionID, err := decodeTaskReference(read.Values[3].Value)
	if err != nil || headRevisionID != desiredRevisionID {
		return nil, ReleaseStagedManifest{}, nil, errs.New(errs.KindStateConflict, "Blueprint desired head changed")
	}
	seal, err := decodeEnvironmentBlueprintSeal(read.Values[4].Value)
	if err != nil || seal.EnvironmentID != task.Owner.EnvironmentID ||
		seal.RevisionID != desiredRevisionID || seal.SourceKind != EnvironmentBlueprintSourceApply ||
		seal.RenderGeneration != uint64(task.RenderGeneration) {
		return nil, ReleaseStagedManifest{}, nil, corruptReleaseRecord()
	}
	observedPredecessor, predecessorErr := taskMaterializationAppliedPredecessorFromValue(
		read.Values[5], task.Owner.EnvironmentID, task.RenderGeneration,
	)
	if predecessorErr != nil || observedPredecessor != appliedPredecessor {
		return nil, ReleaseStagedManifest{}, nil, errs.New(
			errs.KindStateConflict,
			"blueprint applied predecessor changed",
		)
	}
	if epochAuthorityRevision <= 0 || read.Values[2].ModRevision != epochAuthorityRevision {
		return nil, ReleaseStagedManifest{}, nil, errs.New(
			errs.KindStateConflict,
			"Blueprint mutation epoch changed",
		)
	}
	conditions := make([]Condition, len(keys))
	for index, key := range keys {
		conditions[index] = Condition{Key: key, ModRevision: keyValueRevision(read.Values[index])}
	}
	return conditions, manifest, slices.Clone(read.Values[2].Value), nil
}

func (repository *TaskRepository) blueprintCandidateAttempts(
	ctx context.Context,
	task TaskRecord,
	seal EnvironmentBlueprintSeal,
	revision int64,
) ([]domain.Attempt, []Condition, error) {
	if task.RetryOf == "" {
		startedAt := task.CreatedAt
		if task.StartedAt != nil {
			startedAt = *task.StartedAt
		}
		return []domain.Attempt{{ID: task.ID, TaskID: task.ID, StartedAt: startedAt}}, nil, nil
	}
	record, condition, err := repository.blueprintCandidateAttemptAuthority(ctx, task, revision)
	if err != nil || validateBlueprintCandidateAttempts(record, task, seal) != nil {
		return nil, nil, corruptReleaseRecord()
	}
	return slices.Clone(record.Attempts), []Condition{condition}, nil
}

func blueprintAttemptIDs(attempts []domain.Attempt) []string {
	result := make([]string, len(attempts))
	for index, attempt := range attempts {
		result[index] = attempt.TaskID
	}
	return result
}

func appendBlueprintCandidateCondition(conditions []Condition, candidate Condition) ([]Condition, error) {
	for _, condition := range conditions {
		if condition.Key != candidate.Key {
			continue
		}
		if condition != candidate {
			return nil, errs.New(errs.KindInternal, "blueprint candidate authority compare changed")
		}
		return conditions, nil
	}
	return append(conditions, candidate), nil
}
