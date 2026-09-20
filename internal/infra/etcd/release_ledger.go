package etcd

import (
	"context"
	"encoding/json"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"net/http"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type ReleaseLedger struct {
	store etcdstore.Store
	tasks *TaskRepository
}

func NewReleaseLedger(store etcdstore.Store, tasks *TaskRepository) (*ReleaseLedger, error) {
	if store == nil || tasks == nil || tasks.store == nil {
		return nil, errs.New(errs.KindInternal, "release ledger dependencies are not configured")
	}
	return &ReleaseLedger{store: store, tasks: tasks}, nil
}

type VersionedReleaseManifest struct {
	Record       ReleaseStagedManifest
	Revision     int64
	ReadRevision int64
}

type ReleaseDesiredKind string

const (
	ReleaseDesiredService ReleaseDesiredKind = "service"
	ReleaseDesiredGroup   ReleaseDesiredKind = "release_group"
)

type ReleasePublicationEvidence struct {
	Manifest                   VersionedReleaseManifest
	EnvironmentID              string
	ProjectID                  string
	TenantID                   string
	DesiredKind                ReleaseDesiredKind
	DesiredID                  string
	DesiredRevision            int64
	EnvironmentEpochRevision   int64
	EnvironmentEpochValue      []byte
	FenceRevision              int64
	Task                       TaskRecord
	Marker                     idempotencyrecord.IdempotencyMarker
	Fence                      ReleaseFenceSet
	Operation                  ReleaseOperationHead
	CandidateReleaseDescriptor executionplan.CandidateReleaseDescriptor
	Plan                       *agentpb.ExecutionPlan
	Hooks                      []ReleaseHookExecutionPublication
	PublishedAt                time.Time
}

type ReleasePublicationResult struct {
	Revision    int64
	Task        TaskRecord
	Idempotency IdempotencyTransactionResult
}

func (ledger *ReleaseLedger) Stage(ctx context.Context, input ReleaseStage) (VersionedReleaseManifest, error) {
	if ctx == nil || ledger == nil || ledger.store == nil {
		return VersionedReleaseManifest{}, errs.New(errs.KindInternal, "release staging context or ledger is missing")
	}
	if err := ctx.Err(); err != nil {
		return VersionedReleaseManifest{}, err
	}
	input = cloneReleaseStage(input)
	if err := validateReleaseStage(input); err != nil {
		return VersionedReleaseManifest{}, err
	}
	refs := make([]ReleaseStagedMemberRef, len(input.Members))
	records := make([]releaseStagedWrite, 0, len(input.Members)*3)
	for index, member := range input.Members {
		intentValue, err := encodeReleaseRecord("release-intent", member.Intent)
		if err != nil {
			clearStagedReleaseRecords(records)
			return VersionedReleaseManifest{}, err
		}
		renderValue, err := encodeReleaseRecord("release-render-input", json.RawMessage(member.RenderInput))
		if err != nil {
			clear(intentValue)
			clearStagedReleaseRecords(records)
			return VersionedReleaseManifest{}, err
		}
		checkpointValue, err := encodeReleaseRecord("release-checkpoint", member.Checkpoint)
		if err != nil {
			clear(intentValue)
			clear(renderValue)
			clearStagedReleaseRecords(records)
			return VersionedReleaseManifest{}, err
		}
		intentDigest, _ := domain.Digest(member.Intent)
		renderDigest, _ := domain.Digest(json.RawMessage(member.RenderInput))
		checkpointDigest, _ := domain.Digest(member.Checkpoint)
		refs[index] = ReleaseStagedMemberRef{
			ReleaseID: member.Intent.ID, ServiceID: member.Intent.ServiceID, IntentDigest: intentDigest,
			RenderDigest: renderDigest, CheckpointDigest: checkpointDigest,
		}
		records = append(records,
			releaseStagedWrite{releaseIntentStagingKey(input.PublicationID, member.Intent.ID), intentValue},
			releaseStagedWrite{releaseRenderInputStagingKey(input.PublicationID, member.Intent.ID), renderValue},
			releaseStagedWrite{releaseCheckpointStagingKey(input.PublicationID, member.Intent.ID), checkpointValue},
		)
	}
	defer clearStagedReleaseRecords(records)
	for start := 0; start < len(records); start += 16 {
		end := min(start+16, len(records))
		conditions := make([]etcdstore.Condition, end-start)
		mutations := make([]etcdstore.Mutation, end-start)
		for index, record := range records[start:end] {
			conditions[index] = etcdstore.Condition{Key: record.key}
			mutations[index] = etcdstore.Mutation{Type: etcdstore.MutationPut, Key: record.key, Value: record.value}
		}
		result, err := ledger.store.Transact(ctx, conditions, mutations)
		if err != nil {
			return VersionedReleaseManifest{}, err
		}
		if !result.Succeeded {
			return VersionedReleaseManifest{}, errs.New(
				errs.KindStateConflict,
				"release staging identity already exists",
			)
		}
	}
	manifest := ReleaseStagedManifest{
		PublicationID: input.PublicationID, OperationID: input.OperationID, Members: refs,
		CreatedAt: input.CreatedAt,
	}
	manifest.Digest, _ = domain.Digest(struct {
		PublicationID string                   `json:"publication_id"`
		OperationID   string                   `json:"operation_id"`
		Members       []ReleaseStagedMemberRef `json:"members"`
	}{manifest.PublicationID, manifest.OperationID, manifest.Members})
	manifestValue, err := encodeReleaseRecord("release-staged-manifest", manifest)
	if err != nil {
		return VersionedReleaseManifest{}, err
	}
	defer clear(manifestValue)
	result, err := ledger.store.Transact(ctx,
		[]etcdstore.Condition{{Key: releaseManifestStagingKey(input.PublicationID)}},
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: releaseManifestStagingKey(input.PublicationID), Value: manifestValue}},
	)
	if err != nil {
		return VersionedReleaseManifest{}, err
	}
	if !result.Succeeded {
		return VersionedReleaseManifest{}, errs.New(errs.KindStateConflict, "release staging manifest already exists")
	}
	return VersionedReleaseManifest{Record: manifest, Revision: result.Revision, ReadRevision: result.Revision}, nil
}

func (ledger *ReleaseLedger) Publish(
	ctx context.Context,
	evidence ReleasePublicationEvidence,
) (_ ReleasePublicationResult, publicationErr error) {
	if ctx == nil || ledger == nil || ledger.store == nil || ledger.tasks == nil {
		return ReleasePublicationResult{}, errs.New(errs.KindInternal, "release publication is not configured")
	}
	if err := ctx.Err(); err != nil {
		return ReleasePublicationResult{}, err
	}
	evidence.Task.idempotencyMarker = cloneIdempotencyLocator(&evidence.Marker.Locator)
	if err := validateReleasePublicationEvidence(evidence); err != nil {
		return ReleasePublicationResult{}, err
	}
	task, preparedPins, err := prepareRecoverySecretPins(ctx, ledger.store, evidence.Task)
	if err != nil {
		return ReleasePublicationResult{}, err
	}
	evidence.Task = task
	if !preparedPins.IsZero() {
		defer func() { publicationErr = finishRecoverySecretPreparation(ctx, ledger.store, task, publicationErr) }()
	}
	pinChange, err := recoverySecretPinActivation(preparedPins)
	if err != nil {
		return ReleasePublicationResult{}, err
	}
	defer clearTaskMaterializationProjectionChange(pinChange)
	executedArtifact, err := releasePreparedArtifact(evidence)
	if err != nil {
		return ReleasePublicationResult{}, err
	}
	defer clear(executedArtifact)
	runtimes, err := executionplan.PrepareCandidateRuntimes(evidence.Plan)
	if err != nil {
		return ReleasePublicationResult{}, err
	}
	fragment, err := ledger.tasks.prepareReleaseTaskPublicationFragment(evidence.Task)
	if err != nil {
		return ReleasePublicationResult{}, err
	}
	defer clearReleaseTaskPublicationFragment(fragment)
	hookFragment, err := prepareReleaseHookPublicationFragment(evidence)
	if err != nil {
		return ReleasePublicationResult{}, err
	}
	defer clearReleaseHookPublicationFragment(hookFragment)
	markerKey, err := idempotencyrecord.IdempotencyMarkerKey(evidence.Marker.Locator)
	if err != nil {
		return ReleasePublicationResult{}, err
	}
	desiredKey, err := releaseDesiredRecordKey(evidence.DesiredKind, evidence.DesiredID, evidence.EnvironmentID)
	if err != nil {
		return ReleasePublicationResult{}, err
	}
	markerValue, err := idempotencyrecord.EncodeIdempotencyMarker(evidence.Marker)
	if err != nil {
		return ReleasePublicationResult{}, err
	}
	defer clear(markerValue)
	publicationValue, err := encodeReleaseRecord("release-publication", ReleasePublicationMarker{
		PublicationID: evidence.Manifest.Record.PublicationID, OperationID: evidence.Manifest.Record.OperationID,
		ManifestDigest:             evidence.Manifest.Record.Digest,
		CandidateReleaseDescriptor: executionplan.CloneCandidateReleaseDescriptor(evidence.CandidateReleaseDescriptor),
		ExecutedComposeArtifact:    executedArtifact,
		PreparedRuntimes:           runtimes,
		PublishedAt:                evidence.PublishedAt,
	})
	if err != nil {
		return ReleasePublicationResult{}, err
	}
	defer clear(publicationValue)
	fenceValue, err := encodeReleaseRecord("release-fence-set", evidence.Fence)
	if err != nil {
		return ReleasePublicationResult{}, err
	}
	defer clear(fenceValue)
	operationValue, err := encodeReleaseRecord("release-operation", evidence.Operation)
	if err != nil {
		return ReleasePublicationResult{}, err
	}
	defer clear(operationValue)
	conditions := []etcdstore.Condition{
		{Key: hierarchyrecord.EnvironmentMutationEpochKey(evidence.EnvironmentID), ModRevision: evidence.EnvironmentEpochRevision},
		{Key: deletionTombstoneKey("environment", evidence.EnvironmentID)},
		{Key: deletionTombstoneKey("project", evidence.ProjectID)},
		{Key: deletionTombstoneKey("tenant", evidence.TenantID)},
		{Key: desiredKey, ModRevision: evidence.DesiredRevision},
		{Key: releaseFenceSetKey(evidence.EnvironmentID), ModRevision: evidence.FenceRevision},
		{Key: markerKey},
		{
			Key:         releaseManifestStagingKey(evidence.Manifest.Record.PublicationID),
			ModRevision: evidence.Manifest.Revision,
		},
		{Key: releasePublicationKey(evidence.Manifest.Record.PublicationID)},
		{Key: releaseOperationKey(evidence.Manifest.Record.OperationID)},
		fragment.condition,
	}
	conditions = append(conditions, hookFragment.conditions...)
	configurationCondition, hasConfiguration, err := taskConfigurationCondition(evidence.Task)
	if err != nil {
		return ReleasePublicationResult{}, err
	}
	configurationConditions := 0
	if hasConfiguration {
		conditions = append(conditions, configurationCondition)
		configurationConditions = 1
	}
	mutations := make([]etcdstore.Mutation, 0, len(evidence.Manifest.Record.Members)*2+11)
	for _, member := range evidence.Manifest.Record.Members {
		environmentValue, encodeErr := json.Marshal(releaseEnvironmentIndexValue{
			Schema: 1, ServiceID: member.ServiceID, PublicationID: evidence.Manifest.Record.PublicationID,
		})
		if encodeErr != nil {
			clearMutations(mutations)
			return ReleasePublicationResult{}, errs.Wrap(errs.KindInternal, encodeErr)
		}
		serviceValue, encodeErr := json.Marshal(
			releaseServiceIndexValue{Schema: 1, PublicationID: evidence.Manifest.Record.PublicationID},
		)
		if encodeErr != nil {
			clear(environmentValue)
			clearMutations(mutations)
			return ReleasePublicationResult{}, errs.Wrap(errs.KindInternal, encodeErr)
		}
		mutations = append(
			mutations,
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   releaseEnvironmentIndexKey(evidence.EnvironmentID, member.ReleaseID),
				Value: environmentValue,
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   releaseServiceIndexKey(evidence.EnvironmentID, member.ServiceID, member.ReleaseID),
				Value: serviceValue,
			},
		)
	}
	defer clearMutations(mutations)
	mutations = append(
		mutations,
		etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   releasePublicationKey(evidence.Manifest.Record.PublicationID),
			Value: publicationValue,
		},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: releaseFenceSetKey(evidence.EnvironmentID), Value: fenceValue},
		etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   releaseOperationKey(evidence.Manifest.Record.OperationID),
			Value: operationValue,
		},
	)
	mutations = append(mutations, cloneReleaseTaskMutations(fragment)...)
	mutations = append(mutations, hookFragment.mutations...)
	mutations = append(
		mutations,
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: markerKey, Value: markerValue},
		etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   hierarchyrecord.EnvironmentMutationEpochKey(evidence.EnvironmentID),
			Value: slices.Clone(evidence.EnvironmentEpochValue),
		},
	)
	if len(conditions) != 11+len(hookFragment.conditions)+configurationConditions ||
		len(mutations) != len(evidence.Manifest.Record.Members)*2+11+len(hookFragment.mutations) ||
		len(
			conditions,
		)+len(
			mutations,
		)+len(
			pinChange.conditions,
		)+len(
			pinChange.mutations,
		) > etcdstore.MaximumOperations {
		return ReleasePublicationResult{}, errs.New(
			errs.KindInternal,
			"release publication operation budget is invalid",
		)
	}
	conditions = append(conditions, pinChange.conditions...)
	mutations = append(mutations, pinChange.mutations...)
	result, err := ledger.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return ReleasePublicationResult{}, err
	}
	defer clearKeyValues(result.FailureReads)
	if result.Succeeded {
		return ReleasePublicationResult{
			Revision: result.Revision, Task: evidence.Task,
			Idempotency: IdempotencyTransactionResult{
				kind: idempotencyTransactionApplied, revision: result.Revision,
			},
		}, nil
	}
	if len(result.FailureReads) != len(conditions) {
		return ReleasePublicationResult{}, errs.New(
			errs.KindInternal,
			"release publication conflict evidence is incomplete",
		)
	}
	if result.FailureReads[6] != nil {
		existing, err := idempotencyrecord.DecodeIdempotencyMarker(result.FailureReads[6].Value, evidence.Marker.Locator)
		if err != nil {
			return ReleasePublicationResult{}, err
		}
		return ReleasePublicationResult{
			Revision: result.Revision, Task: evidence.Task,
			Idempotency: IdempotencyTransactionResult{
				kind: idempotencyTransactionExisting, revision: result.Revision, marker: existing,
			},
		}, nil
	}
	return ReleasePublicationResult{
		Revision: result.Revision, Task: evidence.Task,
		Idempotency: IdempotencyTransactionResult{
			kind: idempotencyTransactionConflict, revision: result.Revision,
			conflict: classifyReleasePublicationConflict(result.FailureReads[:11]),
		},
	}, nil
}

func (ledger *ReleaseLedger) GetIntent(
	ctx context.Context,
	publicationID, releaseID string,
) (etcdstore.Versioned[domain.Intent], error) {
	if ctx == nil || ledger == nil || validatePublicationID(publicationID) != nil ||
		ids.Validate(ids.KindDeployment, releaseID) != nil {
		return etcdstore.Versioned[domain.Intent]{}, errs.New(errs.KindValidationFailed, "release read identity is invalid")
	}
	result, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		releaseIntentStagingKey(publicationID, releaseID), releasePublicationKey(publicationID),
	}})
	if err != nil {
		return etcdstore.Versioned[domain.Intent]{}, err
	}
	if result == nil || len(result.Values) != 2 || result.Values[1] == nil {
		return etcdstore.Versioned[domain.Intent]{}, errs.New(errs.KindReleaseNotFound, "release was not found")
	}
	if result.Values[0] == nil {
		return etcdstore.Versioned[domain.Intent]{}, corruptReleaseRecord()
	}
	intent, err := decodeReleaseRecord[domain.Intent](result.Values[0].Value, "release-intent")
	if err != nil || domain.ValidateIntent(intent) != nil || intent.ID != releaseID {
		return etcdstore.Versioned[domain.Intent]{}, corruptReleaseRecord()
	}
	return etcdstore.Versioned[domain.Intent]{
		Record:       intent,
		Revision:     result.Values[0].ModRevision,
		ReadRevision: result.ReadRevision,
	}, nil
}

func validateReleasePublicationEvidence(value ReleasePublicationEvidence) error {
	manifest := value.Manifest.Record
	if validatePublicationID(manifest.PublicationID) != nil ||
		ids.Validate(ids.KindOperation, manifest.OperationID) != nil ||
		value.Manifest.Revision <= 0 ||
		value.Manifest.ReadRevision < value.Manifest.Revision ||
		len(manifest.Members) == 0 ||
		len(manifest.Members) > maximumReleasePublicationMembers ||
		ids.Validate(ids.KindEnvironment, value.EnvironmentID) != nil ||
		ids.Validate(ids.KindProject, value.ProjectID) != nil ||
		ids.Validate(ids.KindTenant, value.TenantID) != nil ||
		value.DesiredRevision <= 0 ||
		value.EnvironmentEpochRevision <= 0 ||
		len(value.EnvironmentEpochValue) == 0 ||
		value.PublishedAt.IsZero() ||
		value.PublishedAt.Location() != time.UTC {
		return errs.New(errs.KindValidationFailed, "release publication evidence is invalid")
	}
	if value.DesiredID != value.Task.Target {
		return errs.New(errs.KindValidationFailed, "release publication desired identity is invalid")
	}
	if _, err := releaseDesiredRecordKey(value.DesiredKind, value.DesiredID, value.EnvironmentID); err != nil {
		return err
	}
	if value.Task.OperationID != manifest.OperationID || value.Task.Owner.EnvironmentID != value.EnvironmentID ||
		value.Marker.Kind != idempotencyrecord.IdempotencyMarkerTask || value.Marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		value.Marker.TaskID != value.Task.ID || value.Marker.Response.Status != http.StatusAccepted ||
		value.Marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment || value.Marker.Locator.ScopeID != value.EnvironmentID {
		return errs.New(errs.KindValidationFailed, "release publication task or idempotency evidence is invalid")
	}
	if _, err := validateReleaseCandidateDescriptor(value.CandidateReleaseDescriptor, value.Task, manifest); err != nil {
		return err
	}
	if err := validateReleaseOperationHead(value.Operation, manifest, value.Task); err != nil {
		return err
	}
	if err := validateReleaseFenceSet(value.Fence, value.Operation, manifest); err != nil {
		return err
	}
	return nil
}

func releaseDesiredRecordKey(kind ReleaseDesiredKind, id string, environmentID string) (string, error) {
	switch kind {
	case ReleaseDesiredService:
		if ids.Validate(ids.KindService, id) == nil && ids.Validate(ids.KindEnvironment, environmentID) == nil {
			return environmentBlueprintHeadKey(environmentID), nil
		}
	case ReleaseDesiredGroup:
		if ids.Validate(ids.KindReleaseGroup, id) == nil {
			return releaseGroupRecordKey(id), nil
		}
	}
	return "", errs.New(errs.KindValidationFailed, "release publication desired identity is invalid")
}

func classifyReleasePublicationConflict(values []*etcdstore.KeyValue) error {
	if len(values) != 11 {
		return errs.New(errs.KindInternal, "release publication conflict evidence is incomplete")
	}
	if values[1] != nil || values[2] != nil || values[3] != nil || values[5] != nil {
		return errs.New(errs.KindResourceInUse, "release publication scope is locked or deleting")
	}
	if values[8] != nil || values[9] != nil || values[10] != nil {
		return errs.New(errs.KindInternal, "release publication identity collided")
	}
	return errs.New(errs.KindStateConflict, "release publication fixed-revision evidence changed")
}

func clearStagedReleaseRecords(records []releaseStagedWrite) {
	for index := range records {
		clear(records[index].value)
	}
}

func clearMutations(mutations []etcdstore.Mutation) {
	for index := range mutations {
		clear(mutations[index].Value)
	}
}
