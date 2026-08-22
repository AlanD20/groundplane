package etcd

import (
	"context"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// DeletionTargetKind is the closed hierarchy deletion catalog. Additional
// resource kinds enter this catalog only with their resource-specific
// finalization contract.
type DeletionTargetKind string

const (
	DeletionTargetTenant      DeletionTargetKind = "tenant"
	DeletionTargetProject     DeletionTargetKind = "project"
	DeletionTargetEnvironment DeletionTargetKind = "environment"
)

// DeletionPhase records which authority may advance a destructive operation.
// A completed tombstone is never retained.
type DeletionPhase string

const (
	DeletionPhaseHostEffects DeletionPhase = "host_effects"
	DeletionPhaseFinalizing  DeletionPhase = "finalizing"
)

// DeletionCheckpoint identifies the last fully finalized descendant. Empty
// fields are the initial checkpoint; nonempty fields are always paired.
type DeletionCheckpoint struct {
	ResourceKind string `json:"resource_kind,omitempty"`
	StableID     string `json:"stable_id,omitempty"`
}

// DeletionTombstoneRecord fences all mutations beneath one visible resource
// while its destructive Task performs host effects and stable-id postorder
// finalization.
type DeletionTombstoneRecord struct {
	TargetKind     DeletionTargetKind `json:"target_kind"`
	TargetID       string             `json:"target_id"`
	TargetRevision int64              `json:"target_revision"`
	TaskID         string             `json:"task_id"`
	Phase          DeletionPhase      `json:"phase"`
	Checkpoint     DeletionCheckpoint `json:"checkpoint"`
	CreatedAt      time.Time          `json:"created_at"`
	UpdatedAt      time.Time          `json:"updated_at"`
}

func (repository *HierarchyRepository) GetDeletionTombstone(
	ctx context.Context,
	targetKind DeletionTargetKind,
	targetID string,
) (Versioned[DeletionTombstoneRecord], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[DeletionTombstoneRecord]{}, false, err
	}
	if err := validateDeletionTarget(targetKind, targetID); err != nil {
		return Versioned[DeletionTombstoneRecord]{}, false, err
	}
	result, err := repository.store.Get(ctx, deletionTombstoneKey(string(targetKind), targetID))
	if err != nil {
		return Versioned[DeletionTombstoneRecord]{}, false, err
	}
	if result == nil {
		return Versioned[DeletionTombstoneRecord]{}, false, errs.New(errs.KindInternal, "deletion tombstone read is empty")
	}
	if result.Entry == nil {
		return Versioned[DeletionTombstoneRecord]{ReadRevision: result.ReadRevision}, false, nil
	}
	record, err := decodeDeletionTombstone(result.Entry.Value)
	if err != nil || record.TargetKind != targetKind || record.TargetID != targetID {
		return Versioned[DeletionTombstoneRecord]{}, false, corruptDeletionTombstone()
	}
	return Versioned[DeletionTombstoneRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

// BeginEnvironmentDeletionWithTask atomically fences an Environment and
// publishes the immutable Agent Task that performs its host effects. The
// Environment and all indexes remain visible until finalization.
func (repository *HierarchyRepository) BeginEnvironmentDeletionWithTask(
	ctx context.Context,
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
	expectedBlueprintRevision int64,
	tombstone DeletionTombstoneRecord,
	task TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateProject(project.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateEnvironment(environment.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if expectedBlueprintRevision < 0 {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "expected Blueprint revision cannot be negative")
	}
	if project.Record.Kind != ProjectKindTenant || project.Revision <= 0 || environment.Revision <= 0 ||
		project.ReadRevision < project.Revision || environment.ReadRevision < environment.Revision ||
		environment.Record.ProjectID != project.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Environment hierarchy changed before deletion")
	}
	if err := validateDeletionTombstone(tombstone); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if tombstone.TargetKind != DeletionTargetEnvironment || tombstone.TargetID != environment.Record.ID ||
		tombstone.TargetRevision != environment.Revision || tombstone.TaskID != task.ID ||
		tombstone.Phase != DeletionPhaseHostEffects || !tombstone.CreatedAt.Equal(task.CreatedAt) ||
		!tombstone.UpdatedAt.Equal(tombstone.CreatedAt) || task.Executor != TaskExecutorAgent ||
		task.Type != TaskRemove || task.Target != environment.Record.ID || task.Status != TaskStatusPending {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "Environment deletion Task and tombstone do not match")
	}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != environment.Record.ID || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "Environment deletion marker does not match its Task")
	}

	secondary, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			environmentNameKey(environment.Record.ProjectID, environment.Record.Name),
			environmentOwnerKey(environment.Record.ProjectID, environment.Record.ID),
			environmentBlueprintHeadKey(environment.Record.ID),
			environmentComposeProjectionKey(environment.Record.ID),
		},
		Revision: environment.ReadRevision,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if secondary == nil || len(secondary.Values) != 4 || secondary.Values[0] == nil || secondary.Values[1] == nil ||
		string(secondary.Values[0].Value) != environment.Record.ID || string(secondary.Values[1].Value) != environment.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Environment deletion index evidence is corrupt")
	}
	if expectedBlueprintRevision == 0 {
		if secondary.Values[2] != nil || secondary.Values[3] != nil || len(task.Params) != 1 {
			return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Environment Blueprint state changed")
		}
	} else if secondary.Values[2] == nil || secondary.Values[3] == nil ||
		secondary.Values[2].ModRevision != expectedBlueprintRevision || secondary.Values[3].ModRevision != expectedBlueprintRevision ||
		len(task.Params) != 4 || task.Params[EnvironmentBlueprintRevisionParam] == "" ||
		task.Params[TaskMaterializationEnvironmentParam] != environment.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Environment Blueprint state changed")
	}

	task = cloneTaskRecord(task)
	if task.IdempotencyKey == "" {
		task.IdempotencyKey = marker.Locator.Key
	}
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := validateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	tombstoneValue, err := encodeDeletionTombstone(tombstone)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(tombstoneValue)
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := encodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)

	tombstoneKey := deletionTombstoneKey(string(DeletionTargetEnvironment), environment.Record.ID)
	conditions := []Condition{
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
		{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: environmentNameKey(environment.Record.ProjectID, environment.Record.Name), ModRevision: secondary.Values[0].ModRevision},
		{Key: environmentOwnerKey(environment.Record.ProjectID, environment.Record.ID), ModRevision: secondary.Values[1].ModRevision},
		{Key: tombstoneKey},
		{Key: environmentBlueprintHeadKey(environment.Record.ID), ModRevision: expectedBlueprintRevision},
		{Key: environmentComposeProjectionKey(environment.Record.ID), ModRevision: expectedBlueprintRevision},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey(string(DeletionTargetProject), project.Record.ID)},
		{Key: deletionTombstoneKey(string(DeletionTargetTenant), project.Record.TenantID)},
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: MutationPut, Key: tombstoneKey, Value: tombstoneValue},
	}
	plan, err := newTaskIdempotencyMutationPlan(
		conditions,
		mutations,
		classifyEnvironmentDeletionStartConflict(project, environment, task.OperationID, expectedBlueprintRevision),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func classifyEnvironmentDeletionStartConflict(
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
	operationID string,
	expectedBlueprintRevision int64,
) idempotencyPlanClassifier {
	return func(_ int64, values []*KeyValue) error {
		if len(values) != 13 {
			return errs.New(errs.KindInternal, "Environment deletion compare evidence is incomplete")
		}
		if values[2] != nil {
			activeTaskID, err := decodeTaskReference(values[2].Value)
			if err != nil {
				return err
			}
			return errs.Newf(errs.KindStateConflict, "operation %s already has active task %s", operationID, activeTaskID)
		}
		for _, index := range []int{0, 1, 3} {
			if values[index] != nil {
				return errs.New(errs.KindInternal, "Environment deletion collided with durable Task state")
			}
		}
		if values[4] == nil {
			return errs.New(errs.KindEnvironmentNotFound, "environment was not found")
		}
		if values[4].ModRevision != environment.Revision {
			return stateConflict("environment", environment.Record.ID)
		}
		for _, index := range []int{5, 6} {
			if values[index] == nil || string(values[index].Value) != environment.Record.ID {
				return errs.New(errs.KindInternal, "Environment deletion index changed or is corrupt")
			}
		}
		if values[7] != nil {
			return errs.New(errs.KindResourceInUse, "Environment deletion is already in progress")
		}
		if keyValueRevision(values[8]) != expectedBlueprintRevision || keyValueRevision(values[9]) != expectedBlueprintRevision {
			return errs.New(errs.KindStateConflict, "Environment Blueprint state changed")
		}
		if values[10] == nil {
			return errs.New(errs.KindProjectNotFound, "project was not found")
		}
		if values[10].ModRevision != project.Revision {
			return stateConflict("project", project.Record.ID)
		}
		if values[11] != nil {
			return errs.New(errs.KindResourceInUse, "Project deletion is in progress")
		}
		if values[12] != nil {
			return errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
		}
		return errs.New(errs.KindStateConflict, "Environment deletion state changed")
	}
}

func encodeDeletionTombstone(record DeletionTombstoneRecord) ([]byte, error) {
	if err := validateDeletionTombstone(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("deletion-tombstone", record)
}

func decodeDeletionTombstone(value []byte) (DeletionTombstoneRecord, error) {
	record, err := decodeEnvelope[DeletionTombstoneRecord](value, "deletion-tombstone")
	if err != nil {
		return DeletionTombstoneRecord{}, err
	}
	if err := validateDeletionTombstone(record); err != nil {
		return DeletionTombstoneRecord{}, corruptDeletionTombstone()
	}
	return record, nil
}

func validateDeletionTombstone(record DeletionTombstoneRecord) error {
	if err := validateDeletionTarget(record.TargetKind, record.TargetID); err != nil {
		return err
	}
	if record.TargetRevision <= 0 || validateStableID(ids.KindTask, record.TaskID) != nil ||
		(record.Phase != DeletionPhaseHostEffects && record.Phase != DeletionPhaseFinalizing) ||
		record.CreatedAt.IsZero() || !record.CreatedAt.Equal(record.CreatedAt.UTC()) ||
		record.UpdatedAt.Before(record.CreatedAt) || !record.UpdatedAt.Equal(record.UpdatedAt.UTC()) {
		return errs.New(errs.KindValidationFailed, "deletion tombstone lifecycle is invalid")
	}
	checkpoint := record.Checkpoint
	if (checkpoint.ResourceKind == "") != (checkpoint.StableID == "") {
		return errs.New(errs.KindValidationFailed, "deletion checkpoint is incomplete")
	}
	if checkpoint.ResourceKind != "" && (!validDeletionCheckpointKind(checkpoint.ResourceKind) ||
		!utf8.ValidString(checkpoint.StableID) || checkpoint.StableID == "") {
		return errs.New(errs.KindValidationFailed, "deletion checkpoint is invalid")
	}
	return nil
}

func validateDeletionTarget(kind DeletionTargetKind, id string) error {
	var expected ids.Kind
	switch kind {
	case DeletionTargetTenant:
		expected = ids.KindTenant
	case DeletionTargetProject:
		expected = ids.KindProject
	case DeletionTargetEnvironment:
		expected = ids.KindEnvironment
	default:
		return errs.New(errs.KindValidationFailed, "deletion target kind is invalid")
	}
	if validateStableID(expected, id) != nil {
		return errs.New(errs.KindValidationFailed, "deletion target id is invalid")
	}
	return nil
}

func validDeletionCheckpointKind(value string) bool {
	switch value {
	case "zone", "service", "route", "volume", "entry", "script", "release_group", "component",
		"connector", "backup_policy", "blueprint_revision", "environment", "project":
		return true
	default:
		return false
	}
}

func keyValueRevision(value *KeyValue) int64 {
	if value == nil {
		return 0
	}
	return value.ModRevision
}

func corruptDeletionTombstone() error {
	return errs.New(errs.KindInternal, "deletion tombstone is corrupt")
}
