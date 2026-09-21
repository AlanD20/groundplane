package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	hierarchyDeletionAttemptTimeout = 6 * time.Hour
	hierarchyDeletionPlanBatchSize  = 47
)

type hierarchyDeletionStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	MeasureTransaction(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionBudget, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

type HierarchyDeletionRepository struct {
	store hierarchyDeletionStore
}

func NewHierarchyDeletionRepository(store etcdstore.Store) (*HierarchyDeletionRepository, error) {
	return newHierarchyDeletionRepository(store)
}

func newHierarchyDeletionRepository(store hierarchyDeletionStore) (*HierarchyDeletionRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "hierarchy deletion store is required")
	}
	return &HierarchyDeletionRepository{store: store}, nil
}

func (repository *HierarchyDeletionRepository) ResolveProjectDeletionTargetKind(
	ctx context.Context,
	projectID string,
) (HierarchyDeletionTargetKind, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return "", err
	}
	if err := recordcodec.ValidateID(ids.KindProject, projectID); err != nil {
		return "", err
	}
	project, err := getRecord(
		ctx,
		repository.store,
		hierarchyrecord.ProjectKey(projectID),
		projectID,
		errs.KindProjectNotFound,
		hierarchyrecord.DecodeProject,
		func(record hierarchyrecord.ProjectRecord) string { return record.ID },
	)
	if err != nil {
		return "", err
	}
	switch project.Record.Kind {
	case hierarchyrecord.ProjectKindTenant:
		return HierarchyDeletionTargetProject, nil
	case hierarchyrecord.ProjectKindBacking:
		return "", errs.New(errs.KindValidationFailed, "project deletion requires an ordinary Project")
	default:
		return "", errs.New(errs.KindInternal, "hierarchy deletion Project kind is invalid")
	}
}

type HierarchyDeletionTargetResolution struct {
	TargetKind HierarchyDeletionTargetKind
	ScopeKind  idempotencyrecord.IdempotencyScopeKind
	ScopeID    string
}

func (repository *HierarchyDeletionRepository) ResolveDeletionTarget(
	ctx context.Context,
	requested HierarchyDeletionTargetKind,
	targetID string,
) (HierarchyDeletionTargetResolution, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return HierarchyDeletionTargetResolution{}, err
	}
	switch requested {
	case HierarchyDeletionTargetTenant:
		if err := recordcodec.ValidateID(ids.KindTenant, targetID); err != nil {
			return HierarchyDeletionTargetResolution{}, err
		}
		return HierarchyDeletionTargetResolution{
			TargetKind: requested,
			ScopeKind:  idempotencyrecord.IdempotencyScopeTenant,
			ScopeID:    targetID,
		}, nil
	case HierarchyDeletionTargetProject:
		project, err := getRecord(ctx, repository.store, hierarchyrecord.ProjectKey(targetID), targetID, errs.KindProjectNotFound,
			hierarchyrecord.DecodeProject, func(record hierarchyrecord.ProjectRecord) string { return record.ID })
		if err != nil {
			return HierarchyDeletionTargetResolution{}, err
		}
		if project.Record.Kind == hierarchyrecord.ProjectKindBacking {
			return HierarchyDeletionTargetResolution{}, errs.New(
				errs.KindValidationFailed,
				"project deletion requires an ordinary Project",
			)
		}
		if project.Record.Kind != hierarchyrecord.ProjectKindTenant {
			return HierarchyDeletionTargetResolution{}, errs.New(
				errs.KindInternal,
				"hierarchy deletion Project kind is invalid",
			)
		}
		return HierarchyDeletionTargetResolution{
			TargetKind: requested,
			ScopeKind:  idempotencyrecord.IdempotencyScopeTenant,
			ScopeID:    project.Record.TenantID,
		}, nil
	case HierarchyDeletionTargetBacking:
		if err := recordcodec.ValidateID(ids.KindProject, targetID); err != nil {
			return HierarchyDeletionTargetResolution{}, err
		}
		project, err := getRecord(ctx, repository.store, hierarchyrecord.ProjectKey(targetID), targetID, errs.KindProjectNotFound,
			hierarchyrecord.DecodeProject, func(record hierarchyrecord.ProjectRecord) string { return record.ID })
		if err != nil {
			return HierarchyDeletionTargetResolution{}, err
		}
		if project.Record.Kind != hierarchyrecord.ProjectKindBacking || project.Record.TenantID != "" {
			return HierarchyDeletionTargetResolution{}, errs.New(
				errs.KindBackingServiceNotFound,
				"backing service was not found",
			)
		}
		return HierarchyDeletionTargetResolution{
			TargetKind: requested,
			ScopeKind:  idempotencyrecord.IdempotencyScopePlatform,
			ScopeID:    "-",
		}, nil
	case HierarchyDeletionTargetEnvironment:
		if err := recordcodec.ValidateID(ids.KindEnvironment, targetID); err != nil {
			return HierarchyDeletionTargetResolution{}, err
		}
		environment, err := getRecord(
			ctx,
			repository.store,
			hierarchyrecord.EnvironmentKey(targetID),
			targetID,
			errs.KindEnvironmentNotFound,
			hierarchyrecord.DecodeEnvironment,
			func(record hierarchyrecord.EnvironmentRecord) string { return record.ID },
		)
		if err != nil {
			return HierarchyDeletionTargetResolution{}, err
		}
		if err := recordcodec.ValidateID(ids.KindProject, environment.Record.ProjectID); err != nil {
			return HierarchyDeletionTargetResolution{}, corruptHierarchyDeletion()
		}
		return HierarchyDeletionTargetResolution{
			TargetKind: requested,
			ScopeKind:  idempotencyrecord.IdempotencyScopeProject,
			ScopeID:    environment.Record.ProjectID,
		}, nil
	default:
		return HierarchyDeletionTargetResolution{}, errs.New(
			errs.KindValidationFailed,
			"hierarchy deletion target kind is invalid",
		)
	}
}

type HierarchyDeletionBegin struct {
	OperationID     string
	TaskOperationID string
	OperationKind   HierarchyDeletionOperationKind
	TargetKind      HierarchyDeletionTargetKind
	TargetID        string
	TaskID          string
	IdempotencyHash string
	Marker          idempotencyrecord.IdempotencyMarker
	CreatedAt       time.Time
	DeadlineAt      time.Time
}

type HierarchyDeletionOperation struct {
	Tombstone         HierarchyDeletionTombstone
	RootTaskID        string
	MarkerLocator     idempotencyrecord.IdempotencyLocator
	Owner             TaskOwner
	TombstoneRevision int64
	Fence             HierarchyDeletionCleanupFence
	FenceRevision     int64
	Intent            HierarchyDeletionIntent
	IntentRevision    int64
	PlanCursor        int64
	SucceededCount    int64
	FailedCount       int64
	UpdatedAt         time.Time
}

type HierarchyDeletionBeginResult struct {
	Operation   HierarchyDeletionOperation
	Idempotency IdempotencyTransactionResult
	Existing    bool
}

type hierarchyDeletionRoot struct {
	targetKey        string
	targetRevision   int64
	targetValue      []byte
	rootSlug         string
	workspace        HierarchyDeletionWorkspace
	owner            TaskOwner
	primaryFences    []etcdstore.Condition
	coordination     []etcdstore.Versioned[HierarchyCoordinationRecord]
	coordinationKeys []string
}

func (repository *HierarchyDeletionRepository) Begin(
	ctx context.Context,
	begin HierarchyDeletionBegin,
) (HierarchyDeletionBeginResult, error) {
	if err := validateHierarchyDeletionBegin(begin); err != nil {
		return HierarchyDeletionBeginResult{}, err
	}
	known, found, err := existingIdempotencyTransaction(ctx, repository.store, begin.Marker)
	if err != nil {
		return HierarchyDeletionBeginResult{}, err
	}
	if found {
		outcome, marker, conflict, classifyErr := known.Classify()
		if classifyErr != nil {
			return HierarchyDeletionBeginResult{}, classifyErr
		}
		if conflict != nil {
			return HierarchyDeletionBeginResult{}, conflict
		}
		if outcome != IdempotencyKnownExisting || marker.TaskID == "" {
			return HierarchyDeletionBeginResult{}, corruptHierarchyDeletion()
		}
		existing, readErr := repository.OperationByTask(ctx, marker.TaskID)
		return HierarchyDeletionBeginResult{Operation: existing, Idempotency: known, Existing: true}, readErr
	}
	root, snapshotRevision, err := repository.readDeletionRoot(ctx, begin.TargetKind, begin.TargetID)
	if err != nil {
		return HierarchyDeletionBeginResult{}, err
	}
	defer clear(root.targetValue)

	operation, conditions, mutations, initiation, err := prepareHierarchyDeletionPublication(
		begin,
		root,
		snapshotRevision,
	)
	if err != nil {
		return HierarchyDeletionBeginResult{}, err
	}
	defer clearMutationValues(mutations)
	defer clearHierarchyDeletionOperation(operation)

	task, err := hierarchyDeletionTask(begin, root.owner, operation.Intent)
	if err != nil {
		return HierarchyDeletionBeginResult{}, err
	}
	task.idempotencyMarker = cloneIdempotencyLocator(&begin.Marker.Locator)
	encodedTask, err := encodeTaskRecord(task)
	if err != nil {
		return HierarchyDeletionBeginResult{}, err
	}
	defer clear(encodedTask)
	taskReference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return HierarchyDeletionBeginResult{}, err
	}
	defer clear(taskReference)
	conditions = append(conditions,
		etcdstore.Condition{Key: taskKey(task.ID)},
		etcdstore.Condition{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		etcdstore.Condition{Key: taskActiveOperationKey(task.OperationID)},
		etcdstore.Condition{Key: taskQueueKey(task.Executor, task.ID)},
	)
	mutations = append(mutations,
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: taskKey(task.ID), Value: encodedTask},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: taskReference},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: taskReference},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: taskReference},
	)
	plan, err := newTaskIdempotencyMutationPlan(
		task,
		initiation,
		conditions,
		mutations,
		classifyHierarchyDeletionBegin(begin),
	)
	if err != nil {
		return HierarchyDeletionBeginResult{}, err
	}
	if err := plan.enforceTransactionBounds(func(finalConditions []etcdstore.Condition, finalMutations []etcdstore.Mutation) error {
		return validateHierarchyDeletionTransaction(finalConditions, finalMutations, 64)
	}); err != nil {
		return HierarchyDeletionBeginResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return HierarchyDeletionBeginResult{}, err
	}
	result, err := idempotency.Apply(ctx, begin.Marker, plan)
	if err != nil {
		return HierarchyDeletionBeginResult{}, err
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil {
		return HierarchyDeletionBeginResult{}, classifyErr
	}
	if conflict != nil {
		return HierarchyDeletionBeginResult{}, conflict
	}
	if outcome == IdempotencyKnownExisting {
		_, marker, _, _ := result.Classify()
		existing, readErr := repository.OperationByTask(ctx, marker.TaskID)
		return HierarchyDeletionBeginResult{Operation: existing, Idempotency: result, Existing: true}, readErr
	}
	created := cloneHierarchyDeletionOperation(operation)
	created.TombstoneRevision = result.revision
	created.FenceRevision = result.revision
	created.IntentRevision = result.revision
	return HierarchyDeletionBeginResult{Operation: created, Idempotency: result}, nil
}

func validateHierarchyDeletionBegin(begin HierarchyDeletionBegin) error {
	if !validHierarchyDeletionPrivateID(begin.OperationID, "del") ||
		ids.Validate(ids.KindOperation, begin.TaskOperationID) != nil ||
		!validHierarchyDeletionOperation(begin.OperationKind, begin.TargetKind) ||
		!validHierarchyDeletionTarget(begin.TargetKind, begin.TargetID) ||
		ids.Validate(ids.KindTask, begin.TaskID) != nil || !validHierarchyDeletionDigest(begin.IdempotencyHash) ||
		!validHierarchyDeletionTimestamp(begin.CreatedAt) || !validHierarchyDeletionTimestamp(begin.DeadlineAt) ||
		!begin.DeadlineAt.Equal(begin.CreatedAt.Add(hierarchyDeletionAttemptTimeout)) {
		return errs.New(errs.KindValidationFailed, "hierarchy deletion publication is invalid")
	}
	if begin.Marker.Kind != idempotencyrecord.IdempotencyMarkerTask || begin.Marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		begin.Marker.TaskID != begin.TaskID || !begin.Marker.CreatedAt.Equal(begin.CreatedAt) ||
		!begin.Marker.UpdatedAt.Equal(begin.CreatedAt) || idempotencyrecord.ValidateIdempotencyMarker(begin.Marker) != nil {
		return errs.New(errs.KindValidationFailed, "hierarchy deletion idempotency evidence is invalid")
	}
	return nil
}

func hierarchyDeletionTask(
	begin HierarchyDeletionBegin,
	owner TaskOwner,
	intent HierarchyDeletionIntent,
) (TaskRecord, error) {
	separator := strings.IndexByte(begin.TaskID, '_')
	if separator <= 0 || separator == len(begin.TaskID)-1 {
		return TaskRecord{}, errs.New(errs.KindInternal, "hierarchy deletion Task identity is invalid")
	}
	suffix := begin.TaskID[separator+1:]
	intentValue, err := encodeHierarchyDeletionRecord(intent, hierarchyDeletionLargeRecordBytes)
	if err != nil {
		return TaskRecord{}, err
	}
	defer clear(intentValue)
	task := TaskRecord{
		ID: begin.TaskID, OperationID: begin.TaskOperationID, IdempotencyKey: begin.Marker.Locator.Key,
		Owner: owner, Actor: TaskActorOperator, Executor: taskjournal.TaskExecutorController,
		PlanID: "plan_" + suffix, PlanHash: hierarchyDeletionDigest(intentValue), RenderGeneration: 1,
		Type: taskjournal.TaskRemove, Target: begin.TargetID,
		Params: map[string]string{
			TaskResourceKindParam:                TaskResourceHierarchyDeletion,
			TaskHierarchyDeletionOperationParam:  begin.OperationID,
			TaskHierarchyDeletionTargetKindParam: string(begin.TargetKind),
		},
		Steps: []taskjournal.TaskStepRecord{
			{Kind: taskjournal.TaskStepOperation, ID: "step_" + suffix},
		}, TimeoutSeconds: int64(hierarchyDeletionAttemptTimeout / time.Second),
		Status: taskjournal.TaskStatusPending, NextEventSequence: 1, CreatedAt: begin.CreatedAt, UpdatedAt: begin.CreatedAt,
	}
	if err := validateTaskRecord(task); err != nil {
		return TaskRecord{}, err
	}
	return task, nil
}

func classifyHierarchyDeletionBegin(begin HierarchyDeletionBegin) idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		for _, value := range values {
			if value != nil {
				return errs.New(errs.KindStateConflict, "hierarchy deletion publication evidence changed")
			}
		}
		return errs.Newf(errs.KindInternal, "hierarchy deletion %s compare failure is unclassified", begin.OperationID)
	}
}

func (repository *HierarchyDeletionRepository) validateHierarchyDeletionBeginReplay(
	begin HierarchyDeletionBegin,
) func(context.Context, idempotencyrecord.IdempotencyMarker, int64, int64) error {
	return func(ctx context.Context, marker idempotencyrecord.IdempotencyMarker, _, revision int64) error {
		if marker.TaskID != begin.TaskID || revision <= 0 {
			return errs.New(errs.KindStateConflict, "hierarchy deletion replay identity changed")
		}
		operation, err := repository.OperationByTaskAtRevision(ctx, begin.TaskID, revision)
		if err != nil {
			return err
		}
		if operation.Tombstone.OperationID != begin.OperationID ||
			operation.Tombstone.TaskOperationID != begin.TaskOperationID ||
			operation.Tombstone.TargetKind != begin.TargetKind || operation.Tombstone.TargetID != begin.TargetID {
			return errs.New(errs.KindStateConflict, "hierarchy deletion replay evidence changed")
		}
		return nil
	}
}

func hierarchyDeletionResponseDigest(taskID string) string {
	value := []byte(`{"task_id":"` + taskID + `"}`)
	return hierarchyDeletionDigest(value)
}

func hierarchyDeletionPlanDigest(actions [][]byte) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("gp-deletion-plan-v1\x00"))
	writeUint64(hash, uint64(len(actions)))
	for _, action := range actions {
		writeUint32(hash, uint32(len(action)))
		_, _ = hash.Write(action)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

type byteWriter interface{ Write([]byte) (int, error) }

func writeUint64(writer byteWriter, value uint64) {
	buffer := []byte{byte(value >> 56), byte(value >> 48), byte(value >> 40), byte(value >> 32),
		byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)}
	_, _ = writer.Write(buffer)
}

func writeUint32(writer byteWriter, value uint32) {
	buffer := []byte{byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)}
	_, _ = writer.Write(buffer)
}
