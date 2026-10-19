package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionplanning"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type hierarchyDeletionStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	MeasureTransaction(
		context.Context,
		[]etcdstore.Condition,
		[]etcdstore.Mutation,
	) (etcdstore.TransactionBudget, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

type HierarchyDeletionRepository struct {
	plans *hierarchydeletionplanning.Publication
	store hierarchyDeletionStore
}

func NewHierarchyDeletionRepository(store etcdstore.Store) (*HierarchyDeletionRepository, error) {
	return newHierarchyDeletionRepository(store)
}

func newHierarchyDeletionRepository(store hierarchyDeletionStore) (*HierarchyDeletionRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "hierarchy deletion store is required")
	}
	repository := &HierarchyDeletionRepository{store: store}
	repository.plans = hierarchydeletionplanning.NewPublication(store, repository)
	return repository, nil
}

func (repository *HierarchyDeletionRepository) AppendActions(
	ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
	actions []hierarchydeletion.HierarchyDeletionAction,
	start int64,
	seal bool,
) (hierarchydeletion.HierarchyDeletionOperation, error) {
	return repository.plans.AppendActions(ctx, operation, actions, start, seal)
}

func (repository *HierarchyDeletionRepository) ResolveProjectDeletionTargetKind(
	ctx context.Context,
	projectID string,
) (hierarchydeletion.HierarchyDeletionTargetKind, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return "", err
	}
	if err := recordcodec.ValidateID(ids.KindProject, projectID); err != nil {
		return "", err
	}
	project, err := recordquery.Get(
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
		return hierarchydeletion.HierarchyDeletionTargetProject, nil
	case hierarchyrecord.ProjectKindBacking:
		return "", errs.New(errs.KindValidationFailed, "project deletion requires an ordinary Project")
	default:
		return "", errs.New(errs.KindInternal, "hierarchy deletion Project kind is invalid")
	}
}

type HierarchyDeletionTargetResolution struct {
	TargetKind hierarchydeletion.HierarchyDeletionTargetKind
	ScopeKind  idempotencyrecord.IdempotencyScopeKind
	ScopeID    string
}

func (repository *HierarchyDeletionRepository) ResolveDeletionTarget(
	ctx context.Context,
	requested hierarchydeletion.HierarchyDeletionTargetKind,
	targetID string,
) (HierarchyDeletionTargetResolution, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return HierarchyDeletionTargetResolution{}, err
	}
	switch requested {
	case hierarchydeletion.HierarchyDeletionTargetTenant:
		if err := recordcodec.ValidateID(ids.KindTenant, targetID); err != nil {
			return HierarchyDeletionTargetResolution{}, err
		}
		return HierarchyDeletionTargetResolution{
			TargetKind: requested,
			ScopeKind:  idempotencyrecord.IdempotencyScopeTenant,
			ScopeID:    targetID,
		}, nil
	case hierarchydeletion.HierarchyDeletionTargetProject:
		project, err := recordquery.Get(
			ctx,
			repository.store,
			hierarchyrecord.ProjectKey(targetID),
			targetID,
			errs.KindProjectNotFound,
			hierarchyrecord.DecodeProject,
			func(record hierarchyrecord.ProjectRecord) string { return record.ID },
		)
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
	case hierarchydeletion.HierarchyDeletionTargetBacking:
		if err := recordcodec.ValidateID(ids.KindProject, targetID); err != nil {
			return HierarchyDeletionTargetResolution{}, err
		}
		project, err := recordquery.Get(
			ctx,
			repository.store,
			hierarchyrecord.ProjectKey(targetID),
			targetID,
			errs.KindProjectNotFound,
			hierarchyrecord.DecodeProject,
			func(record hierarchyrecord.ProjectRecord) string { return record.ID },
		)
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
	case hierarchydeletion.HierarchyDeletionTargetEnvironment:
		if err := recordcodec.ValidateID(ids.KindEnvironment, targetID); err != nil {
			return HierarchyDeletionTargetResolution{}, err
		}
		environment, err := recordquery.Get(
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
			return HierarchyDeletionTargetResolution{}, hierarchydeletion.CorruptHierarchyDeletion()
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
	OperationKind   hierarchydeletion.HierarchyDeletionOperationKind
	TargetKind      hierarchydeletion.HierarchyDeletionTargetKind
	TargetID        string
	TaskID          string
	IdempotencyHash string
	Marker          idempotencyrecord.IdempotencyMarker
	CreatedAt       time.Time
	DeadlineAt      time.Time
}

type HierarchyDeletionBeginResult struct {
	Operation   hierarchydeletion.HierarchyDeletionOperation
	Idempotency IdempotencyTransactionResult
	Existing    bool
}

type hierarchyDeletionRoot struct {
	targetKey                      string
	targetRevision                 int64
	targetValue                    []byte
	rootSlug                       string
	workspace                      hierarchydeletion.HierarchyDeletionWorkspace
	scope                          workexecution.Scope
	primaryFences, referenceFences []etcdstore.Condition
	coordination                   []etcdstore.Versioned[hierarchydeletion.HierarchyCoordinationRecord]
	coordinationKeys               []string
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
			return HierarchyDeletionBeginResult{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		existing, readErr := repository.OperationByTask(ctx, marker.TaskID)
		return HierarchyDeletionBeginResult{Operation: existing, Idempotency: known, Existing: true}, readErr
	}
	root, snapshotRevision, err := repository.readDeletionRoot(ctx, begin.TargetKind, begin.TargetID)
	if err != nil {
		return HierarchyDeletionBeginResult{}, err
	}
	defer clear(root.targetValue)

	operation, conditions, mutations, err := prepareHierarchyDeletionPublication(
		begin,
		root,
		snapshotRevision,
	)
	if err != nil {
		return HierarchyDeletionBeginResult{}, err
	}
	defer etcdstore.ClearMutationValues(mutations)
	defer hierarchydeletion.ClearHierarchyDeletionOperation(operation)

	task, err := hierarchyDeletionTask(begin, root.scope, operation.Intent)
	if err != nil {
		return HierarchyDeletionBeginResult{}, err
	}
	work, err := workqueue.PrepareTask(task, begin.Marker)
	if err != nil {
		return HierarchyDeletionBeginResult{}, err
	}
	defer etcdstore.ClearMutationValues(work.Mutations)
	hostID, hostConditions, err := repository.captureDeletionHost(ctx, snapshotRevision)
	if err != nil {
		return HierarchyDeletionBeginResult{}, err
	}
	conditions = append(conditions, hostConditions...)
	binding, err := hierarchydeletion.EncodeTaskBinding(
		hierarchydeletion.TaskBinding{
			TaskID:      task.ID,
			OperationID: begin.OperationID,
			Marker:      begin.Marker.Locator,
			HostID:      hostID,
		},
	)
	if err != nil {
		return HierarchyDeletionBeginResult{}, err
	}
	defer clear(binding)
	conditions = append(conditions, etcdstore.Condition{Key: hierarchydeletion.TaskBindingKey(task.ID)})
	mutations = append(
		mutations,
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: hierarchydeletion.TaskBindingKey(task.ID), Value: binding},
	)
	conditions = append(conditions, work.Conditions...)
	mutations = append(mutations, work.Mutations...)
	plan, err := newIdempotencyMutationPlanForMarker(
		idempotencyrecord.IdempotencyMarkerTask,
		conditions,
		mutations,
		classifyHierarchyDeletionBegin(begin),
	)
	if err != nil {
		return HierarchyDeletionBeginResult{}, err
	}
	if err := plan.enforceTransactionBounds(func(finalConditions []etcdstore.Condition, finalMutations []etcdstore.Mutation) error {
		return hierarchydeletion.ValidateHierarchyDeletionTransaction(finalConditions, finalMutations, 64)
	}); err != nil {
		return HierarchyDeletionBeginResult{}, err
	}
	idempotency, err := NewIdempotencyRepository(repository.store)
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
	created := hierarchydeletion.CloneHierarchyDeletionOperation(operation)
	created.TombstoneRevision = result.revision
	created.FenceRevision = result.revision
	created.IntentRevision = result.revision
	return HierarchyDeletionBeginResult{Operation: created, Idempotency: result}, nil
}

func validateHierarchyDeletionBegin(begin HierarchyDeletionBegin) error {
	if !hierarchydeletion.ValidHierarchyDeletionPrivateID(begin.OperationID, "del") ||
		!hierarchydeletion.ValidHierarchyDeletionOperation(begin.OperationKind, begin.TargetKind) ||
		!hierarchydeletion.ValidHierarchyDeletionTarget(begin.TargetKind, begin.TargetID) ||
		ids.Validate(
			ids.KindTask,
			begin.TaskID,
		) != nil || !hierarchydeletion.ValidHierarchyDeletionDigest(begin.IdempotencyHash) ||
		!hierarchydeletion.ValidHierarchyDeletionTimestamp(
			begin.CreatedAt,
		) || !hierarchydeletion.ValidHierarchyDeletionTimestamp(begin.DeadlineAt) ||
		!begin.DeadlineAt.Equal(begin.CreatedAt.Add(hierarchydeletion.AttemptTimeout)) {
		return errs.New(errs.KindValidationFailed, "hierarchy deletion publication is invalid")
	}
	if begin.Marker.Kind != idempotencyrecord.IdempotencyMarkerTask ||
		begin.Marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		begin.Marker.TaskID != begin.TaskID ||
		!begin.Marker.CreatedAt.Equal(begin.CreatedAt) ||
		!begin.Marker.UpdatedAt.Equal(begin.CreatedAt) ||
		idempotencyrecord.ValidateIdempotencyMarker(begin.Marker) != nil {
		return errs.New(errs.KindValidationFailed, "hierarchy deletion idempotency evidence is invalid")
	}
	return nil
}

func hierarchyDeletionTask(
	begin HierarchyDeletionBegin,
	scope workexecution.Scope,
	intent hierarchydeletion.HierarchyDeletionIntent,
) (workexecution.Task, error) {
	intentValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(
		intent,
		hierarchydeletion.HierarchyDeletionLargeRecordBytes,
	)
	if err != nil {
		return workexecution.Task{}, err
	}
	defer clear(intentValue)
	task := workexecution.Task{
		ID: begin.TaskID, Actor: workexecution.OperatorActor, Action: workexecution.ActionRemove,
		Scope:    scope,
		TargetID: begin.TargetID, TargetName: intent.RootSlug,
		Intent: workexecution.Input{
			ID:     ids.DeriveAt(ids.KindPlan, begin.CreatedAt, begin.TaskID, "hierarchy-removal"),
			SHA256: hierarchydeletion.HierarchyDeletionDigest(intentValue),
		},
		CreatedAt: begin.CreatedAt, UpdatedAt: begin.CreatedAt, Deadline: begin.DeadlineAt,
	}
	return task, task.Validate()
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

func hierarchyDeletionResponseDigest(taskID string) string {
	value := []byte(`{"task_id":"` + taskID + `"}`)
	return hierarchydeletion.HierarchyDeletionDigest(value)
}
