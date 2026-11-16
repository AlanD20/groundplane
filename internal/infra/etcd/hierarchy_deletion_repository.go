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
	TaskScope  workexecution.Scope
	TargetName string
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
		tenant, err := recordquery.Get(
			ctx,
			repository.store,
			hierarchyrecord.TenantKey(targetID),
			targetID,
			errs.KindTenantNotFound,
			hierarchyrecord.DecodeTenant,
			func(record hierarchyrecord.TenantRecord) string { return record.ID },
		)
		if err != nil {
			return HierarchyDeletionTargetResolution{}, err
		}
		return HierarchyDeletionTargetResolution{
			TargetKind: requested,
			ScopeKind:  idempotencyrecord.IdempotencyScopeTenant,
			ScopeID:    targetID,
			TaskScope:  workexecution.Scope{TenantID: targetID},
			TargetName: tenant.Record.Slug,
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
			TaskScope: workexecution.Scope{
				TenantID:  project.Record.TenantID,
				ProjectID: project.Record.ID,
			}, TargetName: project.Record.Slug,
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
			TaskScope:  workexecution.Scope{ProjectID: project.Record.ID}, TargetName: project.Record.Slug,
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
		parent, err := repository.store.GetMany(
			ctx,
			etcdstore.GetManyRequest{
				Keys:     []string{hierarchyrecord.ProjectKey(environment.Record.ProjectID)},
				Revision: environment.ReadRevision,
			},
		)
		if err != nil {
			return HierarchyDeletionTargetResolution{}, err
		}
		if parent == nil || parent.ReadRevision != environment.ReadRevision || len(parent.Values) != 1 ||
			parent.Values[0] == nil {
			return HierarchyDeletionTargetResolution{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		defer etcdstore.ClearValues(parent.Values)
		project, err := hierarchyrecord.DecodeProject(parent.Values[0].Value)
		if err != nil || project.ID != environment.Record.ProjectID {
			return HierarchyDeletionTargetResolution{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		return HierarchyDeletionTargetResolution{
			TargetKind: requested,
			ScopeKind:  idempotencyrecord.IdempotencyScopeProject,
			ScopeID:    project.ID,
			TargetName: environment.Record.Name,
			TaskScope: workexecution.Scope{
				TenantID:      project.TenantID,
				ProjectID:     project.ID,
				EnvironmentID: environment.Record.ID,
			},
		}, nil
	default:
		return HierarchyDeletionTargetResolution{}, errs.New(
			errs.KindValidationFailed,
			"hierarchy deletion target kind is invalid",
		)
	}
}

type HierarchyDeletionCapture struct {
	OperationID     string
	OperationKind   hierarchydeletion.HierarchyDeletionOperationKind
	TargetKind      hierarchydeletion.HierarchyDeletionTargetKind
	TargetID        string
	TaskID          string
	TaskCreatedAt   time.Time
	IdempotencyHash string
	Locator         idempotencyrecord.IdempotencyLocator
	CreatedAt       time.Time
	DeadlineAt      time.Time
}
type HierarchyDeletionCaptureResult struct {
	Intent     workexecution.Input
	Scope      workexecution.Scope
	TargetName string
	Authority  workqueue.Change
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

func (repository *HierarchyDeletionRepository) PrepareCapture(
	ctx context.Context,
	begin HierarchyDeletionCapture,
) (HierarchyDeletionCaptureResult, error) {
	if !hierarchydeletion.ValidHierarchyDeletionPrivateID(begin.OperationID, "del") ||
		!hierarchydeletion.ValidHierarchyDeletionOperation(begin.OperationKind, begin.TargetKind) ||
		!hierarchydeletion.ValidHierarchyDeletionTarget(begin.TargetKind, begin.TargetID) ||
		ids.Validate(
			ids.KindTask,
			begin.TaskID,
		) != nil || !hierarchydeletion.ValidHierarchyDeletionDigest(begin.IdempotencyHash) ||
		!hierarchydeletion.ValidHierarchyDeletionTimestamp(begin.CreatedAt) ||
		!hierarchydeletion.ValidHierarchyDeletionTimestamp(begin.TaskCreatedAt) ||
		begin.CreatedAt.Before(begin.TaskCreatedAt) || !begin.DeadlineAt.After(begin.CreatedAt) ||
		begin.DeadlineAt.After(begin.CreatedAt.Add(hierarchydeletion.AttemptTimeout)) ||
		idempotencyrecord.ValidateIdempotencyLocator(begin.Locator) != nil {
		return HierarchyDeletionCaptureResult{}, errs.New(
			errs.KindValidationFailed,
			"hierarchy capture identity or deadline is invalid",
		)
	}
	root, revision, err := repository.readDeletionRoot(ctx, begin.TargetKind, begin.TargetID)
	if err != nil {
		return HierarchyDeletionCaptureResult{}, err
	}
	defer clear(root.targetValue)
	operation, conditions, mutations, err := prepareHierarchyDeletionPublication(begin, root, revision)
	if err != nil {
		return HierarchyDeletionCaptureResult{}, err
	}
	defer etcdstore.ClearMutationValues(mutations)
	defer hierarchydeletion.ClearHierarchyDeletionOperation(operation)
	hostID, hostConditions, err := repository.captureDeletionHost(ctx, revision)
	if err != nil {
		return HierarchyDeletionCaptureResult{}, err
	}
	conditions = append(conditions, hostConditions...)
	binding, err := hierarchydeletion.EncodeTaskBinding(
		hierarchydeletion.TaskBinding{
			TaskID:      begin.TaskID,
			OperationID: begin.OperationID,
			Marker:      begin.Locator,
			HostID:      hostID,
		},
	)
	if err != nil {
		return HierarchyDeletionCaptureResult{}, err
	}
	defer clear(binding)
	conditions = append(conditions, etcdstore.Condition{Key: hierarchydeletion.TaskBindingKey(begin.TaskID)})
	mutations = append(
		mutations,
		etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   hierarchydeletion.TaskBindingKey(begin.TaskID),
			Value: binding,
		},
	)
	value, err := hierarchydeletion.EncodeHierarchyDeletionRecord(
		operation.Intent,
		hierarchydeletion.HierarchyDeletionLargeRecordBytes,
	)
	if err != nil {
		return HierarchyDeletionCaptureResult{}, err
	}
	defer clear(value)
	if err := hierarchydeletion.ValidateHierarchyDeletionTransaction(conditions, mutations, 64); err != nil {
		return HierarchyDeletionCaptureResult{}, err
	}
	owned := make([]etcdstore.Mutation, len(mutations))
	for index, mutation := range mutations {
		owned[index] = mutation
		owned[index].Value = append([]byte(nil), mutation.Value...)
	}
	return HierarchyDeletionCaptureResult{Scope: root.scope, TargetName: root.rootSlug,
		Intent: workexecution.Input{
			ID:     ids.DeriveAt(ids.KindPlan, begin.TaskCreatedAt, begin.TaskID, "hierarchy-removal"),
			SHA256: hierarchydeletion.HierarchyDeletionDigest(value),
		},
		Authority: workqueue.Change{Conditions: conditions, Mutations: owned}}, nil
}

func hierarchyDeletionResponseDigest(taskID string) string {
	value := []byte(`{"task_id":"` + taskID + `"}`)
	return hierarchydeletion.HierarchyDeletionDigest(value)
}
