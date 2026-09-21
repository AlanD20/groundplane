package etcd

import (
	"bytes"
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TaskInitiation is opaque proof of the durable context that initiated one
// Task publication. Repositories construct it from records they already fence;
// app services can only receive system initiation through TaskRepository.
type TaskInitiation struct {
	owner  taskjournal.TaskOwner
	actor  taskjournal.TaskActor
	fences []etcdstore.Condition
}

func (initiation TaskInitiation) Owner() taskjournal.TaskOwner { return initiation.owner }

func (initiation TaskInitiation) Actor() taskjournal.TaskActor { return initiation.actor }

func (initiation TaskInitiation) RetryScope() (TaskRetryScope, error) {
	record := TaskRecord{Owner: initiation.owner, Actor: initiation.actor}
	if err := validateTaskInitiation(record, initiation, false); err != nil {
		return TaskRetryScope{}, err
	}
	return taskOwnerRetryScope(initiation.owner)
}

func taskOwnerRetryScope(owner taskjournal.TaskOwner) (TaskRetryScope, error) {
	if err := taskjournal.ValidateOwner(owner); err != nil {
		return TaskRetryScope{}, err
	}
	switch {
	case owner.EnvironmentID != "":
		return TaskRetryScope{Kind: idempotencyrecord.IdempotencyScopeEnvironment, ID: owner.EnvironmentID}, nil
	case owner.ProjectID != "":
		return TaskRetryScope{Kind: idempotencyrecord.IdempotencyScopeProject, ID: owner.ProjectID}, nil
	case owner.WorkspaceType == taskjournal.TaskWorkspaceTenant:
		return TaskRetryScope{Kind: idempotencyrecord.IdempotencyScopeTenant, ID: owner.TenantID}, nil
	default:
		return TaskRetryScope{Kind: idempotencyrecord.IdempotencyScopePlatform, ID: "-"}, nil
	}
}

func newTaskInitiation(owner taskjournal.TaskOwner, actor taskjournal.TaskActor, fences ...etcdstore.Condition) (TaskInitiation, error) {
	if err := taskjournal.ValidateOwner(owner); err != nil {
		return TaskInitiation{}, err
	}
	if !taskjournal.ValidActor(actor) {
		return TaskInitiation{}, errs.New(errs.KindValidationFailed, "task initiation actor is invalid")
	}
	for _, fence := range fences {
		if fence.Key == "" || fence.ModRevision <= 0 {
			return TaskInitiation{}, errs.New(errs.KindInternal, "task initiation fence is invalid")
		}
	}
	return TaskInitiation{owner: owner, actor: actor, fences: slices.Clone(fences)}, nil
}

func newPlatformTaskInitiation(actor taskjournal.TaskActor) (TaskInitiation, error) {
	return newTaskInitiation(taskjournal.PlatformTaskOwner(), actor)
}

func newProjectTaskInitiation(
	tenant *etcdstore.Versioned[hierarchyrecord.TenantRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	actor taskjournal.TaskActor,
) (TaskInitiation, error) {
	owner, err := taskjournal.ProjectTaskOwner(project.Record)
	if err != nil {
		return TaskInitiation{}, err
	}
	if project.Revision <= 0 {
		return TaskInitiation{}, errs.New(errs.KindValidationFailed, "task initiation project revision is invalid")
	}
	ancestry, err := taskInitiationTenantFence(tenant, project)
	if err != nil {
		return TaskInitiation{}, err
	}
	ancestry = append(ancestry, etcdstore.Condition{Key: hierarchyrecord.ProjectKey(project.Record.ID), ModRevision: project.Revision})
	return newTaskInitiation(owner, actor, ancestry...)
}

func newEnvironmentTaskInitiation(
	tenant *etcdstore.Versioned[hierarchyrecord.TenantRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	actor taskjournal.TaskActor,
) (TaskInitiation, error) {
	owner, err := taskjournal.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return TaskInitiation{}, err
	}
	if project.Revision <= 0 || environment.Revision <= 0 {
		return TaskInitiation{}, errs.New(errs.KindValidationFailed, "task initiation hierarchy revision is invalid")
	}
	ancestry, err := taskInitiationTenantFence(tenant, project)
	if err != nil {
		return TaskInitiation{}, err
	}
	ancestry = append(
		ancestry,
		etcdstore.Condition{Key: hierarchyrecord.ProjectKey(project.Record.ID), ModRevision: project.Revision},
		etcdstore.Condition{Key: hierarchyrecord.EnvironmentKey(environment.Record.ID), ModRevision: environment.Revision},
	)
	return newTaskInitiation(owner, actor, ancestry...)
}

func newEnvironmentCreationTaskInitiation(
	tenant *etcdstore.Versioned[hierarchyrecord.TenantRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment hierarchyrecord.EnvironmentRecord,
	actor taskjournal.TaskActor,
) (TaskInitiation, error) {
	owner, err := taskjournal.EnvironmentTaskOwner(project.Record, environment)
	if err != nil {
		return TaskInitiation{}, err
	}
	if project.Revision <= 0 {
		return TaskInitiation{}, errs.New(errs.KindValidationFailed, "task initiation project revision is invalid")
	}
	ancestry, err := taskInitiationTenantFence(tenant, project)
	if err != nil {
		return TaskInitiation{}, err
	}
	ancestry = append(ancestry, etcdstore.Condition{Key: hierarchyrecord.ProjectKey(project.Record.ID), ModRevision: project.Revision})
	return newTaskInitiation(owner, actor, ancestry...)
}

func loadTaskInitiationTenant(
	ctx context.Context,
	store interface {
		Get(context.Context, string) (*etcdstore.GetResult, error)
	},
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
) (*etcdstore.Versioned[hierarchyrecord.TenantRecord], error) {
	if project.Record.Kind == hierarchyrecord.ProjectKindBacking {
		return nil, nil
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant || ids.Validate(ids.KindTenant, project.Record.TenantID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "task initiation project ancestry is invalid")
	}
	result, err := store.Get(ctx, hierarchyrecord.TenantKey(project.Record.TenantID))
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errs.New(errs.KindInternal, "task initiation tenant read is missing")
	}
	if result.Entry == nil {
		return nil, errs.New(errs.KindStateConflict, "task initiation tenant is missing")
	}
	tenant, err := hierarchyrecord.DecodeTenant(result.Entry.Value)
	if err != nil {
		return nil, err
	}
	if result.Entry.Key != hierarchyrecord.TenantKey(project.Record.TenantID) || tenant.ID != project.Record.TenantID {
		return nil, errs.New(errs.KindInternal, "task initiation tenant is corrupt")
	}
	return &etcdstore.Versioned[hierarchyrecord.TenantRecord]{
		Record: tenant, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func taskInitiationTenantFence(
	tenant *etcdstore.Versioned[hierarchyrecord.TenantRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
) ([]etcdstore.Condition, error) {
	switch project.Record.Kind {
	case hierarchyrecord.ProjectKindTenant:
		if tenant == nil || tenant.Record.ID != project.Record.TenantID || tenant.Revision <= 0 {
			return nil, errs.New(errs.KindValidationFailed, "task initiation tenant ancestry is invalid")
		}
		return []etcdstore.Condition{{Key: hierarchyrecord.TenantKey(tenant.Record.ID), ModRevision: tenant.Revision}}, nil
	case hierarchyrecord.ProjectKindBacking:
		if tenant != nil || project.Record.TenantID != "" {
			return nil, errs.New(errs.KindValidationFailed, "task initiation backing project ancestry is invalid")
		}
		return nil, nil
	default:
		return nil, errs.New(errs.KindValidationFailed, "task initiation project kind is invalid")
	}
}

func newInheritedTaskInitiation(
	parent etcdstore.Versioned[TaskRecord],
	actor taskjournal.TaskActor,
) (TaskInitiation, error) {
	if err := ValidateTaskRecord(parent.Record); err != nil {
		return TaskInitiation{}, err
	}
	if parent.Revision <= 0 {
		return TaskInitiation{}, errs.New(errs.KindValidationFailed, "task initiation parent revision is invalid")
	}
	return newTaskInitiation(
		parent.Record.Owner,
		actor,
		etcdstore.Condition{Key: taskjournal.TaskStorageKey(parent.Record.ID), ModRevision: parent.Revision},
	)
}

func validateTaskInitiation(record TaskRecord, initiation TaskInitiation, requireRecord bool) error {
	if err := taskjournal.ValidateOwner(initiation.owner); err != nil || !taskjournal.ValidActor(initiation.actor) {
		return errs.New(errs.KindInternal, "task initiation is invalid")
	}
	if requireRecord && (record.Owner != initiation.owner || record.Actor != initiation.actor) {
		return errs.New(errs.KindValidationFailed, "task owner or actor does not match its initiation")
	}
	return nil
}

func prepareTaskInitiationFences(
	initiation TaskInitiation,
	conditions []etcdstore.Condition,
	classify idempotencyPlanClassifier,
) ([]etcdstore.Condition, idempotencyPlanClassifier, error) {
	if classify == nil {
		return nil, nil, errs.New(errs.KindInternal, "task initiation classifier is required")
	}
	baseConditionCount := len(conditions)
	appended := make([]etcdstore.Condition, 0, len(initiation.fences))
	for _, required := range initiation.fences {
		found := false
		for _, condition := range conditions {
			if condition.Key != required.Key {
				continue
			}
			if condition.ModRevision != required.ModRevision || condition.Prefix {
				return nil, nil, errs.New(errs.KindInternal, "task publication has a conflicting initiation fence")
			}
			found = true
			break
		}
		if !found {
			conditions = append(conditions, required)
			appended = append(appended, required)
		}
	}
	wrapped := func(revision int64, values []*etcdstore.KeyValue) error {
		if len(values) != baseConditionCount+len(appended) {
			return errs.New(errs.KindInternal, "task initiation compare evidence is incomplete")
		}
		for index, fence := range appended {
			value := values[baseConditionCount+index]
			if value == nil || value.Key != fence.Key || value.ModRevision != fence.ModRevision {
				return errs.New(errs.KindStateConflict, "task initiation ancestry changed")
			}
		}
		return classify(revision, values[:baseConditionCount])
	}
	return conditions, wrapped, nil
}

func taskOwnerIndexKeys(owner taskjournal.TaskOwner, taskID string) ([]string, error) {
	if err := taskjournal.ValidateOwner(owner); err != nil {
		return nil, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "task owner index task id is invalid")
	}
	workspaceKey := taskjournal.TaskWorkspacePlatformIndexKey(taskID)
	if owner.WorkspaceType == taskjournal.TaskWorkspaceTenant {
		workspaceKey = taskjournal.TaskWorkspaceTenantIndexKey(owner.TenantID, taskID)
	}
	keys := []string{workspaceKey}
	if owner.EnvironmentID != "" {
		keys = append(keys, taskjournal.TaskEnvironmentIndexKey(owner.EnvironmentID, taskID))
	}
	return keys, nil
}

func prepareTaskOwnerIndexPlan(
	record TaskRecord,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
	classify idempotencyPlanClassifier,
) ([]etcdstore.Condition, []etcdstore.Mutation, idempotencyPlanClassifier, error) {
	if err := ValidateTaskRecord(record); err != nil {
		return nil, nil, nil, err
	}
	if classify == nil {
		return nil, nil, nil, errs.New(errs.KindInternal, "task owner index classifier is required")
	}
	taskConditionIndex := -1
	for index, condition := range conditions {
		if condition.Key == taskjournal.TaskStorageKey(record.ID) {
			if taskConditionIndex >= 0 || condition.ModRevision != 0 {
				return nil, nil, nil, errs.New(errs.KindInternal, "task publication compare is invalid")
			}
			taskConditionIndex = index
		}
	}
	if taskConditionIndex < 0 {
		return nil, nil, nil, errs.New(errs.KindInternal, "task publication compare is missing")
	}
	encoded, err := EncodeTaskRecord(record)
	if err != nil {
		return nil, nil, nil, err
	}
	defer clear(encoded)
	matchedMutation := false
	for _, mutation := range mutations {
		if mutation.Key != taskjournal.TaskStorageKey(record.ID) {
			continue
		}
		if matchedMutation || mutation.Type != etcdstore.MutationPut || mutation.Prefix || !bytes.Equal(mutation.Value, encoded) {
			return nil, nil, nil, errs.New(errs.KindInternal, "task publication mutation is invalid")
		}
		matchedMutation = true
	}
	if !matchedMutation {
		return nil, nil, nil, errs.New(errs.KindInternal, "task publication mutation is missing")
	}
	indexKeys, err := taskJournalIndexKeys(record)
	if err != nil {
		return nil, nil, nil, err
	}
	baseConditionCount := len(conditions)
	indexValue := []byte(record.ID)
	for _, key := range indexKeys {
		conditions = append(conditions, etcdstore.Condition{Key: key})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: indexValue})
	}
	wrapped := func(revision int64, values []*etcdstore.KeyValue) error {
		if len(values) != baseConditionCount+len(indexKeys) {
			return errs.New(errs.KindInternal, "task owner index compare evidence is incomplete")
		}
		taskExists := values[taskConditionIndex] != nil
		for index := range indexKeys {
			value := values[baseConditionCount+index]
			if (value != nil) != taskExists {
				return errs.New(errs.KindInternal, "task owner index membership is corrupt")
			}
			if value != nil && (value.Key != indexKeys[index] || string(value.Value) != record.ID) {
				return errs.New(errs.KindInternal, "task owner index membership is corrupt")
			}
		}
		return classify(revision, values[:baseConditionCount])
	}
	return conditions, mutations, wrapped, nil
}
