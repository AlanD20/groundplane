package etcd

import (
	"bytes"
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TaskWorkspaceType is the immutable workspace catalog stored with every Task.
type TaskWorkspaceType string

const (
	TaskWorkspacePlatform TaskWorkspaceType = "platform"
	TaskWorkspaceTenant   TaskWorkspaceType = "tenant"
)

// TaskActor records whether an authenticated operator or the Controller
// initiated one Task attempt. It deliberately contains no token identity.
type TaskActor string

const (
	TaskActorOperator TaskActor = "operator"
	TaskActorSystem   TaskActor = "system"
)

// TaskOwner freezes the initiating product scope. Empty descendant ids are
// meaningful and therefore remain explicit strings rather than pointers.
type TaskOwner struct {
	WorkspaceType TaskWorkspaceType `json:"workspace_type"`
	TenantID      string            `json:"tenant_id,omitempty"`
	ProjectID     string            `json:"project_id,omitempty"`
	EnvironmentID string            `json:"environment_id,omitempty"`
}

// TaskInitiation is opaque proof of the durable context that initiated one
// Task publication. Repositories construct it from records they already fence;
// app services can only receive system initiation through TaskRepository.
type TaskInitiation struct {
	owner  TaskOwner
	actor  TaskActor
	fences []etcdstore.Condition
}

func (initiation TaskInitiation) Owner() TaskOwner { return initiation.owner }

func (initiation TaskInitiation) Actor() TaskActor { return initiation.actor }

func (initiation TaskInitiation) RetryScope() (TaskRetryScope, error) {
	record := TaskRecord{Owner: initiation.owner, Actor: initiation.actor}
	if err := validateTaskInitiation(record, initiation, false); err != nil {
		return TaskRetryScope{}, err
	}
	return taskOwnerRetryScope(initiation.owner)
}

func taskOwnerRetryScope(owner TaskOwner) (TaskRetryScope, error) {
	if err := validateTaskOwner(owner); err != nil {
		return TaskRetryScope{}, err
	}
	switch {
	case owner.EnvironmentID != "":
		return TaskRetryScope{Kind: idempotencyrecord.IdempotencyScopeEnvironment, ID: owner.EnvironmentID}, nil
	case owner.ProjectID != "":
		return TaskRetryScope{Kind: idempotencyrecord.IdempotencyScopeProject, ID: owner.ProjectID}, nil
	case owner.WorkspaceType == TaskWorkspaceTenant:
		return TaskRetryScope{Kind: idempotencyrecord.IdempotencyScopeTenant, ID: owner.TenantID}, nil
	default:
		return TaskRetryScope{Kind: idempotencyrecord.IdempotencyScopePlatform, ID: "-"}, nil
	}
}

func PlatformTaskOwner() TaskOwner {
	return TaskOwner{WorkspaceType: TaskWorkspacePlatform}
}

func TenantTaskOwner(tenantID string) (TaskOwner, error) {
	owner := TaskOwner{WorkspaceType: TaskWorkspaceTenant, TenantID: tenantID}
	if err := validateTaskOwner(owner); err != nil {
		return TaskOwner{}, err
	}
	return owner, nil
}

func TenantProjectTaskOwner(tenantID string, projectID string) (TaskOwner, error) {
	owner := TaskOwner{
		WorkspaceType: TaskWorkspaceTenant,
		TenantID:      tenantID,
		ProjectID:     projectID,
	}
	if err := validateTaskOwner(owner); err != nil {
		return TaskOwner{}, err
	}
	return owner, nil
}

func ProjectTaskOwner(project hierarchyrecord.ProjectRecord) (TaskOwner, error) {
	if ids.Validate(ids.KindProject, project.ID) != nil {
		return TaskOwner{}, errs.New(errs.KindValidationFailed, "task owner Project is invalid")
	}
	var owner TaskOwner
	switch project.Kind {
	case hierarchyrecord.ProjectKindTenant:
		owner = TaskOwner{
			WorkspaceType: TaskWorkspaceTenant,
			TenantID:      project.TenantID,
			ProjectID:     project.ID,
		}
	case hierarchyrecord.ProjectKindBacking:
		if project.TenantID != "" {
			return TaskOwner{}, errs.New(errs.KindValidationFailed, "backing Project task owner has a Tenant")
		}
		owner = TaskOwner{WorkspaceType: TaskWorkspacePlatform, ProjectID: project.ID}
	default:
		return TaskOwner{}, errs.New(errs.KindValidationFailed, "task owner Project kind is invalid")
	}
	if err := validateTaskOwner(owner); err != nil {
		return TaskOwner{}, err
	}
	return owner, nil
}

func EnvironmentTaskOwner(project hierarchyrecord.ProjectRecord, environment hierarchyrecord.EnvironmentRecord) (TaskOwner, error) {
	if ids.Validate(ids.KindEnvironment, environment.ID) != nil || environment.ProjectID != project.ID {
		return TaskOwner{}, errs.New(errs.KindValidationFailed, "task owner Environment hierarchy is invalid")
	}
	owner, err := ProjectTaskOwner(project)
	if err != nil {
		return TaskOwner{}, err
	}
	owner.EnvironmentID = environment.ID
	if err := validateTaskOwner(owner); err != nil {
		return TaskOwner{}, err
	}
	return owner, nil
}

func validateTaskOwner(owner TaskOwner) error {
	switch owner.WorkspaceType {
	case TaskWorkspacePlatform:
		if owner.TenantID != "" {
			return errs.New(errs.KindValidationFailed, "platform task owner cannot contain a Tenant")
		}
	case TaskWorkspaceTenant:
		if ids.Validate(ids.KindTenant, owner.TenantID) != nil {
			return errs.New(errs.KindValidationFailed, "tenant task owner requires a valid Tenant")
		}
	default:
		return errs.New(errs.KindValidationFailed, "task workspace_type is invalid")
	}
	if owner.ProjectID != "" && ids.Validate(ids.KindProject, owner.ProjectID) != nil {
		return errs.New(errs.KindValidationFailed, "task owner Project is invalid")
	}
	if owner.EnvironmentID != "" {
		if owner.ProjectID == "" || ids.Validate(ids.KindEnvironment, owner.EnvironmentID) != nil {
			return errs.New(errs.KindValidationFailed, "task owner Environment requires a valid Project")
		}
	}
	return nil
}

func newTaskInitiation(owner TaskOwner, actor TaskActor, fences ...etcdstore.Condition) (TaskInitiation, error) {
	if err := validateTaskOwner(owner); err != nil {
		return TaskInitiation{}, err
	}
	if !validTaskActor(actor) {
		return TaskInitiation{}, errs.New(errs.KindValidationFailed, "task initiation actor is invalid")
	}
	for _, fence := range fences {
		if fence.Key == "" || fence.ModRevision <= 0 {
			return TaskInitiation{}, errs.New(errs.KindInternal, "task initiation fence is invalid")
		}
	}
	return TaskInitiation{owner: owner, actor: actor, fences: slices.Clone(fences)}, nil
}

func newPlatformTaskInitiation(actor TaskActor) (TaskInitiation, error) {
	return newTaskInitiation(PlatformTaskOwner(), actor)
}

func newProjectTaskInitiation(
	tenant *etcdstore.Versioned[hierarchyrecord.TenantRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	actor TaskActor,
) (TaskInitiation, error) {
	owner, err := ProjectTaskOwner(project.Record)
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
	actor TaskActor,
) (TaskInitiation, error) {
	owner, err := EnvironmentTaskOwner(project.Record, environment.Record)
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
	actor TaskActor,
) (TaskInitiation, error) {
	owner, err := EnvironmentTaskOwner(project.Record, environment)
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
	actor TaskActor,
) (TaskInitiation, error) {
	if err := validateTaskRecord(parent.Record); err != nil {
		return TaskInitiation{}, err
	}
	if parent.Revision <= 0 {
		return TaskInitiation{}, errs.New(errs.KindValidationFailed, "task initiation parent revision is invalid")
	}
	return newTaskInitiation(
		parent.Record.Owner,
		actor,
		etcdstore.Condition{Key: taskKey(parent.Record.ID), ModRevision: parent.Revision},
	)
}

func validateTaskInitiation(record TaskRecord, initiation TaskInitiation, requireRecord bool) error {
	if err := validateTaskOwner(initiation.owner); err != nil || !validTaskActor(initiation.actor) {
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

func validTaskActor(actor TaskActor) bool {
	return actor == TaskActorOperator || actor == TaskActorSystem
}

func taskOwnerIndexKeys(owner TaskOwner, taskID string) ([]string, error) {
	if err := validateTaskOwner(owner); err != nil {
		return nil, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "task owner index task id is invalid")
	}
	workspaceKey := taskWorkspacePlatformIndexKey(taskID)
	if owner.WorkspaceType == TaskWorkspaceTenant {
		workspaceKey = taskWorkspaceTenantIndexKey(owner.TenantID, taskID)
	}
	keys := []string{workspaceKey}
	if owner.EnvironmentID != "" {
		keys = append(keys, taskEnvironmentIndexKey(owner.EnvironmentID, taskID))
	}
	return keys, nil
}

func prepareTaskOwnerIndexPlan(
	record TaskRecord,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
	classify idempotencyPlanClassifier,
) ([]etcdstore.Condition, []etcdstore.Mutation, idempotencyPlanClassifier, error) {
	if err := validateTaskRecord(record); err != nil {
		return nil, nil, nil, err
	}
	if classify == nil {
		return nil, nil, nil, errs.New(errs.KindInternal, "task owner index classifier is required")
	}
	taskConditionIndex := -1
	for index, condition := range conditions {
		if condition.Key == taskKey(record.ID) {
			if taskConditionIndex >= 0 || condition.ModRevision != 0 {
				return nil, nil, nil, errs.New(errs.KindInternal, "task publication compare is invalid")
			}
			taskConditionIndex = index
		}
	}
	if taskConditionIndex < 0 {
		return nil, nil, nil, errs.New(errs.KindInternal, "task publication compare is missing")
	}
	encoded, err := encodeTaskRecord(record)
	if err != nil {
		return nil, nil, nil, err
	}
	defer clear(encoded)
	matchedMutation := false
	for _, mutation := range mutations {
		if mutation.Key != taskKey(record.ID) {
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
