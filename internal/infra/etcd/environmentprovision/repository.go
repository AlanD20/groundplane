package environmentprovision

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type reader interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
}

type Repository struct{ store reader }

func NewRepository(store reader) (*Repository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Environment provisioning storage is required")
	}
	return &Repository{store: store}, nil
}

// Load does not require a retained old Task or reconstruct a plan from current
// desired input. Every read is bound to the native scheduler's snapshot.
func (repository *Repository) Load(ctx context.Context, view workqueue.View) (Input, keyvalue.Condition, error) {
	if view.ReadRevision <= 0 {
		return Input{}, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Environment provisioning snapshot is unavailable")
	}
	key := InputKey(view.Item.Item.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if err != nil {
		return Input{}, keyvalue.Condition{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return Input{}, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Environment provisioning capture is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	input, err := Decode(read.Values[0].Value)
	if err != nil {
		return Input{}, keyvalue.Condition{}, err
	}
	if err := input.Match(view.Task.Task, view.Item.Item); err != nil {
		return Input{}, keyvalue.Condition{}, err
	}
	return input, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}

// PrepareResourceResult contributes only the provisioning-state change. The
// operation owner must join authenticated directory evidence and native attempt
// settlement before committing it. Mutable labels are preserved, not restored.
func (repository *Repository) PrepareResourceResult(ctx context.Context, view workqueue.View, succeeded bool) (workqueue.Change, error) {
	input, capture, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	key := hierarchy.EnvironmentKey(input.Environment.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Environment provisioning resource is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	current, err := hierarchy.DecodeEnvironment(read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	if current.ID != input.Environment.ID || current.ProjectID != input.Scope.ProjectID || current.VolumeDir != input.Environment.VolumeDir || current.CreateTaskID != input.TaskID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Environment provisioning resource ownership changed")
	}
	next, err := hierarchy.CompleteEnvironmentProvisioning(current, input.TaskID, succeeded)
	if err != nil {
		return workqueue.Change{}, err
	}
	raw, err := hierarchy.EncodeEnvironment(next)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{capture, {Key: key, ModRevision: read.Values[0].ModRevision}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: raw}}}, nil
}

// ReadyForHost refuses publication or hierarchy changes after assignment. Host
// effects require both this captured resource and the native start transaction.
func (repository *Repository) ReadyForHost(ctx context.Context, view workqueue.View) (Input, workqueue.Change, error) {
	input, capture, err := repository.Load(ctx, view)
	if err != nil {
		return Input{}, workqueue.Change{}, err
	}
	keys := []string{hierarchy.EnvironmentKey(input.Environment.ID), hierarchy.ProjectKey(input.Scope.ProjectID),
		hierarchy.TenantKey(input.Scope.TenantID), deletions.TombstoneKey("environment", input.Scope.EnvironmentID),
		deletions.TombstoneKey("project", input.Scope.ProjectID), deletions.TombstoneKey("tenant", input.Scope.TenantID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: view.ReadRevision})
	if err != nil {
		return Input{}, workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != len(keys) || read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil {
		return Input{}, workqueue.Change{}, errs.New(errs.KindStateConflict, "Environment provisioning hierarchy is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	environment, err := hierarchy.DecodeEnvironment(read.Values[0].Value)
	if err != nil {
		return Input{}, workqueue.Change{}, err
	}
	project, err := hierarchy.DecodeProject(read.Values[1].Value)
	if err != nil {
		return Input{}, workqueue.Change{}, err
	}
	tenant, err := hierarchy.DecodeTenant(read.Values[2].Value)
	if err != nil {
		return Input{}, workqueue.Change{}, err
	}
	if environment.ID != input.Environment.ID || environment.ProjectID != input.Scope.ProjectID || environment.VolumeDir != input.Environment.VolumeDir ||
		environment.CreateTaskID != input.TaskID || environment.ProvisioningState != hierarchy.EnvironmentProvisioningProvisioning || environment.DeletionTaskID != "" ||
		project.ID != input.Scope.ProjectID || project.TenantID != input.Scope.TenantID || project.Kind != hierarchy.ProjectKindTenant || project.DeletionTaskID != "" ||
		tenant.ID != input.Scope.TenantID || tenant.DeletionTaskID != "" ||
		read.Values[3] != nil || read.Values[4] != nil || read.Values[5] != nil || view.Item.Item.Operation != workexecution.Environment {
		return Input{}, workqueue.Change{}, errs.New(errs.KindStateConflict, "Environment provisioning no longer owns its hierarchy")
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{capture}}
	for index, key := range keys {
		condition := keyvalue.Condition{Key: key}
		if read.Values[index] != nil {
			condition.ModRevision = read.Values[index].ModRevision
		}
		change.Conditions = append(change.Conditions, condition)
	}
	return input, change, nil
}
