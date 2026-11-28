package runneridentity

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core/runneraccess"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (admission *ScopedAdmission) resolveWorkspace(
	ctx context.Context, target runneraccess.Resource, revision int64,
) (runneraccess.Resource, []keyvalue.Condition, error) {
	if ids.Validate(ids.KindTenant, target.TenantID) != nil || ids.Validate(ids.KindProject, target.ProjectID) != nil ||
		ids.Validate(ids.KindEnvironment, target.EnvironmentID) != nil || revision <= 0 {
		return runneraccess.Resource{}, nil, forbidden()
	}
	entityKeys := []string{
		hierarchy.TenantKey(
			target.TenantID,
		), hierarchy.ProjectKey(target.ProjectID), hierarchy.EnvironmentKey(target.EnvironmentID),
	}
	tombstoneKeys := []string{
		deletions.TombstoneKey(string(deletions.DeletionTargetTenant), target.TenantID),
		deletions.TombstoneKey(string(deletions.DeletionTargetProject), target.ProjectID),
		deletions.TombstoneKey(string(deletions.DeletionTargetEnvironment), target.EnvironmentID),
	}
	snapshot, err := admission.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: append(entityKeys[:len(entityKeys):len(entityKeys)], tombstoneKeys...), Revision: revision,
	})
	if err != nil {
		return runneraccess.Resource{}, nil, err
	}
	if snapshot == nil || snapshot.ReadRevision != revision ||
		len(snapshot.Values) != len(entityKeys)+len(tombstoneKeys) {
		return runneraccess.Resource{}, nil, errs.New(errs.KindInternal, "Runner workspace storage read is incomplete")
	}
	defer keyvalue.ClearValues(snapshot.Values)
	conditions := make([]keyvalue.Condition, 0, len(snapshot.Values))
	for index, key := range entityKeys {
		value := snapshot.Values[index]
		if value == nil || snapshot.Values[index+len(entityKeys)] != nil {
			return runneraccess.Resource{}, nil, forbidden()
		}
		if value.Key != key || value.ModRevision <= 0 || value.ModRevision > revision {
			return runneraccess.Resource{}, nil, errs.New(errs.KindInternal, "Runner workspace storage read is invalid")
		}
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: value.ModRevision},
			keyvalue.Condition{Key: tombstoneKeys[index]})
	}
	tenant, err := hierarchy.DecodeTenant(snapshot.Values[0].Value)
	if err != nil {
		return runneraccess.Resource{}, nil, err
	}
	project, err := hierarchy.DecodeProject(snapshot.Values[1].Value)
	if err != nil {
		return runneraccess.Resource{}, nil, err
	}
	environment, err := hierarchy.DecodeEnvironment(snapshot.Values[2].Value)
	if err != nil {
		return runneraccess.Resource{}, nil, err
	}
	if project.Kind != hierarchy.ProjectKindTenant || project.TenantID != tenant.ID ||
		environment.ProjectID != project.ID ||
		tenant.ID != target.TenantID ||
		project.ID != target.ProjectID ||
		environment.ID != target.EnvironmentID ||
		tenant.DeletionTaskID != "" ||
		project.DeletionTaskID != "" ||
		environment.DeletionTaskID != "" {
		return runneraccess.Resource{}, nil, forbidden()
	}
	return runneraccess.Resource{
		TenantID: tenant.ID, ProjectID: project.ID, EnvironmentID: environment.ID,
	}, conditions, nil
}

func (admission *ScopedAdmission) resolveService(
	ctx context.Context, environmentID, serviceID string, revision int64,
) (keyvalue.Versioned[services.ServiceRecord], error) {
	declared, found, err := environmentqueries.ReadDeclaredResources(ctx, admission.store, environmentID, revision)
	if err != nil {
		return keyvalue.Versioned[services.ServiceRecord]{}, err
	}
	if !found {
		return keyvalue.Versioned[services.ServiceRecord]{}, forbidden()
	}
	service, err := services.ReadJoined(ctx, admission.store, services.DesiredSelection{
		Services: declared.Record.DesiredServices, Revision: declared.Revision, ReadRevision: declared.ReadRevision,
	}, serviceID, blueprints.EnvironmentBlueprintHeadKey(environmentID))
	if err != nil {
		return service, err
	}
	if service.Record.EnvironmentID != environmentID || service.Record.BackingNetworkID != "" {
		return keyvalue.Versioned[services.ServiceRecord]{}, forbidden()
	}
	return service, nil
}

func (admission *ScopedAdmission) resolveTask(
	ctx context.Context, workspace runneraccess.Resource, taskID string, revision int64,
) (runneraccess.Resource, workqueue.TaskSnapshot, error) {
	snapshot, _, err := admission.queue.ReadTask(ctx, taskID, revision)
	if err != nil {
		return runneraccess.Resource{}, workqueue.TaskSnapshot{}, err
	}
	scope := snapshot.Task.Scope
	if scope.TenantID != workspace.TenantID || scope.ProjectID != workspace.ProjectID ||
		scope.EnvironmentID != workspace.EnvironmentID || snapshot.Task.CollectionStartedAt != nil {
		return runneraccess.Resource{}, workqueue.TaskSnapshot{}, forbidden()
	}
	workspace.Kind, workspace.ID = ids.KindTask, snapshot.Task.ID
	return workspace, snapshot, nil
}
