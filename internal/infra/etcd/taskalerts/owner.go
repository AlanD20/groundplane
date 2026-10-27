package taskalerts

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Alert settings are operational metadata: recovery may remain unresolved, but
// deletion must fence a new setting from appearing behind parent finalization.
func (repository *Repository) OwnerConditions(
	ctx context.Context,
	scope string,
	revision int64,
) ([]keyvalue.Condition, error) {
	if !ValidScope(scope) {
		return nil, errs.New(errs.KindValidationFailed, "Task alert scope is invalid")
	}
	if scope == "platform" {
		return nil, nil
	}
	envRead, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{hierarchy.EnvironmentKey(scope)}, Revision: revision},
	)
	if err != nil {
		return nil, err
	}
	if envRead == nil || len(envRead.Values) != 1 || envRead.Values[0] == nil {
		return nil, errs.New(errs.KindEnvironmentNotFound, "Environment was not found")
	}
	defer keyvalue.ClearValues(envRead.Values)
	env, err := hierarchy.DecodeEnvironment(envRead.Values[0].Value)
	if err != nil {
		return nil, err
	}
	if env.DeletionTaskID != "" {
		return nil, errs.New(errs.KindResourceInUse, "Environment deletion is in progress")
	}
	projectRead, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{hierarchy.ProjectKey(env.ProjectID)}, Revision: envRead.ReadRevision},
	)
	if err != nil {
		return nil, err
	}
	if projectRead == nil || len(projectRead.Values) != 1 || projectRead.Values[0] == nil {
		return nil, errs.New(errs.KindProjectNotFound, "Project was not found")
	}
	defer keyvalue.ClearValues(projectRead.Values)
	project, err := hierarchy.DecodeProject(projectRead.Values[0].Value)
	if err != nil {
		return nil, err
	}
	if project.DeletionTaskID != "" {
		return nil, errs.New(errs.KindResourceInUse, "Project deletion is in progress")
	}
	conditions := []keyvalue.Condition{
		{Key: hierarchy.EnvironmentKey(scope), ModRevision: envRead.Values[0].ModRevision},
		{Key: hierarchy.ProjectKey(project.ID), ModRevision: projectRead.Values[0].ModRevision},
	}
	if project.TenantID != "" {
		read, err := repository.store.GetMany(
			ctx,
			keyvalue.GetManyRequest{
				Keys:     []string{hierarchy.TenantKey(project.TenantID)},
				Revision: envRead.ReadRevision,
			},
		)
		if err != nil {
			return nil, err
		}
		if read == nil || len(read.Values) != 1 || read.Values[0] == nil {
			return nil, errs.New(errs.KindTenantNotFound, "Tenant was not found")
		}
		defer keyvalue.ClearValues(read.Values)
		tenant, err := hierarchy.DecodeTenant(read.Values[0].Value)
		if err != nil {
			return nil, err
		}
		if tenant.DeletionTaskID != "" {
			return nil, errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
		}
		conditions = append(
			conditions,
			keyvalue.Condition{Key: hierarchy.TenantKey(tenant.ID), ModRevision: read.Values[0].ModRevision},
		)
	}
	return conditions, nil
}
