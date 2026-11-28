package githubconnection

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ownerGuard captures the complete owner ancestry at one store revision.
// Owner deletion is independently fenced even before hierarchy cleanup knows
// how to retire a connection.
func (repository *Repository) ownerGuard(ctx context.Context, scope Scope, at int64) ([]keyvalue.Condition, error) {
	conditions := make([]keyvalue.Condition, 0, 6)
	for scope.Kind != "" {
		if err := scope.validate(); err != nil {
			return nil, err
		}
		primary, tombstone := ownerKeys(scope)
		result, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
			Keys: []string{primary, tombstone}, Revision: at,
		})
		if err != nil {
			return nil, err
		}
		if result == nil {
			return nil, corruption()
		}
		defer keyvalue.ClearValues(result.Values)
		if len(result.Values) != 2 || result.ReadRevision <= 0 || at > 0 && at != result.ReadRevision {
			return nil, corruption()
		}
		at = result.ReadRevision
		entry := result.Values[0]
		if entry == nil {
			return nil, errs.New(errs.KindRequestNotFound, "GitHub connection owner was not found")
		}
		if entry.Key != primary || entry.ModRevision <= 0 || entry.ModRevision > at {
			return nil, corruption()
		}
		if result.Values[1] != nil {
			return nil, ownerDeleting()
		}
		conditions = append(conditions,
			keyvalue.Condition{Key: primary, ModRevision: entry.ModRevision},
			keyvalue.Condition{Key: tombstone},
		)
		scope, err = ownerParent(scope, entry.Value)
		if err != nil {
			return nil, err
		}
	}
	return conditions, nil
}

func ownerKeys(scope Scope) (string, string) {
	switch scope.Kind {
	case ids.KindTenant:
		return hierarchy.TenantKey(scope.ID), deletions.TombstoneKey(string(deletions.DeletionTargetTenant), scope.ID)
	case ids.KindProject:
		return hierarchy.ProjectKey(scope.ID), deletions.TombstoneKey(string(deletions.DeletionTargetProject), scope.ID)
	default:
		return hierarchy.EnvironmentKey(
				scope.ID,
			), deletions.TombstoneKey(
				string(deletions.DeletionTargetEnvironment),
				scope.ID,
			)
	}
}

func ownerParent(scope Scope, value []byte) (Scope, error) {
	switch scope.Kind {
	case ids.KindTenant:
		tenant, err := hierarchy.DecodeTenant(value)
		if err != nil || tenant.ID != scope.ID {
			return Scope{}, corruption()
		}
		if tenant.DeletionTaskID != "" {
			return Scope{}, ownerDeleting()
		}
		return Scope{}, nil
	case ids.KindProject:
		project, err := hierarchy.DecodeProject(value)
		if err != nil || project.ID != scope.ID {
			return Scope{}, corruption()
		}
		if project.DeletionTaskID != "" {
			return Scope{}, ownerDeleting()
		}
		if project.Kind != hierarchy.ProjectKindTenant {
			return Scope{}, errs.New(errs.KindValidationFailed, "GitHub connections require a tenant-owned project")
		}
		return Scope{Kind: ids.KindTenant, ID: project.TenantID}, nil
	default:
		environment, err := hierarchy.DecodeEnvironment(value)
		if err != nil || environment.ID != scope.ID {
			return Scope{}, corruption()
		}
		if environment.DeletionTaskID != "" {
			return Scope{}, ownerDeleting()
		}
		return Scope{Kind: ids.KindProject, ID: environment.ProjectID}, nil
	}
}

func ownerDeleting() error {
	return errs.New(errs.KindResourceInUse, "GitHub connection owner deletion is in progress")
}
