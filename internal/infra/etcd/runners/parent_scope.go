package runners

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RunnerParents struct {
	tenant  etcdstore.Versioned[hierarchyrecord.TenantRecord]
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord]
}

func (repository *Reader) ResolveRunnerParents(
	ctx context.Context,
	desired RunnerDesiredRecord,
) (RunnerParents, error) {
	if err := ValidateRunnerDesired(desired); err != nil {
		return RunnerParents{}, err
	}
	if repository.store == nil {
		return RunnerParents{}, errs.New(errs.KindInternal, "hierarchy store is required")
	}
	hierarchy := hierarchyrecord.NewReader(repository.store)
	tenant, err := hierarchy.GetTenant(ctx, desired.TenantID)
	if err != nil {
		return RunnerParents{}, err
	}
	parents := RunnerParents{tenant: tenant}
	if desired.OwnerKind == RunnerOwnerProject {
		parents.project, err = hierarchy.GetProject(ctx, desired.OwnerID)
		if err != nil {
			return RunnerParents{}, err
		}
		if parents.project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
			parents.project.Record.TenantID != desired.TenantID {
			return RunnerParents{}, errs.New(
				errs.KindValidationFailed,
				"runner project owner does not belong to its tenant",
			)
		}
	}
	return parents, nil
}

func (parents RunnerParents) Tenant() etcdstore.Versioned[hierarchyrecord.TenantRecord] {
	return parents.tenant
}

func (parents RunnerParents) Project() etcdstore.Versioned[hierarchyrecord.ProjectRecord] {
	return parents.project
}
