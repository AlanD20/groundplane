package etcd

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type runnerParents struct {
	tenant  etcdstore.Versioned[hierarchyrecord.TenantRecord]
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord]
}

func (repository *RunnerRepository) resolveRunnerParents(
	ctx context.Context,
	desired runnerrecord.RunnerDesiredRecord,
) (runnerParents, error) {
	if err := runnerrecord.ValidateRunnerDesired(desired); err != nil {
		return runnerParents{}, err
	}
	hierarchy, err := newHierarchyRepository(repository.store)
	if err != nil {
		return runnerParents{}, err
	}
	tenant, err := hierarchy.GetTenant(ctx, desired.TenantID)
	if err != nil {
		return runnerParents{}, err
	}
	parents := runnerParents{tenant: tenant}
	if desired.OwnerKind == runnerrecord.RunnerOwnerProject {
		parents.project, err = hierarchy.GetProject(ctx, desired.OwnerID)
		if err != nil {
			return runnerParents{}, err
		}
		if parents.project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
			parents.project.Record.TenantID != desired.TenantID {
			return runnerParents{}, errs.New(
				errs.KindValidationFailed,
				"runner project owner does not belong to its tenant",
			)
		}
	}
	return parents, nil
}
