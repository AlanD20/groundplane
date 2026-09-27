package runners

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RunnerParents struct {
	tenant      etcdstore.Versioned[hierarchyrecord.TenantRecord]
	project     etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
}

func (repository *Reader) ResolveRunnerParents(
	ctx context.Context,
	desired RunnerDesiredRecord,
) (RunnerParents, error) {
	return repository.ResolveRunnerParentsAtRevision(ctx, desired, 0)
}

// ResolveRunnerParentsAtRevision reads the entire hierarchy and deletion fences
// from one snapshot, including when a retry is bound to an earlier revision.
func (repository *Reader) ResolveRunnerParentsAtRevision(
	ctx context.Context, desired RunnerDesiredRecord, revision int64,
) (RunnerParents, error) {
	if err := ValidateRunnerOwnership(desired); err != nil {
		return RunnerParents{}, err
	}
	if repository.store == nil {
		return RunnerParents{}, errs.New(errs.KindInternal, "hierarchy store is required")
	}
	parents := RunnerParents{}
	parents.tenant.Record.ID = desired.TenantID
	parents.project.Record.ID = desired.ProjectID()
	parents.environment.Record.ID = desired.EnvironmentID()
	fences := parents.fences()
	conditions := parents.AdmissionConditions()
	keys := make([]string, len(conditions))
	for index, condition := range conditions {
		keys[index] = condition.Key
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return RunnerParents{}, err
	}
	if read == nil || len(read.Values) != len(keys) || (revision != 0 && read.ReadRevision != revision) {
		return RunnerParents{}, errs.New(errs.KindInternal, "Runner parent snapshot is incomplete")
	}
	defer etcdstore.ClearValues(read.Values)
	for index, fence := range fences {
		value := read.Values[index*2]
		if value == nil {
			return RunnerParents{}, errs.New(fence.notFound, fence.label+" was not found")
		}
		if value.Key != keys[index*2] || value.ModRevision <= 0 {
			return RunnerParents{}, recordcodec.CorruptRecord()
		}
		if read.Values[index*2+1] != nil {
			return RunnerParents{}, errs.New(errs.KindResourceInUse, "Runner hierarchy deletion is in progress")
		}
	}
	parents.tenant.Record, err = hierarchyrecord.DecodeTenant(read.Values[0].Value)
	if err != nil || parents.tenant.Record.ID != desired.TenantID {
		return RunnerParents{}, recordcodec.CorruptRecord()
	}
	parents.tenant.Revision, parents.tenant.ReadRevision = read.Values[0].ModRevision, read.ReadRevision
	if desired.ProjectID() != "" {
		parents.project.Record, err = hierarchyrecord.DecodeProject(read.Values[2].Value)
		if err != nil || parents.project.Record.ID != desired.ProjectID() ||
			parents.project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
			parents.project.Record.TenantID != desired.TenantID {
			return RunnerParents{}, errs.New(
				errs.KindValidationFailed,
				"runner project owner does not belong to its tenant",
			)
		}
		parents.project.Revision, parents.project.ReadRevision = read.Values[2].ModRevision, read.ReadRevision
	}
	if desired.EnvironmentID() != "" {
		parents.environment.Record, err = hierarchyrecord.DecodeEnvironment(read.Values[4].Value)
		if err != nil || parents.environment.Record.ID != desired.EnvironmentID() ||
			parents.environment.Record.ProjectID != desired.ProjectID() {
			return RunnerParents{}, errs.New(
				errs.KindValidationFailed,
				"Runner Environment owner has a different Project",
			)
		}
		parents.environment.Revision, parents.environment.ReadRevision = read.Values[4].ModRevision, read.ReadRevision
	}
	return parents, nil
}

func (parents RunnerParents) Tenant() etcdstore.Versioned[hierarchyrecord.TenantRecord] {
	return parents.tenant
}

func (parents RunnerParents) Project() etcdstore.Versioned[hierarchyrecord.ProjectRecord] {
	return parents.project
}

func (parents RunnerParents) Environment() etcdstore.Versioned[hierarchyrecord.EnvironmentRecord] {
	return parents.environment
}
