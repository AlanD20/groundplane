package releasequeries

import (
	"bytes"
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	deletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	environmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReleasePlanningScope is one fixed-revision view of every ancestor and
// render input shared by all candidates in an operation.
type ReleasePlanningScope struct {
	Environment              etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	Project                  etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	Tenant                   etcdstore.Versioned[hierarchyrecord.TenantRecord]
	Compose                  etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]
	EnvironmentEpochRevision int64
	EnvironmentEpochValue    []byte
	ReadRevision             int64
}

type ReleasePlanningService struct {
	Service            etcdstore.Versioned[servicerecord.ServiceRecord]
	Projection         domain.ServiceProjection
	ProjectionRevision int64
}

func (scope ReleasePlanningScope) Clone() ReleasePlanningScope {
	clone := scope
	clone.EnvironmentEpochValue = slices.Clone(scope.EnvironmentEpochValue)
	clone.Compose.Record = projectionrecord.CloneEnvironmentComposeProjection(scope.Compose.Record)
	return clone
}

func (ledger *Reader) LoadPlanningScope(
	ctx context.Context,
	environmentID string,
) (ReleasePlanningScope, error) {
	return ledger.loadPlanningScope(ctx, environmentID, 0)
}

func (ledger *Reader) LoadPlanningScopeAtRevision(
	ctx context.Context,
	environmentID string,
	revision int64,
) (ReleasePlanningScope, error) {
	if revision <= 0 {
		return ReleasePlanningScope{}, errs.New(errs.KindValidationFailed, "release planning revision is invalid")
	}
	return ledger.loadPlanningScope(ctx, environmentID, revision)
}

func (ledger *Reader) loadPlanningScope(
	ctx context.Context,
	environmentID string,
	revision int64,
) (ReleasePlanningScope, error) {
	if ctx == nil || ledger == nil || ledger.store == nil || ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return ReleasePlanningScope{}, errs.New(errs.KindValidationFailed, "release planning environment is invalid")
	}
	var initial *etcdstore.GetResult
	var err error
	if revision == 0 {
		initial, err = ledger.store.Get(ctx, hierarchyrecord.EnvironmentKey(environmentID))
	} else {
		loaded, loadErr := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{hierarchyrecord.EnvironmentKey(environmentID)}, Revision: revision})
		if loadErr != nil {
			return ReleasePlanningScope{}, loadErr
		}
		if loaded == nil || loaded.ReadRevision != revision || len(loaded.Values) != 1 {
			return ReleasePlanningScope{}, releases.CorruptReleaseRecord()
		}
		initial = &etcdstore.GetResult{Entry: loaded.Values[0], ReadRevision: loaded.ReadRevision}
	}
	if err != nil {
		return ReleasePlanningScope{}, err
	}
	if initial == nil || initial.ReadRevision <= 0 {
		return ReleasePlanningScope{}, releases.CorruptReleaseRecord()
	}
	if initial.Entry == nil {
		return ReleasePlanningScope{}, errs.New(errs.KindEnvironmentNotFound, "environment was not found")
	}
	environment, err := hierarchyrecord.DecodeEnvironment(initial.Entry.Value)
	if err != nil || environment.ID != environmentID || environment.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return ReleasePlanningScope{}, releases.CorruptReleaseRecord()
	}
	projectRead, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{hierarchyrecord.ProjectKey(environment.ProjectID)}, Revision: initial.ReadRevision,
	})
	if err != nil {
		return ReleasePlanningScope{}, err
	}
	if projectRead == nil || projectRead.ReadRevision != initial.ReadRevision || len(projectRead.Values) != 1 ||
		projectRead.Values[0] == nil {
		return ReleasePlanningScope{}, releases.CorruptReleaseRecord()
	}
	project, err := hierarchyrecord.DecodeProject(projectRead.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID || project.Kind != hierarchyrecord.ProjectKindTenant {
		return ReleasePlanningScope{}, releases.CorruptReleaseRecord()
	}
	compose, found, err := blueprints.ReadProjectionAtRevision(ctx, ledger.store, environmentID, initial.ReadRevision)
	if err != nil {
		return ReleasePlanningScope{}, err
	}
	if !found || compose.ReadRevision != initial.ReadRevision {
		return ReleasePlanningScope{}, releases.CorruptReleaseRecord()
	}
	keys := []string{
		hierarchyrecord.EnvironmentKey(environmentID), hierarchyrecord.ProjectKey(project.ID), hierarchyrecord.TenantKey(project.TenantID),
		hierarchyrecord.EnvironmentMutationEpochKey(environmentID),
		hierarchyrecord.EnvironmentOperationLockKey(environmentID), releases.ReleaseFenceSetKey(environmentID),
		deletions.TombstoneKey("environment", environmentID), deletions.TombstoneKey("project", project.ID),
		deletions.TombstoneKey("tenant", project.TenantID),
	}
	loaded, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: initial.ReadRevision})
	if err != nil {
		return ReleasePlanningScope{}, err
	}
	if loaded == nil || loaded.ReadRevision != initial.ReadRevision || len(loaded.Values) != len(keys) {
		return ReleasePlanningScope{}, releases.CorruptReleaseRecord()
	}
	for _, index := range []int{0, 1, 2, 3} {
		if loaded.Values[index] == nil {
			return ReleasePlanningScope{}, releases.CorruptReleaseRecord()
		}
	}
	if loaded.Values[4] != nil || loaded.Values[5] != nil || loaded.Values[6] != nil || loaded.Values[7] != nil ||
		loaded.Values[8] != nil {
		return ReleasePlanningScope{}, errs.New(errs.KindResourceInUse, "release planning scope is locked or deleting")
	}
	environment, err = hierarchyrecord.DecodeEnvironment(loaded.Values[0].Value)
	if err != nil || environment.ID != environmentID || environment.ProjectID != project.ID {
		return ReleasePlanningScope{}, releases.CorruptReleaseRecord()
	}
	project, err = hierarchyrecord.DecodeProject(loaded.Values[1].Value)
	if err != nil || project.ID != environment.ProjectID || project.Kind != hierarchyrecord.ProjectKindTenant {
		return ReleasePlanningScope{}, releases.CorruptReleaseRecord()
	}
	tenant, err := hierarchyrecord.DecodeTenant(loaded.Values[2].Value)
	if err != nil || tenant.ID != project.TenantID {
		return ReleasePlanningScope{}, releases.CorruptReleaseRecord()
	}
	epoch, err := backupruntime.DecodeEnvironmentMutationEpochRecord(loaded.Values[3].Value)
	if err != nil || epoch.EnvironmentID != environmentID {
		return ReleasePlanningScope{}, releases.CorruptReleaseRecord()
	}
	return ReleasePlanningScope{
		Environment: etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{
			Record:       environment,
			Revision:     loaded.Values[0].ModRevision,
			ReadRevision: loaded.ReadRevision,
		},
		Project: etcdstore.Versioned[hierarchyrecord.ProjectRecord]{
			Record:       project,
			Revision:     loaded.Values[1].ModRevision,
			ReadRevision: loaded.ReadRevision,
		},
		Tenant: etcdstore.Versioned[hierarchyrecord.TenantRecord]{
			Record:       tenant,
			Revision:     loaded.Values[2].ModRevision,
			ReadRevision: loaded.ReadRevision,
		},
		Compose:                  compose,
		EnvironmentEpochRevision: loaded.Values[3].ModRevision,
		EnvironmentEpochValue:    slices.Clone(loaded.Values[3].Value), ReadRevision: loaded.ReadRevision,
	}, nil
}

func (ledger *Reader) LoadPlanningServices(
	ctx context.Context,
	scope ReleasePlanningScope,
	serviceIDs []string,
) ([]ReleasePlanningService, error) {
	if ctx == nil || ledger == nil || scope.ReadRevision <= 0 || len(serviceIDs) == 0 ||
		len(serviceIDs) > releases.MaximumReleasePublicationMembers {
		return nil, errs.New(errs.KindValidationFailed, "release planning service selection is invalid")
	}
	keys := make([]string, 0, len(serviceIDs)*2)
	seen := make(map[string]struct{}, len(serviceIDs))
	for _, serviceID := range serviceIDs {
		if ids.Validate(ids.KindService, serviceID) != nil {
			return nil, errs.New(errs.KindValidationFailed, "release planning service id is invalid")
		}
		if _, duplicate := seen[serviceID]; duplicate {
			return nil, errs.New(errs.KindValidationFailed, "release planning service is duplicated")
		}
		seen[serviceID] = struct{}{}
		keys = append(keys, deletions.TombstoneKey("service", serviceID), releases.ReleaseProjectionKey(serviceID))
	}
	loaded, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: scope.ReadRevision})
	if err != nil {
		return nil, err
	}
	if loaded == nil || loaded.ReadRevision != scope.ReadRevision || len(loaded.Values) != len(keys) {
		return nil, releases.CorruptReleaseRecord()
	}
	projected := make(map[string]struct{}, len(scope.Compose.Record.DesiredServices))
	for _, desired := range scope.Compose.Record.DesiredServices {
		projected[desired.Desired.ID] = struct{}{}
	}
	result := make([]ReleasePlanningService, len(serviceIDs))
	for index, serviceID := range serviceIDs {
		base := index * 2
		if loaded.Values[base] != nil {
			return nil, errs.New(errs.KindResourceInUse, "release service ownership changed or is deleting")
		}
		service, serviceErr := environmentqueries.FindServiceAtRevision(ctx, ledger.store, serviceID, scope.ReadRevision)
		if serviceErr != nil {
			return nil, serviceErr
		}
		if service.Record.EnvironmentID != scope.Environment.Record.ID {
			return nil, errs.New(errs.KindServiceNotFound, "service was not found")
		}
		_, desired := projected[serviceID]
		if !desired && !releasePlanningTargetsGeneratedService(scope.Compose.Record.Components, serviceID) {
			return nil, errs.New(errs.KindValidationFailed, "release service is not in the applied Compose projection")
		}
		planning := ReleasePlanningService{Service: service}
		if loaded.Values[base+1] != nil {
			projection, err := releases.DecodeReleaseRecord[domain.ServiceProjection](
				loaded.Values[base+1].Value,
				"service-release-projection",
			)
			if err != nil || projection.EnvironmentID != scope.Environment.Record.ID ||
				projection.ServiceID != serviceID {
				return nil, releases.CorruptReleaseRecord()
			}
			planning.Projection = projection
			planning.ProjectionRevision = loaded.Values[base+1].ModRevision
		}
		result[index] = planning
	}
	return result, nil
}

func releasePlanningTargetsGeneratedService(components []componentrecord.Record, serviceID string) bool {
	for _, component := range components {
		for _, generatedServiceID := range component.Runtime.GeneratedServices {
			if generatedServiceID == serviceID {
				return true
			}
		}
	}
	return false
}

func (ledger *Reader) GetPlanningServingIntent(
	ctx context.Context,
	scope ReleasePlanningScope,
	service ReleasePlanningService,
) (domain.Intent, bool, error) {
	if service.Projection.ServingReleaseID == "" {
		return domain.Intent{}, false, nil
	}
	index, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		releases.ReleaseServiceIndexKey(
			scope.Environment.Record.ID,
			service.Service.Record.Desired.ID,
			service.Projection.ServingReleaseID,
		),
	}, Revision: scope.ReadRevision})
	if err != nil {
		return domain.Intent{}, false, err
	}
	if index == nil || index.ReadRevision != scope.ReadRevision || len(index.Values) != 1 || index.Values[0] == nil {
		return domain.Intent{}, false, releases.CorruptReleaseRecord()
	}
	publicationID, err := DecodeReleaseIndex(index.Values[0].Value, service.Service.Record.Desired.ID)
	if err != nil {
		return domain.Intent{}, false, err
	}
	view, err := ledger.ReadViewAt(ctx, publicationID, service.Projection.ServingReleaseID, scope.ReadRevision)
	if err != nil {
		return domain.Intent{}, false, err
	}
	return view.Intent, true, nil
}

func (ledger *Reader) rejectSelectedHooks(
	ctx context.Context,
	scope ReleasePlanningScope,
	services map[string]struct{},
) error {
	active, err := scriptrecord.ReadActiveScriptSet(ctx, ledger.store, scope.Environment.Record.ID, scope.ReadRevision)
	if err != nil {
		return err
	}
	prefix := scriptrecord.ScriptSetOwnerPrefix(scope.Environment.Record.ID, active.Record.GenerationID)
	start := ""
	for {
		page, err := ledger.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: prefix, StartExclusive: start, Limit: 200, Revision: scope.ReadRevision,
		})
		if err != nil {
			return err
		}
		if page == nil || page.ReadRevision != scope.ReadRevision {
			return releases.CorruptReleaseRecord()
		}
		if len(page.Values) != 0 {
			keys := make([]string, len(page.Values))
			for index, value := range page.Values {
				scriptID := strings.TrimPrefix(value.Key, prefix)
				if strings.Contains(scriptID, "/") || ids.Validate(ids.KindScript, scriptID) != nil ||
					!bytes.Equal(value.Value, []byte(scriptID)) {
					return releases.CorruptReleaseRecord()
				}
				keys[index] = scriptrecord.ScriptSetScriptKey(scope.Environment.Record.ID, active.Record.GenerationID, scriptID)
			}
			records, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: scope.ReadRevision})
			if err != nil {
				return err
			}
			if records == nil || records.ReadRevision != scope.ReadRevision || len(records.Values) != len(keys) {
				return releases.CorruptReleaseRecord()
			}
			for _, value := range records.Values {
				if value == nil {
					return releases.CorruptReleaseRecord()
				}
				record, err := scriptrecord.DecodeRecord(value.Value)
				if err != nil || record.EnvironmentID != scope.Environment.Record.ID {
					return releases.CorruptReleaseRecord()
				}
				if _, selected := services[record.ServiceID]; selected && record.Desired.When != "manual" {
					return errs.New(
						errs.KindValidationFailed,
						"release hook is selected but the MVP hook runner is unavailable",
					)
				}
			}
		}
		if len(page.Values) == 0 || !page.More {
			return nil
		}
		start = page.Values[len(page.Values)-1].Key
	}
}
