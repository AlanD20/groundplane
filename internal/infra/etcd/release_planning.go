package etcd

import (
	"bytes"
	"context"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReleasePlanningScope is one fixed-revision view of every ancestor and
// render input shared by all candidates in an operation.
type ReleasePlanningScope struct {
	Environment              Versioned[EnvironmentRecord]
	Project                  Versioned[ProjectRecord]
	Tenant                   Versioned[TenantRecord]
	Compose                  Versioned[EnvironmentComposeProjection]
	EnvironmentEpochRevision int64
	EnvironmentEpochValue    []byte
	ReadRevision             int64
}

type ReleasePlanningService struct {
	Service            Versioned[ServiceRecord]
	Projection         domain.ServiceProjection
	ProjectionRevision int64
}

func (scope ReleasePlanningScope) Clone() ReleasePlanningScope {
	clone := scope
	clone.EnvironmentEpochValue = slices.Clone(scope.EnvironmentEpochValue)
	clone.Compose.Record = cloneEnvironmentComposeProjection(scope.Compose.Record)
	return clone
}

func (ledger *ReleaseLedger) LoadPlanningScope(
	ctx context.Context,
	environmentID string,
) (ReleasePlanningScope, error) {
	if ctx == nil || ledger == nil || ledger.store == nil || ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return ReleasePlanningScope{}, errs.New(errs.KindValidationFailed, "release planning environment is invalid")
	}
	initial, err := ledger.store.Get(ctx, environmentKey(environmentID))
	if err != nil {
		return ReleasePlanningScope{}, err
	}
	if initial == nil || initial.ReadRevision <= 0 {
		return ReleasePlanningScope{}, corruptReleaseRecord()
	}
	if initial.Entry == nil {
		return ReleasePlanningScope{}, errs.New(errs.KindEnvironmentNotFound, "environment was not found")
	}
	environment, err := decodeEnvironment(initial.Entry.Value)
	if err != nil || environment.ID != environmentID || environment.ProvisioningState != EnvironmentProvisioningReady {
		return ReleasePlanningScope{}, corruptReleaseRecord()
	}
	projectRead, err := ledger.store.GetMany(ctx, GetManyRequest{
		Keys: []string{projectKey(environment.ProjectID)}, Revision: initial.ReadRevision,
	})
	if err != nil {
		return ReleasePlanningScope{}, err
	}
	if projectRead == nil || projectRead.ReadRevision != initial.ReadRevision || len(projectRead.Values) != 1 || projectRead.Values[0] == nil {
		return ReleasePlanningScope{}, corruptReleaseRecord()
	}
	project, err := decodeProject(projectRead.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID || project.Kind != ProjectKindTenant {
		return ReleasePlanningScope{}, corruptReleaseRecord()
	}
	keys := []string{
		environmentKey(environmentID), projectKey(project.ID), tenantKey(project.TenantID),
		environmentComposeProjectionKey(environmentID), environmentMutationEpochKey(environmentID),
		environmentOperationLockKey(environmentID), releaseFenceSetKey(environmentID),
		deletionTombstoneKey("environment", environmentID), deletionTombstoneKey("project", project.ID),
		deletionTombstoneKey("tenant", project.TenantID),
	}
	loaded, err := ledger.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: initial.ReadRevision})
	if err != nil {
		return ReleasePlanningScope{}, err
	}
	if loaded == nil || loaded.ReadRevision != initial.ReadRevision || len(loaded.Values) != len(keys) {
		return ReleasePlanningScope{}, corruptReleaseRecord()
	}
	for _, index := range []int{0, 1, 2, 3, 4} {
		if loaded.Values[index] == nil {
			return ReleasePlanningScope{}, corruptReleaseRecord()
		}
	}
	if loaded.Values[5] != nil || loaded.Values[6] != nil || loaded.Values[7] != nil || loaded.Values[8] != nil || loaded.Values[9] != nil {
		return ReleasePlanningScope{}, errs.New(errs.KindResourceInUse, "release planning scope is locked or deleting")
	}
	environment, err = decodeEnvironment(loaded.Values[0].Value)
	if err != nil || environment.ID != environmentID || environment.ProjectID != project.ID {
		return ReleasePlanningScope{}, corruptReleaseRecord()
	}
	project, err = decodeProject(loaded.Values[1].Value)
	if err != nil || project.ID != environment.ProjectID || project.Kind != ProjectKindTenant {
		return ReleasePlanningScope{}, corruptReleaseRecord()
	}
	tenant, err := decodeTenant(loaded.Values[2].Value)
	if err != nil || tenant.ID != project.TenantID {
		return ReleasePlanningScope{}, corruptReleaseRecord()
	}
	compose, err := decodeEnvironmentComposeProjection(loaded.Values[3].Value)
	if err != nil || compose.EnvironmentID != environmentID {
		return ReleasePlanningScope{}, corruptReleaseRecord()
	}
	epoch, err := decodeEnvironmentMutationEpochRecord(loaded.Values[4].Value)
	if err != nil || epoch.EnvironmentID != environmentID {
		return ReleasePlanningScope{}, corruptReleaseRecord()
	}
	return ReleasePlanningScope{
		Environment:              Versioned[EnvironmentRecord]{Record: environment, Revision: loaded.Values[0].ModRevision, ReadRevision: loaded.ReadRevision},
		Project:                  Versioned[ProjectRecord]{Record: project, Revision: loaded.Values[1].ModRevision, ReadRevision: loaded.ReadRevision},
		Tenant:                   Versioned[TenantRecord]{Record: tenant, Revision: loaded.Values[2].ModRevision, ReadRevision: loaded.ReadRevision},
		Compose:                  Versioned[EnvironmentComposeProjection]{Record: compose, Revision: loaded.Values[3].ModRevision, ReadRevision: loaded.ReadRevision},
		EnvironmentEpochRevision: loaded.Values[4].ModRevision,
		EnvironmentEpochValue:    slices.Clone(loaded.Values[4].Value), ReadRevision: loaded.ReadRevision,
	}, nil
}

func (ledger *ReleaseLedger) LoadPlanningServices(
	ctx context.Context,
	scope ReleasePlanningScope,
	serviceIDs []string,
) ([]ReleasePlanningService, error) {
	if ctx == nil || ledger == nil || scope.ReadRevision <= 0 || len(serviceIDs) == 0 ||
		len(serviceIDs) > maximumReleasePublicationMembers {
		return nil, errs.New(errs.KindValidationFailed, "release planning service selection is invalid")
	}
	keys := make([]string, 0, len(serviceIDs)*4)
	seen := make(map[string]struct{}, len(serviceIDs))
	for _, serviceID := range serviceIDs {
		if ids.Validate(ids.KindService, serviceID) != nil {
			return nil, errs.New(errs.KindValidationFailed, "release planning service id is invalid")
		}
		if _, duplicate := seen[serviceID]; duplicate {
			return nil, errs.New(errs.KindValidationFailed, "release planning service is duplicated")
		}
		seen[serviceID] = struct{}{}
		keys = append(keys, serviceKey(serviceID), serviceOwnerKey(scope.Environment.Record.ID, serviceID),
			deletionTombstoneKey("service", serviceID), releaseProjectionKey(serviceID))
	}
	loaded, err := ledger.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: scope.ReadRevision})
	if err != nil {
		return nil, err
	}
	if loaded == nil || loaded.ReadRevision != scope.ReadRevision || len(loaded.Values) != len(keys) {
		return nil, corruptReleaseRecord()
	}
	projected := make(map[string]struct{}, len(scope.Compose.Record.Services))
	for _, identity := range scope.Compose.Record.Services {
		projected[identity.ID] = struct{}{}
	}
	result := make([]ReleasePlanningService, len(serviceIDs))
	for index, serviceID := range serviceIDs {
		base := index * 4
		if loaded.Values[base] == nil {
			return nil, errs.New(errs.KindServiceNotFound, "service was not found")
		}
		if loaded.Values[base+1] == nil || !bytes.Equal(loaded.Values[base+1].Value, []byte(serviceID)) ||
			loaded.Values[base+2] != nil {
			return nil, errs.New(errs.KindResourceInUse, "release service ownership changed or is deleting")
		}
		record, err := decodeServiceRecord(loaded.Values[base].Value)
		if err != nil || record.Desired.ID != serviceID || record.EnvironmentID != scope.Environment.Record.ID {
			return nil, corruptReleaseRecord()
		}
		if _, exists := projected[serviceID]; !exists {
			return nil, errs.New(errs.KindValidationFailed, "release service is not in the applied Compose projection")
		}
		planning := ReleasePlanningService{Service: Versioned[ServiceRecord]{
			Record: record, Revision: loaded.Values[base].ModRevision, ReadRevision: loaded.ReadRevision,
		}}
		if loaded.Values[base+3] != nil {
			projection, err := decodeReleaseRecord[domain.ServiceProjection](loaded.Values[base+3].Value, "service-release-projection")
			if err != nil || projection.EnvironmentID != scope.Environment.Record.ID || projection.ServiceID != serviceID {
				return nil, corruptReleaseRecord()
			}
			planning.Projection = projection
			planning.ProjectionRevision = loaded.Values[base+3].ModRevision
		}
		result[index] = planning
	}
	if err := ledger.rejectSelectedHooks(ctx, scope, seen); err != nil {
		return nil, err
	}
	return result, nil
}

func (ledger *ReleaseLedger) GetPlanningServingIntent(
	ctx context.Context,
	scope ReleasePlanningScope,
	service ReleasePlanningService,
) (domain.Intent, bool, error) {
	if service.Projection.ServingReleaseID == "" {
		return domain.Intent{}, false, nil
	}
	index, err := ledger.store.GetMany(ctx, GetManyRequest{Keys: []string{
		releaseServiceIndexKey(scope.Environment.Record.ID, service.Service.Record.Desired.ID, service.Projection.ServingReleaseID),
	}, Revision: scope.ReadRevision})
	if err != nil {
		return domain.Intent{}, false, err
	}
	if index == nil || index.ReadRevision != scope.ReadRevision || len(index.Values) != 1 || index.Values[0] == nil {
		return domain.Intent{}, false, corruptReleaseRecord()
	}
	publicationID, err := decodeReleaseIndex(index.Values[0].Value, service.Service.Record.Desired.ID)
	if err != nil {
		return domain.Intent{}, false, err
	}
	view, err := ledger.readViewAt(ctx, publicationID, service.Projection.ServingReleaseID, scope.ReadRevision)
	if err != nil {
		return domain.Intent{}, false, err
	}
	return view.Intent, true, nil
}

func (ledger *ReleaseLedger) rejectSelectedHooks(
	ctx context.Context,
	scope ReleasePlanningScope,
	services map[string]struct{},
) error {
	prefix := scriptOwnerPrefix(scope.Environment.Record.ID)
	start := ""
	for {
		page, err := ledger.store.Range(ctx, RangeRequest{
			Prefix: prefix, StartExclusive: start, Limit: 200, Revision: scope.ReadRevision,
		})
		if err != nil {
			return err
		}
		if page == nil || page.ReadRevision != scope.ReadRevision {
			return corruptReleaseRecord()
		}
		if len(page.Values) != 0 {
			keys := make([]string, len(page.Values))
			for index, value := range page.Values {
				scriptID := strings.TrimPrefix(value.Key, prefix)
				if strings.Contains(scriptID, "/") || ids.Validate(ids.KindScript, scriptID) != nil ||
					!bytes.Equal(value.Value, []byte(scriptID)) {
					return corruptReleaseRecord()
				}
				keys[index] = scriptKey(scriptID)
			}
			records, err := ledger.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: scope.ReadRevision})
			if err != nil {
				return err
			}
			if records == nil || records.ReadRevision != scope.ReadRevision || len(records.Values) != len(keys) {
				return corruptReleaseRecord()
			}
			for _, value := range records.Values {
				if value == nil {
					return corruptReleaseRecord()
				}
				record, err := decodeScriptRecord(value.Value)
				if err != nil || record.EnvironmentID != scope.Environment.Record.ID {
					return corruptReleaseRecord()
				}
				if _, selected := services[record.ServiceID]; selected && record.Desired.When != "manual" {
					return errs.New(errs.KindValidationFailed, "release hook is selected but the MVP hook runner is unavailable")
				}
			}
		}
		if len(page.Values) == 0 || !page.More {
			return nil
		}
		start = page.Values[len(page.Values)-1].Key
	}
}
