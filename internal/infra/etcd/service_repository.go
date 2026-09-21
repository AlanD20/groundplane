package etcd

import (
	"context"
	deletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// ServiceRepository projects head-selected desired Services and joins their
// independently mutable runtime sidecars.
type ServiceRepository struct {
	store hierarchyStore
}

func existingIdempotencyTransaction(
	ctx context.Context,
	store hierarchyStore,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, bool, error) {
	repository, err := NewIdempotencyRepository(store)
	if err != nil {
		return IdempotencyTransactionResult{}, false, err
	}
	evidence, err := repository.Read(ctx, marker.Locator)
	if err != nil {
		return IdempotencyTransactionResult{}, false, err
	}
	if evidence == nil {
		return IdempotencyTransactionResult{}, false, nil
	}
	existing, err := evidence.Marker()
	if err != nil {
		return IdempotencyTransactionResult{}, false, err
	}
	return IdempotencyTransactionResult{
		kind: idempotencyTransactionExisting, revision: evidence.modRevision, marker: existing,
	}, true, nil
}

func NewServiceRepository(store etcdstore.Store) (*ServiceRepository, error) {
	return newServiceRepository(store)
}

func newServiceRepository(store hierarchyStore) (*ServiceRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Service store is required")
	}
	return &ServiceRepository{store: store}, nil
}

// ServiceMutationReferences are the live desired resources used to construct
// one complete candidate Environment revision.
type ServiceMutationReferences struct {
	Zones        []etcdstore.Versioned[zonerecord.Record]
	Dependencies []etcdstore.Versioned[servicerecord.ServiceRecord]
}

func serviceMutationReferenceConditions(
	record servicerecord.ServiceRecord,
	references ServiceMutationReferences,
) ([]etcdstore.Condition, error) {
	wantZones := make(map[string]struct{}, len(record.Desired.Zones))
	for _, name := range record.Desired.Zones {
		wantZones[name] = struct{}{}
	}
	if len(wantZones) != len(references.Zones) || len(record.Desired.DependsOn) != len(references.Dependencies) {
		return nil, errs.New(errs.KindValidationFailed, "Service mutation references are incomplete")
	}
	conditions := make([]etcdstore.Condition, 0, len(references.Zones)+2*len(references.Dependencies))
	for _, zone := range references.Zones {
		if err := zonerecord.ValidateRecord(zone.Record); err != nil || zone.Revision <= 0 ||
			zone.ReadRevision < zone.Revision || zone.Record.EnvironmentID != record.EnvironmentID {
			return nil, errs.New(errs.KindValidationFailed, "Service Zone reference is invalid")
		}
		if _, ok := wantZones[zone.Record.Desired.Name]; !ok {
			return nil, errs.New(errs.KindValidationFailed, "Service Zone reference is not desired")
		}
		delete(wantZones, zone.Record.Desired.Name)
		conditions = append(conditions, etcdstore.Condition{Key: deletions.TombstoneKey("zone", zone.Record.Desired.ID)})
	}
	wantDependencies := make(map[string]struct{}, len(record.Desired.DependsOn))
	for name := range record.Desired.DependsOn {
		wantDependencies[name] = struct{}{}
	}
	for _, dependency := range references.Dependencies {
		if err := validateServiceVersion(
			dependency,
		); err != nil ||
			dependency.Record.EnvironmentID != record.EnvironmentID ||
			dependency.Record.Desired.ID == record.Desired.ID {
			return nil, errs.New(errs.KindValidationFailed, "Service dependency reference is invalid")
		}
		if _, ok := wantDependencies[dependency.Record.Desired.Name]; !ok {
			return nil, errs.New(errs.KindValidationFailed, "Service dependency reference is not desired")
		}
		delete(wantDependencies, dependency.Record.Desired.Name)
		conditions = append(conditions,
			servicerecord.ServiceDesiredCondition(dependency),
			etcdstore.Condition{Key: deletions.TombstoneKey("service", dependency.Record.Desired.ID)},
		)
	}
	if len(wantZones) != 0 || len(wantDependencies) != 0 {
		return nil, errs.New(errs.KindValidationFailed, "Service mutation references are incomplete")
	}
	return conditions, nil
}

func classifyServiceMutationReferenceConflict(values []*etcdstore.KeyValue, references ServiceMutationReferences) error {
	if len(values) != len(references.Zones)+2*len(references.Dependencies) {
		return errs.New(errs.KindInternal, "Service reference compare evidence is incomplete")
	}
	offset := 0
	for range references.Zones {
		if values[offset] != nil {
			return errs.New(errs.KindResourceInUse, "Service Zone removal is in progress")
		}
		offset++
	}
	for _, dependency := range references.Dependencies {
		if values[offset] == nil || values[offset].ModRevision != dependency.Revision {
			return errs.New(errs.KindStateConflict, "Service dependency changed")
		}
		if values[offset+1] != nil {
			return errs.New(errs.KindResourceInUse, "Service dependency removal is in progress")
		}
		offset += 2
	}
	return nil
}

func validateServiceMutationMarker(marker idempotencyrecord.IdempotencyMarker, environmentID string) error {
	if marker.Kind != idempotencyrecord.IdempotencyMarkerDirect || marker.State != idempotencyrecord.IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment || marker.Locator.ScopeID != environmentID {
		return errs.New(
			errs.KindValidationFailed,
			"Service mutation marker must be a completed Environment-scoped direct mutation",
		)
	}
	return idempotencyrecord.ValidateIdempotencyMarker(marker)
}

func validateServiceHierarchy(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record servicerecord.ServiceRecord,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateEnvironment(environment.Record); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateProject(project.Record); err != nil {
		return err
	}
	if err := servicerecord.ValidateServiceRecord(record); err != nil {
		return err
	}
	if environment.Revision <= 0 || environment.ReadRevision < environment.Revision || project.Revision <= 0 ||
		project.ReadRevision < project.Revision || record.EnvironmentID != environment.Record.ID ||
		environment.Record.ProjectID != project.Record.ID {
		return errs.New(errs.KindValidationFailed, "Service hierarchy ownership or revisions are invalid")
	}
	return nil
}

func validateServiceVersion(current etcdstore.Versioned[servicerecord.ServiceRecord]) error {
	if err := servicerecord.ValidateServiceRecord(current.Record); err != nil {
		return err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision {
		return errs.New(errs.KindValidationFailed, "Service version metadata is invalid")
	}
	return nil
}

func classifyServiceWriteConflict(
	values []*etcdstore.KeyValue,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record servicerecord.ServiceRecord,
	expectedServiceRevision int64,
) error {
	expected := 8
	if project.Record.TenantID != "" {
		expected++
	}
	if len(values) != expected {
		return errs.New(errs.KindInternal, "Service write compare evidence is incomplete")
	}
	if expectedServiceRevision == 0 {
		if values[0] != nil || values[2] != nil {
			return errs.New(errs.KindStateConflict, "Service stable identity is already in use")
		}
		if values[1] != nil {
			return errs.New(errs.KindNameConflict, "Service name is already in use")
		}
	} else {
		if values[0] == nil {
			return errs.New(errs.KindServiceNotFound, "Service was not found")
		}
		if values[0].ModRevision != expectedServiceRevision {
			return recordcodec.StateConflict("service", record.Desired.ID)
		}
		for _, index := range []int{1, 2} {
			if values[index] == nil || string(values[index].Value) != record.Desired.ID {
				return errs.New(errs.KindInternal, "Service index changed or is corrupt")
			}
		}
	}
	if values[3] == nil {
		return errs.New(errs.KindEnvironmentNotFound, "Environment was not found")
	}
	if values[3].ModRevision != environment.Revision {
		return recordcodec.StateConflict("environment", environment.Record.ID)
	}
	if values[4] == nil {
		return errs.New(errs.KindProjectNotFound, "Project was not found")
	}
	if values[4].ModRevision != project.Revision {
		return recordcodec.StateConflict("project", project.Record.ID)
	}
	for _, index := range []int{5, 6, 7} {
		if values[index] != nil {
			return errs.New(errs.KindResourceInUse, "Service hierarchy deletion is in progress")
		}
	}
	if expected == 9 && values[8] != nil {
		return errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
	}
	return recordcodec.StateConflict("service", record.Desired.ID)
}
