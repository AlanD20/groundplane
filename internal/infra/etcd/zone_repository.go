package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ZoneRepository owns stable Zone records, Environment-scoped name
// uniqueness, membership indexes, fixed-revision pagination, and CAS updates.
type ZoneRepository struct {
	store hierarchyStore
}

func NewZoneRepository(store Store) (*ZoneRepository, error) {
	return newZoneRepository(store)
}

func newZoneRepository(store hierarchyStore) (*ZoneRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Zone store is required")
	}
	return &ZoneRepository{store: store}, nil
}

func (repository *ZoneRepository) CreateZone(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record ZoneRecord,
) (Versioned[ZoneRecord], error) {
	if err := validateZoneHierarchy(ctx, environment, project, record); err != nil {
		return Versioned[ZoneRecord]{}, err
	}
	poolRegistry, err := repository.getZonePoolRegistry(ctx, environment.Record.ID)
	if err != nil {
		return Versioned[ZoneRecord]{}, err
	}
	nextPoolRegistry, err := poolRegistry.Record.reserve(environment.Record, record)
	if err != nil {
		return Versioned[ZoneRecord]{}, err
	}
	poolRegistry.Record = nextPoolRegistry
	value, err := encodeZoneRecord(record)
	if err != nil {
		return Versioned[ZoneRecord]{}, err
	}
	defer clear(value)
	poolRegistryValue, err := encodeEnvelope("zone_pool_registry", poolRegistry.Record)
	if err != nil {
		return Versioned[ZoneRecord]{}, err
	}
	defer clear(poolRegistryValue)

	conditions := []Condition{
		{Key: zoneKey(record.Desired.ID)},
		{Key: zoneNameKey(record.EnvironmentID, record.Desired.Name)},
		{Key: zoneOwnerKey(record.EnvironmentID, record.Desired.ID)},
		{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey("zone", record.Desired.ID)},
		{Key: deletionTombstoneKey("environment", environment.Record.ID)},
		{Key: deletionTombstoneKey("project", project.Record.ID)},
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, Condition{Key: deletionTombstoneKey("tenant", project.Record.TenantID)})
	}
	conditions = append(conditions, Condition{
		Key: zonePoolRegistryKey(environment.Record.ID), ModRevision: poolRegistry.Revision,
	})
	result, err := repository.store.Transact(ctx, conditions, []Mutation{
		{Type: MutationPut, Key: zoneKey(record.Desired.ID), Value: value},
		{
			Type: MutationPut, Key: zoneNameKey(record.EnvironmentID, record.Desired.Name),
			Value: []byte(record.Desired.ID),
		},
		{Type: MutationPut, Key: zonePoolRegistryKey(environment.Record.ID), Value: poolRegistryValue},
		{
			Type: MutationPut, Key: zoneOwnerKey(record.EnvironmentID, record.Desired.ID),
			Value: []byte(record.Desired.ID),
		},
	})
	if err != nil {
		return Versioned[ZoneRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[ZoneRecord]{}, classifyZoneWriteConflict(
			result.FailureReads, environment, project, poolRegistry, record,
		)
	}
	return Versioned[ZoneRecord]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *ZoneRepository) GetZone(ctx context.Context, id string) (Versioned[ZoneRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[ZoneRecord]{}, err
	}
	if err := validateID(ids.KindNetwork, id); err != nil {
		return Versioned[ZoneRecord]{}, err
	}
	return getRecord(
		ctx,
		repository.store,
		zoneKey(id),
		id,
		errs.KindZoneNotFound,
		decodeZoneRecord,
		func(record ZoneRecord) string { return record.Desired.ID },
	)
}

func (repository *ZoneRepository) ListZones(
	ctx context.Context,
	environmentID string,
	request PageRequest,
) (Page[ZoneRecord], error) {
	if err := validateID(ids.KindEnvironment, environmentID); err != nil {
		return Page[ZoneRecord]{}, err
	}
	return listIndexPage(
		ctx,
		repository.store,
		"zones",
		"environment",
		environmentID,
		zoneOwnerPrefix(environmentID),
		zoneKey,
		ids.KindNetwork,
		request,
		decodeZoneRecord,
		func(record ZoneRecord) string { return record.Desired.ID },
		func(record ZoneRecord) bool { return record.EnvironmentID == environmentID },
	)
}

func validateZoneHierarchy(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record ZoneRecord,
) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateEnvironment(environment.Record); err != nil {
		return err
	}
	if err := validateProject(project.Record); err != nil {
		return err
	}
	if err := validateZoneRecord(record); err != nil {
		return err
	}
	if environment.Revision <= 0 || environment.ReadRevision < environment.Revision || project.Revision <= 0 ||
		project.ReadRevision < project.Revision || record.EnvironmentID != environment.Record.ID ||
		environment.Record.ProjectID != project.Record.ID {
		return errs.New(errs.KindValidationFailed, "Zone hierarchy ownership or revisions are invalid")
	}
	if project.Record.Kind == ProjectKindTenant &&
		(record.Desired.OwnerKind != core.ZoneOwnerEnvironment || record.Desired.OwnerID != environment.Record.ID) {
		return errs.New(errs.KindValidationFailed, "Tenant Project Zone ownership is invalid")
	}
	if project.Record.Kind == ProjectKindBacking &&
		(record.Desired.OwnerKind != core.ZoneOwnerBackingProject || record.Desired.OwnerID != project.Record.ID) {
		return errs.New(errs.KindValidationFailed, "Backing Project Zone ownership is invalid")
	}
	return nil
}

func classifyZoneWriteConflict(
	values []*KeyValue,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	poolRegistry Versioned[zonePoolRegistry],
	record ZoneRecord,
) error {
	registryIndex := 8
	if project.Record.TenantID != "" {
		registryIndex++
	}
	expected := registryIndex + 1
	if len(values) != expected {
		return errs.New(errs.KindInternal, "Zone write compare evidence is incomplete")
	}
	if values[0] != nil || values[2] != nil {
		return errs.New(errs.KindStateConflict, "Zone stable identity is already in use")
	}
	if values[1] != nil {
		return errs.New(errs.KindNameConflict, "Zone name is already in use")
	}
	if values[3] == nil {
		return errs.New(errs.KindEnvironmentNotFound, "Environment was not found")
	}
	if values[3].ModRevision != environment.Revision {
		return stateConflict("environment", environment.Record.ID)
	}
	if values[4] == nil {
		return errs.New(errs.KindProjectNotFound, "Project was not found")
	}
	if values[4].ModRevision != project.Revision {
		return stateConflict("project", project.Record.ID)
	}
	for _, index := range []int{5, 6, 7} {
		if values[index] != nil {
			return errs.New(errs.KindResourceInUse, "Zone hierarchy deletion is in progress")
		}
	}
	if project.Record.TenantID != "" && values[8] != nil {
		return errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
	}
	if (poolRegistry.Revision == 0 && values[registryIndex] != nil) ||
		(poolRegistry.Revision > 0 &&
			(values[registryIndex] == nil || values[registryIndex].ModRevision != poolRegistry.Revision)) {
		return stateConflict("Zone pool registry", environment.Record.ID)
	}
	return stateConflict("zone", record.Desired.ID)
}
