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
	value, err := encodeZoneRecord(record)
	if err != nil {
		return Versioned[ZoneRecord]{}, err
	}
	defer clear(value)

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
	result, err := repository.store.Transact(ctx, conditions, []Mutation{
		{Type: MutationPut, Key: zoneKey(record.Desired.ID), Value: value},
		{
			Type: MutationPut, Key: zoneNameKey(record.EnvironmentID, record.Desired.Name),
			Value: []byte(record.Desired.ID),
		},
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
			result.FailureReads, environment, project, record, 0,
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

func (repository *ZoneRepository) ReplaceDesired(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[ZoneRecord],
	desired core.Zone,
) (Versioned[ZoneRecord], error) {
	replacement, err := ReplaceZoneDesired(current.Record, desired)
	if err != nil {
		return Versioned[ZoneRecord]{}, err
	}
	if err := validateZoneHierarchy(ctx, environment, project, replacement); err != nil {
		return Versioned[ZoneRecord]{}, err
	}
	if err := validateZoneVersion(current); err != nil {
		return Versioned[ZoneRecord]{}, err
	}
	indexes, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			zoneNameKey(current.Record.EnvironmentID, current.Record.Desired.Name),
			zoneOwnerKey(current.Record.EnvironmentID, current.Record.Desired.ID),
		},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return Versioned[ZoneRecord]{}, err
	}
	if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != current.Record.Desired.ID ||
		string(indexes.Values[1].Value) != current.Record.Desired.ID {
		return Versioned[ZoneRecord]{}, errs.New(errs.KindInternal, "Zone indexes are missing or corrupt")
	}
	value, err := encodeZoneRecord(replacement)
	if err != nil {
		return Versioned[ZoneRecord]{}, err
	}
	defer clear(value)
	conditions := []Condition{
		{Key: zoneKey(current.Record.Desired.ID), ModRevision: current.Revision},
		{
			Key:         zoneNameKey(current.Record.EnvironmentID, current.Record.Desired.Name),
			ModRevision: indexes.Values[0].ModRevision,
		},
		{
			Key:         zoneOwnerKey(current.Record.EnvironmentID, current.Record.Desired.ID),
			ModRevision: indexes.Values[1].ModRevision,
		},
		{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey("zone", current.Record.Desired.ID)},
		{Key: deletionTombstoneKey("environment", environment.Record.ID)},
		{Key: deletionTombstoneKey("project", project.Record.ID)},
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, Condition{Key: deletionTombstoneKey("tenant", project.Record.TenantID)})
	}
	result, err := repository.store.Transact(ctx, conditions, []Mutation{
		{Type: MutationPut, Key: zoneKey(replacement.Desired.ID), Value: value},
	})
	if err != nil {
		return Versioned[ZoneRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[ZoneRecord]{}, classifyZoneWriteConflict(
			result.FailureReads, environment, project, current.Record, current.Revision,
		)
	}
	return Versioned[ZoneRecord]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
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
	return nil
}

func validateZoneVersion(current Versioned[ZoneRecord]) error {
	if err := validateZoneRecord(current.Record); err != nil {
		return err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision {
		return errs.New(errs.KindValidationFailed, "Zone version metadata is invalid")
	}
	return nil
}

func classifyZoneWriteConflict(
	values []*KeyValue,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record ZoneRecord,
	expectedZoneRevision int64,
) error {
	expected := 8
	if project.Record.TenantID != "" {
		expected++
	}
	if len(values) != expected {
		return errs.New(errs.KindInternal, "Zone write compare evidence is incomplete")
	}
	if expectedZoneRevision == 0 {
		if values[0] != nil || values[2] != nil {
			return errs.New(errs.KindStateConflict, "Zone stable identity is already in use")
		}
		if values[1] != nil {
			return errs.New(errs.KindNameConflict, "Zone name is already in use")
		}
	} else {
		if values[0] == nil {
			return errs.New(errs.KindZoneNotFound, "Zone was not found")
		}
		if values[0].ModRevision != expectedZoneRevision {
			return stateConflict("zone", record.Desired.ID)
		}
		for _, index := range []int{1, 2} {
			if values[index] == nil || string(values[index].Value) != record.Desired.ID {
				return errs.New(errs.KindInternal, "Zone index changed or is corrupt")
			}
		}
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
	if expected == 9 && values[8] != nil {
		return errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
	}
	return stateConflict("zone", record.Desired.ID)
}
