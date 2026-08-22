package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ServiceRepository owns the flat Service primary, scoped-name uniqueness,
// Environment membership, fixed-revision pagination, and revision-fenced
// desired/runtime updates.
type ServiceRepository struct {
	store hierarchyStore
}

func NewServiceRepository(store Store) (*ServiceRepository, error) {
	return newServiceRepository(store)
}

func newServiceRepository(store hierarchyStore) (*ServiceRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Service store is required")
	}
	return &ServiceRepository{store: store}, nil
}

func (repository *ServiceRepository) CreateService(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record ServiceRecord,
) (Versioned[ServiceRecord], error) {
	if err := validateServiceHierarchy(ctx, environment, project, record); err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	value, err := encodeServiceRecord(record)
	if err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	defer clear(value)

	conditions := []Condition{
		{Key: serviceKey(record.Desired.ID)},
		{Key: serviceNameKey(record.EnvironmentID, record.Desired.Name)},
		{Key: serviceOwnerKey(record.EnvironmentID, record.Desired.ID)},
		{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey("service", record.Desired.ID)},
		{Key: deletionTombstoneKey("environment", environment.Record.ID)},
		{Key: deletionTombstoneKey("project", project.Record.ID)},
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, Condition{Key: deletionTombstoneKey("tenant", project.Record.TenantID)})
	}
	result, err := repository.store.Transact(ctx, conditions, []Mutation{
		{Type: MutationPut, Key: serviceKey(record.Desired.ID), Value: value},
		{
			Type: MutationPut, Key: serviceNameKey(record.EnvironmentID, record.Desired.Name),
			Value: []byte(record.Desired.ID),
		},
		{
			Type: MutationPut, Key: serviceOwnerKey(record.EnvironmentID, record.Desired.ID),
			Value: []byte(record.Desired.ID),
		},
	})
	if err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[ServiceRecord]{}, classifyServiceWriteConflict(
			result.FailureReads, environment, project, record, 0,
		)
	}
	return Versioned[ServiceRecord]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *ServiceRepository) GetService(
	ctx context.Context,
	id string,
) (Versioned[ServiceRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	if err := validateID(ids.KindService, id); err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	return getRecord(
		ctx,
		repository.store,
		serviceKey(id),
		id,
		errs.KindServiceNotFound,
		decodeServiceRecord,
		func(record ServiceRecord) string { return record.Desired.ID },
	)
}

func (repository *ServiceRepository) ListServices(
	ctx context.Context,
	environmentID string,
	request PageRequest,
) (Page[ServiceRecord], error) {
	if err := validateID(ids.KindEnvironment, environmentID); err != nil {
		return Page[ServiceRecord]{}, err
	}
	return listIndexPage(
		ctx,
		repository.store,
		"services",
		"environment",
		environmentID,
		serviceOwnerPrefix(environmentID),
		serviceKey,
		ids.KindService,
		request,
		decodeServiceRecord,
		func(record ServiceRecord) string { return record.Desired.ID },
		func(record ServiceRecord) bool { return record.EnvironmentID == environmentID },
	)
}

func (repository *ServiceRepository) ReplaceDesired(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[ServiceRecord],
	desired core.Service,
) (Versioned[ServiceRecord], error) {
	replacement, err := ReplaceServiceDesired(current.Record, desired)
	if err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	return repository.updateService(ctx, environment, project, current, replacement)
}

func (repository *ServiceRepository) SetRuntimeIntent(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[ServiceRecord],
	intent core.ServiceRuntimeIntent,
) (Versioned[ServiceRecord], error) {
	replacement, err := SetServiceRuntimeIntent(current.Record, intent)
	if err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	return repository.updateService(ctx, environment, project, current, replacement)
}

func (repository *ServiceRepository) updateService(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[ServiceRecord],
	replacement ServiceRecord,
) (Versioned[ServiceRecord], error) {
	if err := validateServiceHierarchy(ctx, environment, project, replacement); err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	if err := validateServiceVersion(current); err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	if current.Record.EnvironmentID != replacement.EnvironmentID ||
		current.Record.Desired.ID != replacement.Desired.ID ||
		current.Record.Desired.Name != replacement.Desired.Name {
		return Versioned[ServiceRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Service update changed immutable identity or ownership",
		)
	}
	indexes, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			serviceNameKey(current.Record.EnvironmentID, current.Record.Desired.Name),
			serviceOwnerKey(current.Record.EnvironmentID, current.Record.Desired.ID),
		},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != current.Record.Desired.ID ||
		string(indexes.Values[1].Value) != current.Record.Desired.ID {
		return Versioned[ServiceRecord]{}, errs.New(errs.KindInternal, "Service indexes are missing or corrupt")
	}
	value, err := encodeServiceRecord(replacement)
	if err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	defer clear(value)
	conditions := []Condition{
		{Key: serviceKey(current.Record.Desired.ID), ModRevision: current.Revision},
		{
			Key:         serviceNameKey(current.Record.EnvironmentID, current.Record.Desired.Name),
			ModRevision: indexes.Values[0].ModRevision,
		},
		{
			Key:         serviceOwnerKey(current.Record.EnvironmentID, current.Record.Desired.ID),
			ModRevision: indexes.Values[1].ModRevision,
		},
		{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey("service", current.Record.Desired.ID)},
		{Key: deletionTombstoneKey("environment", environment.Record.ID)},
		{Key: deletionTombstoneKey("project", project.Record.ID)},
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, Condition{Key: deletionTombstoneKey("tenant", project.Record.TenantID)})
	}
	result, err := repository.store.Transact(ctx, conditions, []Mutation{
		{Type: MutationPut, Key: serviceKey(replacement.Desired.ID), Value: value},
	})
	if err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[ServiceRecord]{}, classifyServiceWriteConflict(
			result.FailureReads, environment, project, current.Record, current.Revision,
		)
	}
	return Versioned[ServiceRecord]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func validateServiceHierarchy(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record ServiceRecord,
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
	if err := validateServiceRecord(record); err != nil {
		return err
	}
	if environment.Revision <= 0 || environment.ReadRevision < environment.Revision || project.Revision <= 0 ||
		project.ReadRevision < project.Revision || record.EnvironmentID != environment.Record.ID ||
		environment.Record.ProjectID != project.Record.ID {
		return errs.New(errs.KindValidationFailed, "Service hierarchy ownership or revisions are invalid")
	}
	return nil
}

func validateServiceVersion(current Versioned[ServiceRecord]) error {
	if err := validateServiceRecord(current.Record); err != nil {
		return err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision {
		return errs.New(errs.KindValidationFailed, "Service version metadata is invalid")
	}
	return nil
}

func classifyServiceWriteConflict(
	values []*KeyValue,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record ServiceRecord,
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
			return stateConflict("service", record.Desired.ID)
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
			return errs.New(errs.KindResourceInUse, "Service hierarchy deletion is in progress")
		}
	}
	if expected == 9 && values[8] != nil {
		return errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
	}
	return stateConflict("service", record.Desired.ID)
}
