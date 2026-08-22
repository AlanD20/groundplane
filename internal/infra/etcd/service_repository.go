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
	conditions, mutations, classify, err := repository.prepareServiceCreation(
		ctx, environment, project, record, ServiceMutationReferences{}, false,
	)
	if err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	defer clearMutationValues(mutations)
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[ServiceRecord]{}, classify(result.Revision, result.FailureReads)
	}
	return Versioned[ServiceRecord]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

// ServiceMutationReferences are every live Zone and dependency Service used by
// a direct desired mutation. Their revisions and deletion fences join the same
// transaction as the Service and replay marker.
type ServiceMutationReferences struct {
	Zones        []Versioned[ZoneRecord]
	Dependencies []Versioned[ServiceRecord]
}

func (repository *ServiceRepository) CreateServiceIdempotent(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record ServiceRecord,
	references ServiceMutationReferences,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateServiceMutationMarker(marker, record.EnvironmentID); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	conditions, mutations, classify, err := repository.prepareServiceCreation(
		ctx, environment, project, record, references, true,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearMutationValues(mutations)
	plan, err := newIdempotencyMutationPlan(conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func (repository *ServiceRepository) prepareServiceCreation(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record ServiceRecord,
	references ServiceMutationReferences,
	enforceReferences bool,
) ([]Condition, []Mutation, idempotencyPlanClassifier, error) {
	if err := validateServiceHierarchy(ctx, environment, project, record); err != nil {
		return nil, nil, nil, err
	}
	value, err := encodeServiceRecord(record)
	if err != nil {
		return nil, nil, nil, err
	}
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
	baseCount := len(conditions)
	if enforceReferences {
		referenceConditions, referenceErr := serviceMutationReferenceConditions(record, references)
		if referenceErr != nil {
			clear(value)
			return nil, nil, nil, referenceErr
		}
		conditions = append(conditions, referenceConditions...)
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: serviceKey(record.Desired.ID), Value: value},
		{
			Type:  MutationPut,
			Key:   serviceNameKey(record.EnvironmentID, record.Desired.Name),
			Value: []byte(record.Desired.ID),
		},
		{
			Type:  MutationPut,
			Key:   serviceOwnerKey(record.EnvironmentID, record.Desired.ID),
			Value: []byte(record.Desired.ID),
		},
	}
	classify := func(_ int64, values []*KeyValue) error {
		if enforceReferences {
			if err := classifyServiceMutationReferenceConflict(values[baseCount:], references); err != nil {
				return err
			}
		}
		return classifyServiceWriteConflict(values[:baseCount], environment, project, record, 0)
	}
	return conditions, mutations, classify, nil
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
	return repository.updateService(ctx, environment, project, current, replacement, ServiceMutationReferences{}, false)
}

func (repository *ServiceRepository) ReplaceDesiredIdempotent(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[ServiceRecord],
	desired core.Service,
	references ServiceMutationReferences,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateServiceMutationMarker(marker, current.Record.EnvironmentID); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	replacement, err := ReplaceServiceDesired(current.Record, desired)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	conditions, mutations, classify, err := repository.prepareServiceUpdate(
		ctx, environment, project, current, replacement, references, true,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearMutationValues(mutations)
	plan, err := newIdempotencyMutationPlan(conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
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
	return repository.updateService(ctx, environment, project, current, replacement, ServiceMutationReferences{}, false)
}

func (repository *ServiceRepository) updateService(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[ServiceRecord],
	replacement ServiceRecord,
	references ServiceMutationReferences,
	enforceReferences bool,
) (Versioned[ServiceRecord], error) {
	conditions, mutations, classify, err := repository.prepareServiceUpdate(
		ctx, environment, project, current, replacement, references, enforceReferences,
	)
	if err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	defer clearMutationValues(mutations)
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[ServiceRecord]{}, classify(result.Revision, result.FailureReads)
	}
	return Versioned[ServiceRecord]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *ServiceRepository) prepareServiceUpdate(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[ServiceRecord],
	replacement ServiceRecord,
	references ServiceMutationReferences,
	enforceReferences bool,
) ([]Condition, []Mutation, idempotencyPlanClassifier, error) {
	if err := validateServiceHierarchy(ctx, environment, project, replacement); err != nil {
		return nil, nil, nil, err
	}
	if err := validateServiceVersion(current); err != nil {
		return nil, nil, nil, err
	}
	if current.Record.EnvironmentID != replacement.EnvironmentID ||
		current.Record.Desired.ID != replacement.Desired.ID ||
		current.Record.Desired.Name != replacement.Desired.Name {
		return nil, nil, nil, errs.New(
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
		return nil, nil, nil, err
	}
	if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != current.Record.Desired.ID ||
		string(indexes.Values[1].Value) != current.Record.Desired.ID {
		return nil, nil, nil, errs.New(errs.KindInternal, "Service indexes are missing or corrupt")
	}
	value, err := encodeServiceRecord(replacement)
	if err != nil {
		return nil, nil, nil, err
	}
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
	baseCount := len(conditions)
	if enforceReferences {
		referenceConditions, referenceErr := serviceMutationReferenceConditions(replacement, references)
		if referenceErr != nil {
			clear(value)
			return nil, nil, nil, referenceErr
		}
		conditions = append(conditions, referenceConditions...)
	}
	mutations := []Mutation{{Type: MutationPut, Key: serviceKey(replacement.Desired.ID), Value: value}}
	classify := func(_ int64, values []*KeyValue) error {
		if enforceReferences {
			if err := classifyServiceMutationReferenceConflict(values[baseCount:], references); err != nil {
				return err
			}
		}
		return classifyServiceWriteConflict(values[:baseCount], environment, project, current.Record, current.Revision)
	}
	return conditions, mutations, classify, nil
}

func serviceMutationReferenceConditions(
	record ServiceRecord,
	references ServiceMutationReferences,
) ([]Condition, error) {
	wantZones := make(map[string]struct{}, len(record.Desired.Zones))
	for _, name := range record.Desired.Zones {
		wantZones[name] = struct{}{}
	}
	if len(wantZones) != len(references.Zones) || len(record.Desired.DependsOn) != len(references.Dependencies) {
		return nil, errs.New(errs.KindValidationFailed, "Service mutation references are incomplete")
	}
	conditions := make([]Condition, 0, 2*(len(references.Zones)+len(references.Dependencies)))
	for _, zone := range references.Zones {
		if err := validateZoneRecord(zone.Record); err != nil || zone.Revision <= 0 ||
			zone.ReadRevision < zone.Revision || zone.Record.EnvironmentID != record.EnvironmentID {
			return nil, errs.New(errs.KindValidationFailed, "Service Zone reference is invalid")
		}
		if _, ok := wantZones[zone.Record.Desired.Name]; !ok {
			return nil, errs.New(errs.KindValidationFailed, "Service Zone reference is not desired")
		}
		delete(wantZones, zone.Record.Desired.Name)
		conditions = append(conditions,
			Condition{Key: zoneKey(zone.Record.Desired.ID), ModRevision: zone.Revision},
			Condition{Key: deletionTombstoneKey("zone", zone.Record.Desired.ID)},
		)
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
			Condition{Key: serviceKey(dependency.Record.Desired.ID), ModRevision: dependency.Revision},
			Condition{Key: deletionTombstoneKey("service", dependency.Record.Desired.ID)},
		)
	}
	if len(wantZones) != 0 || len(wantDependencies) != 0 {
		return nil, errs.New(errs.KindValidationFailed, "Service mutation references are incomplete")
	}
	return conditions, nil
}

func classifyServiceMutationReferenceConflict(values []*KeyValue, references ServiceMutationReferences) error {
	if len(values) != 2*(len(references.Zones)+len(references.Dependencies)) {
		return errs.New(errs.KindInternal, "Service reference compare evidence is incomplete")
	}
	offset := 0
	for _, zone := range references.Zones {
		if values[offset] == nil || values[offset].ModRevision != zone.Revision {
			return errs.New(errs.KindStateConflict, "Service Zone reference changed")
		}
		if values[offset+1] != nil {
			return errs.New(errs.KindResourceInUse, "Service Zone removal is in progress")
		}
		offset += 2
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

func validateServiceMutationMarker(marker IdempotencyMarker, environmentID string) error {
	if marker.Kind != IdempotencyMarkerDirect || marker.State != IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != IdempotencyScopeEnvironment || marker.Locator.ScopeID != environmentID {
		return errs.New(
			errs.KindValidationFailed,
			"Service mutation marker must be a completed Environment-scoped direct mutation",
		)
	}
	return validateIdempotencyMarker(marker)
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
