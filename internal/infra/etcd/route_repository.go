package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RouteRepository owns stable Route records, Environment membership,
// fixed-revision pagination, target-Service fences, and CAS updates.
type RouteRepository struct {
	store hierarchyStore
}

func NewRouteRepository(store Store) (*RouteRepository, error) {
	return newRouteRepository(store)
}

func newRouteRepository(store hierarchyStore) (*RouteRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Route store is required")
	}
	return &RouteRepository{store: store}, nil
}

func (repository *RouteRepository) CreateRoute(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	target Versioned[ServiceRecord],
	record RouteRecord,
) (Versioned[RouteRecord], error) {
	if err := validateRouteHierarchy(ctx, environment, project, target, record); err != nil {
		return Versioned[RouteRecord]{}, err
	}
	value, err := encodeRouteRecord(record)
	if err != nil {
		return Versioned[RouteRecord]{}, err
	}
	defer clear(value)

	conditions := routeWriteConditions(environment, project, target, record, nil, 0)
	result, err := repository.store.Transact(ctx, conditions, []Mutation{
		{Type: MutationPut, Key: routeKey(record.Desired.ID), Value: value},
		{
			Type: MutationPut, Key: routeOwnerKey(record.EnvironmentID, record.Desired.ID),
			Value: []byte(record.Desired.ID),
		},
	})
	if err != nil {
		return Versioned[RouteRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[RouteRecord]{}, classifyRouteWriteConflict(
			result.FailureReads, environment, project, target, record, 0,
		)
	}
	return Versioned[RouteRecord]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *RouteRepository) GetRoute(ctx context.Context, id string) (Versioned[RouteRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[RouteRecord]{}, err
	}
	if err := validateID(ids.KindRoute, id); err != nil {
		return Versioned[RouteRecord]{}, err
	}
	return getRecord(
		ctx,
		repository.store,
		routeKey(id),
		id,
		errs.KindRouteNotFound,
		decodeRouteRecord,
		func(record RouteRecord) string { return record.Desired.ID },
	)
}

func (repository *RouteRepository) ListRoutes(
	ctx context.Context,
	environmentID string,
	request PageRequest,
) (Page[RouteRecord], error) {
	if err := validateID(ids.KindEnvironment, environmentID); err != nil {
		return Page[RouteRecord]{}, err
	}
	return listIndexPage(
		ctx,
		repository.store,
		"routes",
		"environment",
		environmentID,
		routeOwnerPrefix(environmentID),
		routeKey,
		ids.KindRoute,
		request,
		decodeRouteRecord,
		func(record RouteRecord) string { return record.Desired.ID },
		func(record RouteRecord) bool { return record.EnvironmentID == environmentID },
	)
}

func (repository *RouteRepository) ReplaceDesired(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	target Versioned[ServiceRecord],
	current Versioned[RouteRecord],
	desired core.Route,
) (Versioned[RouteRecord], error) {
	replacement, err := ReplaceRouteDesired(current.Record, desired)
	if err != nil {
		return Versioned[RouteRecord]{}, err
	}
	if err := validateRouteHierarchy(ctx, environment, project, target, replacement); err != nil {
		return Versioned[RouteRecord]{}, err
	}
	if err := validateRouteVersion(current); err != nil {
		return Versioned[RouteRecord]{}, err
	}
	index, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys:     []string{routeOwnerKey(current.Record.EnvironmentID, current.Record.Desired.ID)},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return Versioned[RouteRecord]{}, err
	}
	if index == nil || len(index.Values) != 1 || index.Values[0] == nil ||
		string(index.Values[0].Value) != current.Record.Desired.ID {
		return Versioned[RouteRecord]{}, errs.New(errs.KindInternal, "Route owner index is missing or corrupt")
	}
	value, err := encodeRouteRecord(replacement)
	if err != nil {
		return Versioned[RouteRecord]{}, err
	}
	defer clear(value)
	result, err := repository.store.Transact(
		ctx,
		routeWriteConditions(
			environment,
			project,
			target,
			current.Record,
			&current,
			index.Values[0].ModRevision,
		),
		[]Mutation{{Type: MutationPut, Key: routeKey(replacement.Desired.ID), Value: value}},
	)
	if err != nil {
		return Versioned[RouteRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[RouteRecord]{}, classifyRouteWriteConflict(
			result.FailureReads, environment, project, target, current.Record, current.Revision,
		)
	}
	return Versioned[RouteRecord]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func routeWriteConditions(
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	target Versioned[ServiceRecord],
	record RouteRecord,
	current *Versioned[RouteRecord],
	ownerRevision int64,
) []Condition {
	routeCondition := Condition{Key: routeKey(record.Desired.ID)}
	ownerCondition := Condition{Key: routeOwnerKey(record.EnvironmentID, record.Desired.ID)}
	if current != nil {
		routeCondition.ModRevision = current.Revision
		ownerCondition.ModRevision = ownerRevision
	}
	conditions := []Condition{
		routeCondition,
		ownerCondition,
		{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: serviceKey(target.Record.Desired.ID), ModRevision: target.Revision},
		{Key: deletionTombstoneKey("route", record.Desired.ID)},
		{Key: deletionTombstoneKey("environment", environment.Record.ID)},
		{Key: deletionTombstoneKey("project", project.Record.ID)},
		{Key: deletionTombstoneKey("service", target.Record.Desired.ID)},
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, Condition{Key: deletionTombstoneKey("tenant", project.Record.TenantID)})
	}
	return conditions
}

func validateRouteHierarchy(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	target Versioned[ServiceRecord],
	record RouteRecord,
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
	if err := validateServiceVersion(target); err != nil {
		return err
	}
	if err := validateRouteRecord(record); err != nil {
		return err
	}
	if environment.Revision <= 0 || environment.ReadRevision < environment.Revision || project.Revision <= 0 ||
		project.ReadRevision < project.Revision || record.EnvironmentID != environment.Record.ID ||
		environment.Record.ProjectID != project.Record.ID || target.Record.EnvironmentID != environment.Record.ID ||
		target.Record.Desired.Name != record.Desired.ServiceName {
		return errs.New(errs.KindValidationFailed, "Route hierarchy or target Service is invalid")
	}
	return nil
}

func validateRouteVersion(current Versioned[RouteRecord]) error {
	if err := validateRouteRecord(current.Record); err != nil {
		return err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision {
		return errs.New(errs.KindValidationFailed, "Route version metadata is invalid")
	}
	return nil
}

func classifyRouteWriteConflict(
	values []*KeyValue,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	target Versioned[ServiceRecord],
	record RouteRecord,
	expectedRouteRevision int64,
) error {
	expected := 9
	if project.Record.TenantID != "" {
		expected++
	}
	if len(values) != expected {
		return errs.New(errs.KindInternal, "Route write compare evidence is incomplete")
	}
	if expectedRouteRevision == 0 {
		if values[0] != nil || values[1] != nil {
			return errs.New(errs.KindStateConflict, "Route stable identity is already in use")
		}
	} else {
		if values[0] == nil {
			return errs.New(errs.KindRouteNotFound, "Route was not found")
		}
		if values[0].ModRevision != expectedRouteRevision {
			return stateConflict("route", record.Desired.ID)
		}
		if values[1] == nil || string(values[1].Value) != record.Desired.ID {
			return errs.New(errs.KindInternal, "Route owner index changed or is corrupt")
		}
	}
	if values[2] == nil {
		return errs.New(errs.KindEnvironmentNotFound, "Environment was not found")
	}
	if values[2].ModRevision != environment.Revision {
		return stateConflict("environment", environment.Record.ID)
	}
	if values[3] == nil {
		return errs.New(errs.KindProjectNotFound, "Project was not found")
	}
	if values[3].ModRevision != project.Revision {
		return stateConflict("project", project.Record.ID)
	}
	if values[4] == nil {
		return errs.New(errs.KindServiceNotFound, "Route target Service was not found")
	}
	if values[4].ModRevision != target.Revision {
		return stateConflict("service", target.Record.Desired.ID)
	}
	for _, index := range []int{5, 6, 7, 8} {
		if values[index] != nil {
			return errs.New(errs.KindResourceInUse, "Route hierarchy or target deletion is in progress")
		}
	}
	if expected == 10 && values[9] != nil {
		return errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
	}
	return stateConflict("route", record.Desired.ID)
}
