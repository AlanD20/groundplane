package routepersistence

import (
	"context"
	deletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	environmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) CreateRoute(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[servicerecord.ServiceRecord],
	record routerecord.Record,
) (etcdstore.Versioned[routerecord.Record], error) {
	conditions, mutations, classify, err := repository.prepareRouteCreation(ctx, environment, project, target, record)
	if err != nil {
		return etcdstore.Versioned[routerecord.Record]{}, err
	}
	defer etcdstore.ClearMutationValues(mutations)
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcdstore.Versioned[routerecord.Record]{}, err
	}
	if !result.Succeeded {
		return etcdstore.Versioned[routerecord.Record]{}, classify(result.Revision, result.FailureReads)
	}
	return etcdstore.Versioned[routerecord.Record]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *Repository) prepareRouteCreation(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[servicerecord.ServiceRecord],
	record routerecord.Record,
) ([]etcdstore.Condition, []etcdstore.Mutation, func(int64, []*etcdstore.KeyValue) error, error) {
	if err := ValidateRouteHierarchy(ctx, environment, project, target, record); err != nil {
		return nil, nil, nil, err
	}
	value, err := routerecord.EncodeRecord(record)
	if err != nil {
		return nil, nil, nil, err
	}
	conditions := routeWriteConditions(environment, project, target, record, nil, 0, 0)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: routerecord.RecordKey(record.Desired.ID), Value: value},
		{
			Type: etcdstore.MutationPut, Key: routerecord.OwnerKey(record.EnvironmentID, record.Desired.ID),
			Value: []byte(record.Desired.ID),
		},
		{
			Type: etcdstore.MutationPut, Key: routerecord.MatchKey(record.EnvironmentID, record.Desired.Host, record.Desired.Path),
			Value: []byte(record.Desired.ID),
		},
	}
	classify := func(_ int64, values []*etcdstore.KeyValue) error {
		return classifyRouteWriteConflict(values, environment, project, target, record, 0)
	}
	return conditions, mutations, classify, nil
}

func (repository *Repository) GetRoute(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[routerecord.Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[routerecord.Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindRoute, id); err != nil {
		return etcdstore.Versioned[routerecord.Record]{}, err
	}
	return environmentqueries.FindRouteAtRevision(ctx, repository.store, id, 0)
}

func (repository *Repository) ListRoutes(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[routerecord.Record], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Page[routerecord.Record]{}, err
	}
	return environmentqueries.ListRoutes(ctx, repository.store, environmentID, request)
}

// SnapshotRevision returns one truthful MVCC view for a multi-collection
// Route planning read. Empty Route collections still produce a store revision.
func (repository *Repository) SnapshotRevision(ctx context.Context) (int64, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return 0, err
	}
	result, err := repository.store.Range(ctx, etcdstore.RangeRequest{Prefix: routerecord.RecordPrefix, Limit: 1})
	if err != nil {
		return 0, err
	}
	if result == nil || result.ReadRevision <= 0 {
		return 0, errs.New(errs.KindInternal, "Route planning snapshot revision is unavailable")
	}
	defer recordquery.ClearRangeKeyValues(result.Values)
	return result.ReadRevision, nil
}

func (repository *Repository) ReplaceDesired(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[servicerecord.ServiceRecord],
	current etcdstore.Versioned[routerecord.Record],
	desired core.Route,
) (etcdstore.Versioned[routerecord.Record], error) {
	replacement, conditions, mutations, classify, err := repository.prepareRouteReplacement(
		ctx, environment, project, target, current, desired,
	)
	if err != nil {
		return etcdstore.Versioned[routerecord.Record]{}, err
	}
	defer etcdstore.ClearMutationValues(mutations)
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcdstore.Versioned[routerecord.Record]{}, err
	}
	if !result.Succeeded {
		return etcdstore.Versioned[routerecord.Record]{}, classify(result.Revision, result.FailureReads)
	}
	return etcdstore.Versioned[routerecord.Record]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *Repository) prepareRouteReplacement(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[servicerecord.ServiceRecord],
	current etcdstore.Versioned[routerecord.Record],
	desired core.Route,
) (routerecord.Record, []etcdstore.Condition, []etcdstore.Mutation, func(int64, []*etcdstore.KeyValue) error, error) {
	replacement, err := routerecord.ReplaceDesired(current.Record, desired)
	if err != nil {
		return routerecord.Record{}, nil, nil, nil, err
	}
	if err := ValidateRouteHierarchy(ctx, environment, project, target, replacement); err != nil {
		return routerecord.Record{}, nil, nil, nil, err
	}
	if err := ValidateRouteVersion(current); err != nil {
		return routerecord.Record{}, nil, nil, nil, err
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			routerecord.OwnerKey(current.Record.EnvironmentID, current.Record.Desired.ID),
			routerecord.MatchKey(
				current.Record.EnvironmentID,
				current.Record.Desired.Host,
				current.Record.Desired.Path,
			),
		},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return routerecord.Record{}, nil, nil, nil, err
	}
	if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != current.Record.Desired.ID ||
		string(indexes.Values[1].Value) != current.Record.Desired.ID {
		return routerecord.Record{}, nil, nil, nil, errs.New(errs.KindInternal, "Route indexes are missing or corrupt")
	}
	value, err := routerecord.EncodeRecord(replacement)
	if err != nil {
		return routerecord.Record{}, nil, nil, nil, err
	}
	conditions := routeWriteConditions(
		environment, project, target, current.Record, &current,
		indexes.Values[0].ModRevision, indexes.Values[1].ModRevision,
	)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: routerecord.RecordKey(replacement.Desired.ID), Value: value},
	}
	classify := func(_ int64, values []*etcdstore.KeyValue) error {
		return classifyRouteWriteConflict(values, environment, project, target, current.Record, current.Revision)
	}
	return replacement, conditions, mutations, classify, nil
}

func routeWriteConditions(
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[servicerecord.ServiceRecord],
	record routerecord.Record,
	current *etcdstore.Versioned[routerecord.Record],
	ownerRevision int64,
	matchRevision int64,
) []etcdstore.Condition {
	routeCondition := etcdstore.Condition{Key: routerecord.RecordKey(record.Desired.ID)}
	ownerCondition := etcdstore.Condition{Key: routerecord.OwnerKey(record.EnvironmentID, record.Desired.ID)}
	matchCondition := etcdstore.Condition{
		Key: routerecord.MatchKey(record.EnvironmentID, record.Desired.Host, record.Desired.Path),
	}
	if current != nil {
		routeCondition.ModRevision = current.Revision
		ownerCondition.ModRevision = ownerRevision
		matchCondition.ModRevision = matchRevision
	}
	conditions := []etcdstore.Condition{
		routeCondition,
		ownerCondition,
		matchCondition,
		{Key: hierarchyrecord.EnvironmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: hierarchyrecord.ProjectKey(project.Record.ID), ModRevision: project.Revision},
		servicerecord.ServiceDesiredCondition(target),
		{Key: deletions.TombstoneKey("route", record.Desired.ID)},
		{Key: deletions.TombstoneKey("environment", environment.Record.ID)},
		{Key: deletions.TombstoneKey("project", project.Record.ID)},
		{Key: deletions.TombstoneKey("service", target.Record.Desired.ID)},
	}
	if project.Record.TenantID != "" {
		conditions = append(
			conditions,
			etcdstore.Condition{Key: deletions.TombstoneKey("tenant", project.Record.TenantID)},
		)
	}
	return conditions
}

func ValidateRouteHierarchy(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[servicerecord.ServiceRecord],
	record routerecord.Record,
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
	if err := servicerecord.ValidateServiceVersion(target); err != nil {
		return err
	}
	if err := routerecord.ValidateRecord(record); err != nil {
		return err
	}
	if environment.Revision <= 0 || environment.ReadRevision < environment.Revision || project.Revision <= 0 ||
		project.ReadRevision < project.Revision || record.EnvironmentID != environment.Record.ID ||
		environment.Record.ProjectID != project.Record.ID || target.Record.EnvironmentID != environment.Record.ID ||
		target.Record.Desired.ID != record.Desired.TargetServiceID {
		return errs.New(errs.KindValidationFailed, "Route hierarchy or target Service is invalid")
	}
	return nil
}

func ValidateRouteVersion(current etcdstore.Versioned[routerecord.Record]) error {
	if err := routerecord.ValidateRecord(current.Record); err != nil {
		return err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision {
		return errs.New(errs.KindValidationFailed, "Route version metadata is invalid")
	}
	return nil
}

func classifyRouteWriteConflict(
	values []*etcdstore.KeyValue,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[servicerecord.ServiceRecord],
	record routerecord.Record,
	expectedRouteRevision int64,
) error {
	expected := 10
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
		if values[2] != nil {
			return errs.New(errs.KindNameConflict, "Route host and path are already in use")
		}
	} else {
		if values[0] == nil {
			return errs.New(errs.KindRouteNotFound, "Route was not found")
		}
		if values[0].ModRevision != expectedRouteRevision {
			return recordcodec.StateConflict("route", record.Desired.ID)
		}
		for _, index := range []int{1, 2} {
			if values[index] == nil || string(values[index].Value) != record.Desired.ID {
				return errs.New(errs.KindInternal, "Route index changed or is corrupt")
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
	if values[5] == nil {
		return errs.New(errs.KindServiceNotFound, "Route target Service was not found")
	}
	if values[5].ModRevision != target.Revision {
		return recordcodec.StateConflict("service", target.Record.Desired.ID)
	}
	for _, index := range []int{6, 7, 8, 9} {
		if values[index] != nil {
			return errs.New(errs.KindResourceInUse, "Route hierarchy or target deletion is in progress")
		}
	}
	if expected == 11 && values[10] != nil {
		return errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
	}
	return recordcodec.StateConflict("route", record.Desired.ID)
}
