package routepersistence

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareDefinition compares the authoritative desired head and separately
// maintains mutable lookup/observation records. A Blueprint Route's revision is
// the head revision; it must never be used as a standalone record revision.
func (repository *Repository) PrepareDefinition(ctx context.Context,
	environment keyvalue.Versioned[hierarchy.EnvironmentRecord], project keyvalue.Versioned[hierarchy.ProjectRecord],
	target keyvalue.Versioned[services.ServiceRecord], current *keyvalue.Versioned[routes.Record], record routes.Record,
	headRevision, readRevision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	if err := ValidateRouteHierarchy(ctx, environment, project, target, record); err != nil {
		return nil, nil, err
	}
	if headRevision <= 0 || readRevision < headRevision {
		return nil, nil, errs.New(errs.KindStateConflict, "Route publication requires its captured desired head")
	}
	projection, found, err := environmentqueries.ReadDeclaredResources(ctx, repository.store, environment.Record.ID, readRevision)
	if err != nil {
		return nil, nil, err
	}
	if !found || projection.Revision != headRevision {
		return nil, nil, errs.New(errs.KindStateConflict, "Route desired head changed before publication")
	}
	matched := false
	for _, desired := range projection.Record.DesiredRoutes {
		if desired.Desired.ID != record.Desired.ID {
			if desired.Desired.Host == record.Desired.Host && desired.Desired.Path == record.Desired.Path {
				return nil, nil, errs.New(errs.KindStateConflict, "Another Route already owns this host and path")
			}
			continue
		}
		matched = true
		if current == nil || ValidateRouteVersion(*current) != nil || current.Revision != headRevision ||
			current.Record.EnvironmentID != desired.EnvironmentID || current.Record.Desired != desired.Desired ||
			current.Record.DesiredGeneration != desired.DesiredGeneration {
			return nil, nil, errs.New(errs.KindStateConflict, "Route publication differs from its captured desired definition")
		}
		expected, err := routes.ReplaceDesired(current.Record, record.Desired)
		if err != nil {
			return nil, nil, err
		}
		expected.Observed = record.Observed
		if expected != record {
			return nil, nil, errs.New(errs.KindStateConflict, "Route publication changed its validated generation")
		}
	}
	if matched != (current != nil) || current == nil && record.DesiredGeneration != 1 {
		return nil, nil, errs.New(errs.KindStateConflict, "Route create or edit no longer matches the desired head")
	}
	keys := []string{routes.RecordKey(record.Desired.ID), routes.OwnerKey(record.EnvironmentID, record.Desired.ID),
		routes.MatchKey(record.EnvironmentID, record.Desired.Host, record.Desired.Path), routes.ObservationKey(record.Desired.ID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: readRevision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != readRevision || len(read.Values) != len(keys) {
		return nil, nil, errs.New(errs.KindStateConflict, "Route publication metadata snapshot is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for index, value := range read.Values {
		if value == nil {
			continue
		}
		if value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > readRevision {
			return nil, nil, errs.New(errs.KindInternal, "Route publication metadata snapshot is invalid")
		}
		if current == nil {
			return nil, nil, errs.New(errs.KindStateConflict, "Route create identity or match is already retained")
		}
		switch index {
		case 0:
			stored, err := routes.DecodeRecord(value.Value)
			if err != nil || stored.EnvironmentID != record.EnvironmentID || stored.Desired.ID != record.Desired.ID ||
				stored.Desired.Host != record.Desired.Host || stored.Desired.Path != record.Desired.Path ||
				stored.Desired.TargetServiceID != record.Desired.TargetServiceID || stored.Desired.TargetPort != record.Desired.TargetPort {
				return nil, nil, errs.New(errs.KindStateConflict, "Route retained metadata belongs to another definition")
			}
		case 1, 2:
			if string(value.Value) != record.Desired.ID {
				return nil, nil, errs.New(errs.KindStateConflict, "Route lookup is owned by another resource")
			}
		case 3:
			stored, err := routes.DecodeObservation(value.Value)
			if err != nil || stored.EnvironmentID != record.EnvironmentID || stored.RouteID != record.Desired.ID ||
				stored.DesiredGeneration > record.DesiredGeneration {
				return nil, nil, errs.New(errs.KindStateConflict, "Route observation belongs to another generation")
			}
		}
	}
	guards := routeWriteConditions(environment, project, target, record, nil, 0, 0)
	for index := 0; index < 3; index++ {
		guards[index].ModRevision = keyvalue.RevisionOf(read.Values[index])
	}
	guards = append(guards, keyvalue.Condition{Key: blueprints.EnvironmentBlueprintHeadKey(record.EnvironmentID), ModRevision: headRevision},
		keyvalue.Condition{Key: keys[3], ModRevision: keyvalue.RevisionOf(read.Values[3])})
	value, err := routes.EncodeRecord(record)
	if err != nil {
		return nil, nil, err
	}
	observation, err := routes.NewObservationRecord(record.EnvironmentID, record.Desired.ID, record.DesiredGeneration, record.Observed)
	if err != nil {
		clear(value)
		return nil, nil, err
	}
	observed, err := routes.EncodeObservation(observation)
	if err != nil {
		clear(value)
		return nil, nil, err
	}
	return guards, []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: keys[0], Value: value},
		{Type: keyvalue.MutationPut, Key: keys[1], Value: []byte(record.Desired.ID)},
		{Type: keyvalue.MutationPut, Key: keys[2], Value: []byte(record.Desired.ID)},
		{Type: keyvalue.MutationPut, Key: keys[3], Value: observed},
	}, nil
}
