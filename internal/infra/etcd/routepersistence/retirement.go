package routepersistence

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareRetirement removes only mutable metadata. Its caller must first prove
// the router has withdrawn the Route and commit that proof with these writes.
func PrepareRetirement(ctx context.Context, store interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
}, environmentID, routeID string, revision int64) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return nil, nil, err
	}
	if store == nil || ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		ids.Validate(ids.KindRoute, routeID) != nil || revision <= 0 {
		return nil, nil, errs.New(errs.KindValidationFailed, "Route retirement identity is invalid")
	}
	keys := []string{routes.RecordKey(routeID), routes.OwnerKey(environmentID, routeID),
		routes.ObservationKey(routeID), deletions.TombstoneKey("route", routeID)}
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, nil, errs.New(errs.KindInternal, "Route retirement snapshot is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := make([]keyvalue.Condition, 0, 5)
	mutations := make([]keyvalue.Mutation, 0, 4)
	for index, value := range read.Values {
		if value != nil && value.Key != keys[index] {
			return nil, nil, errs.New(errs.KindInternal, "Route retirement read changed")
		}
		conditions = append(conditions, keyvalue.Condition{Key: keys[index], ModRevision: keyvalue.RevisionOf(value)})
		if index < 3 && value != nil {
			mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: keys[index]})
		}
	}
	if read.Values[3] != nil {
		return nil, nil, errs.New(errs.KindResourceInUse, "Route already has an active removal")
	}
	if read.Values[2] != nil {
		observation, err := routes.DecodeObservation(read.Values[2].Value)
		if err != nil || observation.EnvironmentID != environmentID || observation.RouteID != routeID {
			return nil, nil, errs.New(errs.KindStateConflict, "Route observation owner changed")
		}
	}
	if read.Values[0] == nil {
		if read.Values[1] != nil {
			return nil, nil, errs.New(errs.KindStateConflict, "Route owner index has no matching definition")
		}
		return conditions, mutations, nil
	}
	record, err := routes.DecodeRecord(read.Values[0].Value)
	if err != nil || record.EnvironmentID != environmentID || record.Desired.ID != routeID ||
		read.Values[1] == nil || string(read.Values[1].Value) != routeID {
		return nil, nil, errs.New(errs.KindStateConflict, "Route retirement owner changed")
	}
	matchKey := routes.MatchKey(environmentID, record.Desired.Host, record.Desired.Path)
	match, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{matchKey}, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if match == nil || match.ReadRevision != revision || len(match.Values) != 1 {
		return nil, nil, errs.New(errs.KindInternal, "Route match snapshot is incomplete")
	}
	defer keyvalue.ClearValues(match.Values)
	if match.Values[0] == nil || match.Values[0].Key != matchKey || string(match.Values[0].Value) != routeID {
		return nil, nil, errs.New(errs.KindStateConflict, "Route match index changed")
	}
	conditions = append(conditions, keyvalue.Condition{Key: matchKey, ModRevision: match.Values[0].ModRevision})
	mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: matchKey})
	return conditions, mutations, nil
}
