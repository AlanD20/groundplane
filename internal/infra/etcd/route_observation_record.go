package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func routeObservationAtRevision(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	routeID string,
	desiredGeneration uint64,
	revision int64,
) (routerecord.Observation, error) {
	read, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{routerecord.ObservationKey(routeID)}, Revision: revision,
	})
	if err != nil {
		return routerecord.Observation{}, err
	}
	if read == nil || len(read.Values) != 1 || read.ReadRevision != revision {
		return routerecord.Observation{}, errs.New(errs.KindInternal, "Route observation read is incomplete")
	}
	if read.Values[0] == nil {
		return routerecord.Observation{Status: routerecord.ObservedUnserved, DesiredGeneration: desiredGeneration}, nil
	}
	observation, err := routerecord.DecodeObservation(read.Values[0].Value)
	if err != nil {
		return routerecord.Observation{}, err
	}
	if observation.EnvironmentID != environmentID || observation.RouteID != routeID {
		return routerecord.Observation{}, recordcodec.CorruptRecord()
	}
	if observation.DesiredGeneration != desiredGeneration {
		return routerecord.Observation{Status: routerecord.ObservedUnserved, DesiredGeneration: desiredGeneration}, nil
	}
	return routerecord.CloneObservation(observation.Observation), nil
}

func routeRecordFromDesiredProjection(
	ctx context.Context,
	store hierarchyStore,
	projection etcdstore.Versioned[EnvironmentComposeProjection],
	desired EnvironmentRouteProjection,
) (etcdstore.Versioned[routerecord.Record], error) {
	if desired.EnvironmentID != projection.Record.EnvironmentID {
		return etcdstore.Versioned[routerecord.Record]{}, recordcodec.CorruptRecord()
	}
	observation, err := routeObservationAtRevision(
		ctx, store, desired.EnvironmentID, desired.Desired.ID,
		desired.DesiredGeneration, projection.ReadRevision,
	)
	if err != nil {
		return etcdstore.Versioned[routerecord.Record]{}, err
	}
	record := routerecord.Record{
		EnvironmentID: desired.EnvironmentID, Desired: desired.Desired,
		DesiredGeneration: desired.DesiredGeneration, Observed: observation,
	}
	if err := routerecord.ValidateRecord(record); err != nil {
		return etcdstore.Versioned[routerecord.Record]{}, recordcodec.CorruptRecord()
	}
	return etcdstore.Versioned[routerecord.Record]{
		Record: record, Revision: projection.Revision, ReadRevision: projection.ReadRevision,
	}, nil
}
