package environmentprojection

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
)

type observationReader interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}

func ReadRoute(
	ctx context.Context,
	store observationReader,
	environmentID string,
	revision int64,
	readRevision int64,
	desired EnvironmentRouteProjection,
) (etcdstore.Versioned[routerecord.Record], error) {
	if desired.EnvironmentID != environmentID {
		return etcdstore.Versioned[routerecord.Record]{}, recordcodec.CorruptRecord()
	}
	observation, err := routerecord.ReadObservationAtRevision(
		ctx, store, desired.EnvironmentID, desired.Desired.ID,
		desired.DesiredGeneration, readRevision,
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
		Record: record, Revision: revision, ReadRevision: readRevision,
	}, nil
}
