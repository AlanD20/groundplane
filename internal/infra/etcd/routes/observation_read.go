package routes

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type observationReader interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}

func ReadObservationAtRevision(
	ctx context.Context,
	store observationReader,
	environmentID string,
	routeID string,
	desiredGeneration uint64,
	revision int64,
) (Observation, error) {
	read, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{ObservationKey(routeID)}, Revision: revision,
	})
	if err != nil {
		return Observation{}, err
	}
	if read == nil || len(read.Values) != 1 || read.ReadRevision != revision {
		return Observation{}, errs.New(errs.KindInternal, "Route observation read is incomplete")
	}
	if read.Values[0] == nil {
		return Observation{Status: ObservedUnserved, DesiredGeneration: desiredGeneration}, nil
	}
	observation, err := DecodeObservation(read.Values[0].Value)
	if err != nil {
		return Observation{}, err
	}
	if observation.EnvironmentID != environmentID || observation.RouteID != routeID {
		return Observation{}, recordcodec.CorruptRecord()
	}
	if observation.DesiredGeneration != desiredGeneration {
		return Observation{Status: ObservedUnserved, DesiredGeneration: desiredGeneration}, nil
	}
	return CloneObservation(observation.Observation), nil
}
