package etcd

import (
	"context"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type preparedEnvironmentBlueprintZonePool struct {
	currentRevision int64
	value           []byte
}

func (repository *HierarchyRepository) prepareEnvironmentBlueprintZonePoolAtRevision(
	ctx context.Context,
	environment hierarchyrecord.EnvironmentRecord,
	desired []projectionrecord.EnvironmentZoneProjection,
	readRevision int64,
) (preparedEnvironmentBlueprintZonePool, error) {
	current, err := repository.getEnvironmentBlueprintZoneRegistryAtRevision(
		ctx, environment.ID, readRevision,
	)
	if err != nil {
		return preparedEnvironmentBlueprintZonePool{}, err
	}
	next := zonePoolRegistry{Reservations: make(map[string]string, len(desired))}
	for _, projection := range desired {
		zone := zonerecord.Record(projection)
		if projection.EnvironmentID != environment.ID {
			return preparedEnvironmentBlueprintZonePool{}, errs.New(
				errs.KindValidationFailed, "Blueprint Zone does not belong to its Environment",
			)
		}
		next, err = next.reserve(environment, zone)
		if err != nil {
			return preparedEnvironmentBlueprintZonePool{}, err
		}
	}
	value, err := recordcodec.Encode("zone_pool_registry", next)
	if err != nil {
		return preparedEnvironmentBlueprintZonePool{}, err
	}
	return preparedEnvironmentBlueprintZonePool{currentRevision: current.Revision, value: value}, nil
}

func (repository *HierarchyRepository) getEnvironmentBlueprintZoneRegistryAtRevision(
	ctx context.Context,
	environmentID string,
	readRevision int64,
) (etcdstore.Versioned[zonePoolRegistry], error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{zonePoolRegistryKey(environmentID)}, Revision: readRevision,
	})
	if err != nil {
		return etcdstore.Versioned[zonePoolRegistry]{}, err
	}
	if result == nil || result.ReadRevision != readRevision || len(result.Values) != 1 {
		return etcdstore.Versioned[zonePoolRegistry]{}, errs.New(
			errs.KindInternal,
			"Zone pool registry read is incomplete",
		)
	}
	defer clearKeyValues(result.Values)
	if result.Values[0] == nil {
		return etcdstore.Versioned[zonePoolRegistry]{
			Record: zonePoolRegistry{Reservations: map[string]string{}}, ReadRevision: readRevision,
		}, nil
	}
	registry, err := recordcodec.Decode[zonePoolRegistry](result.Values[0].Value, "zone_pool_registry")
	if err != nil || validateZonePoolRegistry(registry) != nil {
		return etcdstore.Versioned[zonePoolRegistry]{}, corruptZonePoolRegistry()
	}
	return etcdstore.Versioned[zonePoolRegistry]{
		Record: registry, Revision: result.Values[0].ModRevision, ReadRevision: readRevision,
	}, nil
}
