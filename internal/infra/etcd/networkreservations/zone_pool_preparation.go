package networkreservations

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ZonePoolChange struct {
	currentRevision int64
	value           []byte
}

func (repository *Planner) PrepareZonePoolAtRevision(
	ctx context.Context,
	environment hierarchyrecord.EnvironmentRecord,
	desired []zonerecord.Record,
	readRevision int64,
) (ZonePoolChange, error) {
	current, err := repository.getEnvironmentBlueprintZoneRegistryAtRevision(
		ctx, environment.ID, readRevision,
	)
	if err != nil {
		return ZonePoolChange{}, err
	}
	next := ZonePoolRegistry{Reservations: make(map[string]string, len(desired))}
	for _, projection := range desired {
		zone := zonerecord.Record(projection)
		if projection.EnvironmentID != environment.ID {
			return ZonePoolChange{}, errs.New(
				errs.KindValidationFailed, "Blueprint Zone does not belong to its Environment",
			)
		}
		next, err = next.Reserve(environment, zone)
		if err != nil {
			return ZonePoolChange{}, err
		}
	}
	value, err := recordcodec.Encode("zone_pool_registry", next)
	if err != nil {
		return ZonePoolChange{}, err
	}
	return ZonePoolChange{currentRevision: current.Revision, value: value}, nil
}

func (repository *Planner) getEnvironmentBlueprintZoneRegistryAtRevision(
	ctx context.Context,
	environmentID string,
	readRevision int64,
) (etcdstore.Versioned[ZonePoolRegistry], error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{ZonePoolRegistryKey(environmentID)}, Revision: readRevision,
	})
	if err != nil {
		return etcdstore.Versioned[ZonePoolRegistry]{}, err
	}
	if result == nil || result.ReadRevision != readRevision || len(result.Values) != 1 {
		return etcdstore.Versioned[ZonePoolRegistry]{}, errs.New(
			errs.KindInternal,
			"Zone pool registry read is incomplete",
		)
	}
	defer etcdstore.ClearValues(result.Values)
	if result.Values[0] == nil {
		return etcdstore.Versioned[ZonePoolRegistry]{
			Record: ZonePoolRegistry{Reservations: map[string]string{}}, ReadRevision: readRevision,
		}, nil
	}
	registry, err := recordcodec.Decode[ZonePoolRegistry](result.Values[0].Value, "zone_pool_registry")
	if err != nil || ValidateZonePoolRegistry(registry) != nil {
		return etcdstore.Versioned[ZonePoolRegistry]{}, CorruptZonePoolRegistry()
	}
	return etcdstore.Versioned[ZonePoolRegistry]{
		Record: registry, Revision: result.Values[0].ModRevision, ReadRevision: readRevision,
	}, nil
}
