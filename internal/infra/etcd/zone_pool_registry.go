package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	networkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ListZoneSubnetReservationsAtRevision returns validated reservations from an
// exact MVCC view without exposing the registry persistence record.
func (r *ZoneRepository) ListZoneSubnetReservationsAtRevision(
	ctx context.Context, id string, at int64,
) ([]string, error) {
	stored, err := r.getZonePoolRegistryAtRevision(ctx, id, at)
	if err != nil {
		return nil, err
	}
	reservations := make([]string, 0, len(stored.Record.Reservations))
	for _, subnet := range stored.Record.Reservations {
		reservations = append(reservations, subnet)
	}
	return reservations, nil
}

func (r *ZoneRepository) getZonePoolRegistry(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[networkreservations.ZonePoolRegistry], error) {
	return r.getZonePoolRegistryAtRevision(ctx, id, 0)
}

func (r *ZoneRepository) getZonePoolRegistryAtRevision(
	ctx context.Context,
	id string, revision int64,
) (etcdstore.Versioned[networkreservations.ZonePoolRegistry], error) {
	if r == nil || r.store == nil || ids.Validate(ids.KindEnvironment, id) != nil || revision < 0 {
		return etcdstore.Versioned[networkreservations.ZonePoolRegistry]{}, errs.New(
			errs.KindInternal,
			"Environment capacity read is invalid",
		)
	}
	result, err := r.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{networkreservations.ZonePoolRegistryKey(id)}, Revision: revision},
	)
	if err != nil {
		return etcdstore.Versioned[networkreservations.ZonePoolRegistry]{}, err
	}
	if len(result.Values) != 1 || result.Values[0] == nil {
		return etcdstore.Versioned[networkreservations.ZonePoolRegistry]{
			Record: networkreservations.ZonePoolRegistry{
				Reservations: map[string]string{},
			}, ReadRevision: result.ReadRevision,
		}, nil
	}
	registry, err := recordcodec.Decode[networkreservations.ZonePoolRegistry](
		result.Values[0].Value,
		"zone_pool_registry",
	)
	if err != nil || networkreservations.ValidateZonePoolRegistry(registry) != nil {
		return etcdstore.Versioned[networkreservations.ZonePoolRegistry]{}, networkreservations.CorruptZonePoolRegistry()
	}
	return etcdstore.Versioned[networkreservations.ZonePoolRegistry]{
		Record: registry, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}
