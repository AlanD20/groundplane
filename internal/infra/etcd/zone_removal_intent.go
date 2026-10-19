package etcd

import (
	"context"

	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyRepository) GetZoneRemovalIntent(
	ctx context.Context,
	operationID string,
) (etcdstore.Versioned[environmentchanges.ZoneRemovalIntent], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[environmentchanges.ZoneRemovalIntent]{}, false, err
	}
	if ids.Validate(ids.KindOperation, operationID) != nil {
		return etcdstore.Versioned[environmentchanges.ZoneRemovalIntent]{}, false, errs.New(
			errs.KindValidationFailed,
			"Zone removal operation id is invalid",
		)
	}
	result, err := repository.store.Get(ctx, environmentchanges.ZoneRemovalIntentKey(operationID))
	if err != nil {
		return etcdstore.Versioned[environmentchanges.ZoneRemovalIntent]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[environmentchanges.ZoneRemovalIntent]{}, false, errs.New(
			errs.KindInternal,
			"Zone removal intent read is empty",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[environmentchanges.ZoneRemovalIntent]{ReadRevision: result.ReadRevision}, false, nil
	}
	intent, err := environmentchanges.DecodeZoneRemovalIntent(result.Entry.Value)
	if err != nil || intent.OperationID != operationID {
		return etcdstore.Versioned[environmentchanges.ZoneRemovalIntent]{}, false, environmentchanges.CorruptZoneRemovalIntent()
	}
	return etcdstore.Versioned[environmentchanges.ZoneRemovalIntent]{
		Record:       intent,
		Revision:     result.Entry.ModRevision,
		ReadRevision: result.ReadRevision,
	}, true, nil
}
