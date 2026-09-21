package etcd

import (
	"context"
	connectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *ConnectorRepository) GetConnectorRemovalIntent(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[connectors.RemovalIntent], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[connectors.RemovalIntent]{}, false, err
	}
	if recordcodec.ValidateID(ids.KindTask, taskID) != nil {
		return etcdstore.Versioned[connectors.RemovalIntent]{}, false, errs.New(
			errs.KindValidationFailed,
			"connector removal intent task id is invalid",
		)
	}
	result, err := repository.store.Get(ctx, connectors.RemovalIntentKey(taskID))
	if err != nil {
		return etcdstore.Versioned[connectors.RemovalIntent]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[connectors.RemovalIntent]{}, false, errs.New(
			errs.KindInternal,
			"connector removal intent read is empty",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[connectors.RemovalIntent]{ReadRevision: result.ReadRevision}, false, nil
	}
	intent, err := connectors.DecodeRemovalIntent(result.Entry.Value)
	if err != nil || intent.TaskID != taskID {
		return etcdstore.Versioned[connectors.RemovalIntent]{}, false, connectors.CorruptRemovalIntent()
	}
	return etcdstore.Versioned[connectors.RemovalIntent]{
		Record: intent, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}
