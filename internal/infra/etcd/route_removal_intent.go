package etcd

import (
	"context"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RouteRemovalTaskPreparation struct {
	Intent environmentchanges.RouteRemovalIntent
	Task   TaskRecord
}

func (repository *HierarchyRepository) GetRouteRemovalIntent(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[environmentchanges.RouteRemovalIntent], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[environmentchanges.RouteRemovalIntent]{}, false, err
	}
	if recordcodec.ValidateID(ids.KindTask, taskID) != nil {
		return etcdstore.Versioned[environmentchanges.RouteRemovalIntent]{}, false, errs.New(
			errs.KindValidationFailed,
			"Route removal intent Task id is invalid",
		)
	}
	result, err := repository.store.Get(ctx, environmentchanges.RouteRemovalIntentKey(taskID))
	if err != nil {
		return etcdstore.Versioned[environmentchanges.RouteRemovalIntent]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[environmentchanges.RouteRemovalIntent]{}, false, errs.New(errs.KindInternal, "Route removal intent read is empty")
	}
	if result.Entry == nil {
		return etcdstore.Versioned[environmentchanges.RouteRemovalIntent]{ReadRevision: result.ReadRevision}, false, nil
	}
	intent, err := environmentchanges.DecodeRouteRemovalIntent(result.Entry.Value)
	if err != nil || intent.TaskID != taskID {
		return etcdstore.Versioned[environmentchanges.RouteRemovalIntent]{}, false, environmentchanges.CorruptRouteRemovalIntent()
	}
	return etcdstore.Versioned[environmentchanges.RouteRemovalIntent]{
		Record:       intent,
		Revision:     result.Entry.ModRevision,
		ReadRevision: result.ReadRevision,
	}, true, nil
}
