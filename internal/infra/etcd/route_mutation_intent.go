package etcd

import (
	"context"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RouteMutationTaskPreparation struct {
	Intent environmentchanges.RouteMutationIntent
	Task   TaskRecord
}

func (repository *HierarchyRepository) GetRouteMutationIntent(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[environmentchanges.RouteMutationIntent], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[environmentchanges.RouteMutationIntent]{}, false, err
	}
	if recordcodec.ValidateID(ids.KindTask, taskID) != nil {
		return etcdstore.Versioned[environmentchanges.RouteMutationIntent]{}, false, errs.New(
			errs.KindValidationFailed,
			"Route mutation intent Task id is invalid",
		)
	}
	result, err := repository.store.Get(ctx, environmentchanges.RouteMutationIntentKey(taskID))
	if err != nil {
		return etcdstore.Versioned[environmentchanges.RouteMutationIntent]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[environmentchanges.RouteMutationIntent]{}, false, errs.New(
			errs.KindInternal,
			"Route mutation intent read is empty",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[environmentchanges.RouteMutationIntent]{
			ReadRevision: result.ReadRevision,
		}, false, nil
	}
	intent, err := environmentchanges.DecodeRouteMutationIntent(result.Entry.Value)
	if err != nil || intent.TaskID != taskID {
		return etcdstore.Versioned[environmentchanges.RouteMutationIntent]{}, false, environmentchanges.CorruptRouteMutationIntent()
	}
	return etcdstore.Versioned[environmentchanges.RouteMutationIntent]{
		Record:       intent,
		Revision:     result.Entry.ModRevision,
		ReadRevision: result.ReadRevision,
	}, true, nil
}
