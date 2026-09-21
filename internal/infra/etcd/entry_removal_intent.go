package etcd

import (
	"context"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyRepository) GetEntryRemovalIntent(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[environmentchanges.EntryRemovalIntent], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[environmentchanges.EntryRemovalIntent]{}, false, err
	}
	if recordcodec.ValidateID(ids.KindTask, taskID) != nil {
		return etcdstore.Versioned[environmentchanges.EntryRemovalIntent]{}, false, errs.New(
			errs.KindValidationFailed,
			"Entry removal intent Task id is invalid",
		)
	}
	result, err := repository.store.Get(ctx, environmentchanges.EntryRemovalIntentKey(taskID))
	if err != nil {
		return etcdstore.Versioned[environmentchanges.EntryRemovalIntent]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[environmentchanges.EntryRemovalIntent]{}, false, errs.New(
			errs.KindInternal,
			"Entry removal intent read is empty",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[environmentchanges.EntryRemovalIntent]{ReadRevision: result.ReadRevision}, false, nil
	}
	intent, err := environmentchanges.DecodeEntryRemovalIntent(result.Entry.Value)
	if err != nil || intent.TaskID != taskID {
		return etcdstore.Versioned[environmentchanges.EntryRemovalIntent]{}, false, environmentchanges.CorruptEntryRemovalIntent()
	}
	return etcdstore.Versioned[environmentchanges.EntryRemovalIntent]{
		Record: intent, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}
