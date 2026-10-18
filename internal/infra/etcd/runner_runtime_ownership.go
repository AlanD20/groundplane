package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *RunnerRepository) GetRunnerRuntimeOwnership(
	ctx context.Context,
	runnerID string,
) (etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindRunner, runnerID); err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{}, false, err
	}
	result, err := repository.store.Get(ctx, runnerrecord.RunnerRuntimeOwnershipKey(runnerID))
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{}, false, errs.New(
			errs.KindInternal,
			"runner runtime ownership read is missing",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{
			ReadRevision: result.ReadRevision,
		}, false, nil
	}
	record, err := runnerrecord.DecodeRunnerRuntimeOwnership(result.Entry.Value)
	if err != nil || record.RunnerID != runnerID {
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{}, false, errs.New(
			errs.KindInternal,
			"runner runtime ownership is corrupt",
		)
	}
	return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}
