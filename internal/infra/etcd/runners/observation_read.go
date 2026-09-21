package runners

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Reader) GetRunnerObservation(
	ctx context.Context,
	runnerID string,
) (etcdstore.Versioned[RunnerObservationRecord], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[RunnerObservationRecord]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindRunner, runnerID); err != nil {
		return etcdstore.Versioned[RunnerObservationRecord]{}, false, err
	}
	result, err := repository.store.Get(ctx, RunnerObservationKey(runnerID))
	if err != nil {
		return etcdstore.Versioned[RunnerObservationRecord]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[RunnerObservationRecord]{}, false, errs.New(
			errs.KindInternal,
			"runner observation read is empty",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[RunnerObservationRecord]{ReadRevision: result.ReadRevision}, false, nil
	}
	record, err := DecodeRunnerObservation(result.Entry.Value)
	if err != nil || record.RunnerID != runnerID {
		return etcdstore.Versioned[RunnerObservationRecord]{}, false, errs.New(
			errs.KindInternal,
			"runner observation is corrupt",
		)
	}
	return etcdstore.Versioned[RunnerObservationRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}
