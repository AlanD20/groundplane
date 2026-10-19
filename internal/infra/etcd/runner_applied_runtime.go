package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *RunnerRepository) LoadAppliedRunnerPlan(ctx context.Context, runnerID string,
	epoch uint64, containerID string,
) (runnerallocation.RuntimePlan, error) {
	if ids.Validate(ids.KindRunner, runnerID) != nil || epoch == 0 || containerID == "" {
		return runnerallocation.RuntimePlan{}, errs.New(
			errs.KindValidationFailed,
			"Runner applied-runtime identity is invalid",
		)
	}
	read, err := repository.store.Get(ctx, runnerwork.AppliedKey(runnerID))
	if err != nil {
		return runnerallocation.RuntimePlan{}, err
	}
	if read == nil || read.Entry == nil {
		return runnerallocation.RuntimePlan{}, errs.New(
			errs.KindStateConflict,
			"Runner has no applied runtime configuration",
		)
	}
	defer clear(read.Entry.Value)
	record, err := runnerwork.DecodeApplied(read.Entry.Value)
	if err != nil {
		return runnerallocation.RuntimePlan{}, err
	}
	if record.Plan.RunnerID != runnerID || record.Plan.RuntimeEpoch != epoch ||
		record.Evidence.ContainerID != containerID {
		return runnerallocation.RuntimePlan{}, errs.New(
			errs.KindStateConflict,
			"Runner applied runtime differs from its active ownership",
		)
	}
	return record.Plan.Clone(), nil
}
