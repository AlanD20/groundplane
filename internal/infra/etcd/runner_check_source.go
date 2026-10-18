package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RunnerCheckSource struct {
	source       runnerchecks.Source
	runner       runners.RunnerRecord
	parents      runners.RunnerParents
	conditions   []keyvalue.Condition
	readRevision int64
}

func (source RunnerCheckSource) Configuration() runnerchecks.Source {
	value := source.source
	value.Desired.Labels = slices.Clone(value.Desired.Labels)
	return value
}
func (source RunnerCheckSource) Runner() runners.RunnerRecord {
	value := source.runner
	value.Desired.Labels = slices.Clone(value.Desired.Labels)
	return value
}

func (repository *RunnerRepository) LoadRunnerCheckSource(
	ctx context.Context,
	runnerID string,
	revision int64,
) (RunnerCheckSource, error) {
	if ids.Validate(ids.KindRunner, runnerID) != nil || revision <= 0 {
		return RunnerCheckSource{}, errs.New(
			errs.KindValidationFailed,
			"Runner check requires its current id and revision",
		)
	}
	keys := []string{
		runners.RunnerKey(runnerID),
		runners.RunnerLifecycleKey(runnerID),
		runners.RunnerRuntimeOwnershipKey(runnerID),
		deletions.TombstoneKey(
			string(deletions.DeletionTargetRunner),
			runnerID,
		),
		runnerchecks.ActiveKey(runnerID),
		runnerchecks.LatestKey(runnerID),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return RunnerCheckSource{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return RunnerCheckSource{}, errs.New(errs.KindInternal, "Runner diagnostic source read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil || read.Values[1] == nil {
		return RunnerCheckSource{}, errs.New(errs.KindRunnerNotFound, "Runner was not found")
	}
	if read.Values[0].ModRevision != revision {
		return RunnerCheckSource{}, errs.New(
			errs.KindStateConflict,
			"Runner changed; refresh before checking connections",
		)
	}
	if read.Values[2] == nil || read.Values[3] != nil || read.Values[4] != nil {
		return RunnerCheckSource{}, errs.New(
			errs.KindResourceInUse,
			"Runner runtime is unavailable, being removed or already has an active connection check",
		)
	}
	current, err := runners.DecodeRunnerAggregate(read.Values[0], read.Values[1])
	if err != nil {
		return RunnerCheckSource{}, err
	}
	ownership, err := runners.DecodeRunnerRuntimeOwnership(read.Values[2].Value)
	if err != nil {
		return RunnerCheckSource{}, err
	}
	if current.Desired.ID != runnerID || current.ProvisioningState != runners.RunnerProvisioningReady ||
		ownership.RunnerID != runnerID ||
		ownership.RuntimeEpoch != current.RuntimeEpoch {
		return RunnerCheckSource{}, errs.New(
			errs.KindStateConflict,
			"Runner must have a completed registration and current runtime before checking connections",
		)
	}
	parents, err := repository.ResolveRunnerParentsAtRevision(ctx, current.Desired, read.ReadRevision)
	if err != nil {
		return RunnerCheckSource{}, err
	}
	source := RunnerCheckSource{
		source: runnerchecks.Source{
			Software:          current.Software,
			RunnerID:          runnerID,
			RunnerRevision:    revision,
			LifecycleRevision: read.Values[1].ModRevision,
			Desired:           current.Desired,
			Ownership:         ownership,
			ContainerID:       current.ContainerID,
		},
		runner:       current,
		parents:      parents,
		readRevision: read.ReadRevision,
	}
	if err := source.source.Validate(); err != nil {
		return RunnerCheckSource{}, err
	}
	for index, key := range keys {
		source.conditions = append(
			source.conditions,
			keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])},
		)
	}
	source.conditions = append(source.conditions, parents.AdmissionConditions()...)
	return source, nil
}

func (repository *RunnerRepository) RunnerCheckSourcesChanged(
	ctx context.Context,
	source runnerchecks.Source,
) (bool, error) {
	if err := source.Validate(); err != nil {
		return false, err
	}
	keys := []string{
		runners.RunnerKey(source.RunnerID),
		runners.RunnerLifecycleKey(source.RunnerID),
		runners.RunnerRuntimeOwnershipKey(source.RunnerID),
		deletions.TombstoneKey(
			string(deletions.DeletionTargetRunner),
			source.RunnerID,
		),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return false, errs.New(errs.KindInternal, "Runner diagnostic freshness read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil || read.Values[3] != nil {
		return true, nil
	}
	current, err := runners.DecodeRunnerAggregate(read.Values[0], read.Values[1])
	if err != nil {
		return false, err
	}
	ownership, err := runners.DecodeRunnerRuntimeOwnership(read.Values[2].Value)
	if err != nil {
		return false, err
	}
	// Maintenance can republish unchanged ownership. Its storage revision is
	// not a new daemon generation and must not invalidate this recorded result.
	return read.Values[0].ModRevision != source.RunnerRevision ||
		read.Values[1].ModRevision != source.LifecycleRevision ||
		current.ProvisioningState != runners.RunnerProvisioningReady ||
		current.ContainerID != source.ContainerID ||
		current.Software != source.Software ||
		current.Desired.ID != source.RunnerID ||
		ownership != source.Ownership, nil
}
