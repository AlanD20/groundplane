package runners

import (
	"context"

	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RefreshRuntime records newly observed process/socket evidence for the same
// registered container. It cannot advance an epoch, replace a container, or
// race removal into restoring a deleted Runner.
func (repository *Repository) RefreshRuntime(
	ctx context.Context,
	current etcdstore.Versioned[RunnerRecord],
	prior etcdstore.Versioned[RunnerRuntimeOwnershipRecord],
	containerID string,
	updated RunnerRuntimeOwnershipRecord,
	observation RunnerObservationRecord,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if ValidateRunnerRecord(current.Record) != nil || ValidateRunnerRuntimeOwnership(updated) != nil ||
		ValidateRunnerObservation(observation) != nil || current.Record.ProvisioningState != RunnerProvisioningReady ||
		current.Revision <= 0 || current.Record.LifecycleRevision <= 0 || prior.Revision <= 0 ||
		containerID != current.Record.ContainerID || updated.RunnerID != current.Record.Desired.ID ||
		updated.RuntimeEpoch != current.Record.RuntimeEpoch || prior.Record.RunnerID != updated.RunnerID ||
		prior.Record.RuntimeEpoch != updated.RuntimeEpoch || observation.RunnerID != updated.RunnerID || !observation.Online {
		return errs.New(errs.KindStateConflict, "Runner runtime refresh lost its ready ownership")
	}
	ownershipValue, err := EncodeRunnerRuntimeOwnership(updated)
	if err != nil {
		return err
	}
	defer clear(ownershipValue)
	observationValue, err := EncodeRunnerObservation(observation)
	if err != nil {
		return err
	}
	defer clear(observationValue)
	result, err := repository.store.Transact(ctx, []etcdstore.Condition{
		{Key: RunnerKey(updated.RunnerID), ModRevision: current.Revision},
		{Key: RunnerLifecycleKey(updated.RunnerID), ModRevision: current.Record.LifecycleRevision},
		{Key: RunnerRuntimeOwnershipKey(updated.RunnerID), ModRevision: prior.Revision},
		{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetRunner), updated.RunnerID)},
	}, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: RunnerRuntimeOwnershipKey(updated.RunnerID), Value: ownershipValue},
		{Type: etcdstore.MutationPut, Key: RunnerObservationKey(updated.RunnerID), Value: observationValue},
	})
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Runner changed during runtime refresh")
	}
	return nil
}
