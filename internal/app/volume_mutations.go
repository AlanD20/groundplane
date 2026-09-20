package app

import (
	"github.com/AlanD20/groundplane/internal/controller"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumeremoval"
	"github.com/AlanD20/groundplane/internal/volume"
)

type volumeEvidenceStore interface {
	etcd.Store
	VolumeRemovalEvidenceTransactionSize([]etcd.Condition, []etcd.Mutation) (int, error)
}

func configureVolumeMutationPlans(
	volumeRoot string, repository volume.MutationRepository, coordinator *requestidempotency.Coordinator,
	idempotency *etcd.IdempotencyRepository, reads *volume.ReadService, policies *etcd.BackupPolicyRepository,
	store volumeEvidenceStore, plans *controller.TaskPlanResolver, channel *agentChannelRuntime,
) (*volume.MutationService, error) {
	evidence, err := volumeremoval.NewEvidenceRepository(store)
	if err != nil {
		return nil, err
	}
	if err := plans.EnableVolumeRemovalPlans(evidence); err != nil {
		return nil, err
	}
	runtime, err := volumeremoval.NewEnvironmentVolumeRemovalRuntimeRepository(store)
	if err != nil {
		return nil, err
	}
	checkpoints, err := controller.NewVolumeRemovalCheckpointService(runtime)
	if err != nil {
		return nil, err
	}
	channel.volumeCheckpoints = checkpoints
	return volume.NewMutationService(volumeRoot, repository, coordinator, idempotency, reads, policies, evidence)
}
