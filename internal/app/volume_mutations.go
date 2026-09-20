package app

import (
	"github.com/AlanD20/groundplane/internal/controller"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumeremoval"
	"github.com/AlanD20/groundplane/internal/volume"
)

type volumeEvidenceStore interface {
	etcdstore.Store
	VolumeRemovalEvidenceTransactionSize([]etcdstore.Condition, []etcdstore.Mutation) (int, error)
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
