package app

import (
	channeltransport "github.com/AlanD20/groundplane/internal/controller/agentchannel/transport"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	taskcheckpoint "github.com/AlanD20/groundplane/internal/controller/taskcheckpoint"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/controller/volume"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumeremoval"
)

type volumeEvidenceStore interface {
	etcdstore.Store
	VolumeRemovalEvidenceTransactionSize([]etcdstore.Condition, []etcdstore.Mutation) (int, error)
}

func configureVolumeMutationPlans(
	volumeRoot string, repository volume.MutationRepository, coordinator *requestidempotency.Coordinator,
	idempotency *etcd.IdempotencyRepository, reads *volume.ReadService, policies *etcd.BackupPolicyRepository,
	store volumeEvidenceStore, plans *taskplanning.TaskPlanResolver, channel *channeltransport.Runtime,
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
	checkpoints, err := taskcheckpoint.NewVolumeRemovalCheckpointService(runtime)
	if err != nil {
		return nil, err
	}
	channel.VolumeCheckpoints = checkpoints
	return volume.NewMutationService(volumeRoot, repository, coordinator, idempotency, reads, policies, evidence)
}
