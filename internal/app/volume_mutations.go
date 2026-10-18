package app

import (
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/volume"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func configureVolumeMutations(
	repository volume.MutationRepository, coordinator *requestidempotency.Coordinator,
	idempotency *etcd.IdempotencyRepository, reads *volume.ReadService, policies *etcd.BackupPolicyRepository,
) (*volume.MutationService, error) {
	return volume.NewMutationService(repository, coordinator, idempotency, reads, policies)
}
