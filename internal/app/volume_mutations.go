package app

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumeremoval"
	"github.com/AlanD20/groundplane/internal/volume"
)

type volumeEvidenceStore interface {
	GetMany(context.Context, etcd.GetManyRequest) (*etcd.GetManyResult, error)
	Range(context.Context, etcd.RangeRequest) (*etcd.RangeResult, error)
	Transact(context.Context, []etcd.Condition, []etcd.Mutation) (etcd.TransactionResult, error)
	VolumeRemovalEvidenceTransactionSize([]etcd.Condition, []etcd.Mutation) (int, error)
}

func configureVolumeMutationPlans(
	volumeRoot string, repository volume.MutationRepository, coordinator *idempotentintent.Coordinator,
	idempotency *etcd.IdempotencyRepository, reads *volume.ReadService, policies *etcd.BackupPolicyRepository,
	store volumeEvidenceStore, plans *controller.TaskPlanResolver,
) (*volume.MutationService, error) {
	evidence, err := volumeremoval.NewEvidenceRepository(store)
	if err != nil {
		return nil, err
	}
	if err := plans.EnableVolumeRemovalPlans(evidence); err != nil {
		return nil, err
	}
	return volume.NewMutationService(volumeRoot, repository, coordinator, idempotency, reads, policies, evidence)
}
