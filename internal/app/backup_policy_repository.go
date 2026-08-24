package app

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type durableBackupPolicyRepository struct {
	repository *etcd.BackupPolicyRepository
}

func newDurableBackupPolicyRepository(
	repository *etcd.BackupPolicyRepository,
) (*durableBackupPolicyRepository, error) {
	if repository == nil {
		return nil, errs.New(errs.KindInternal, "backup policy repository is required")
	}
	return &durableBackupPolicyRepository{repository: repository}, nil
}

func (repository *durableBackupPolicyRepository) GetBackupPolicyProjection(
	ctx context.Context,
	environmentID string,
) (etcd.BackupPolicyProjection, error) {
	return repository.repository.GetBackupPolicyProjection(ctx, environmentID)
}

func (repository *durableBackupPolicyRepository) PrepareBackupPolicyReplacement(
	ctx context.Context,
	input etcd.BackupPolicyReplacementInput,
) (etcd.PreparedBackupPolicyReplacement, bool, error) {
	prepared, err := repository.repository.PrepareBackupPolicyReplacement(ctx, input)
	return prepared, prepared.RequiresInitialKey(), err
}

func (repository *durableBackupPolicyRepository) SupplyBackupPolicyInitialKey(
	ctx context.Context,
	prepared etcd.PreparedBackupPolicyReplacement,
	material etcd.BackupPolicyInitialKeyMaterial,
) (etcd.PreparedBackupPolicyReplacement, error) {
	return repository.repository.SupplyBackupPolicyInitialKey(ctx, prepared, material)
}

func (repository *durableBackupPolicyRepository) ReplaceBackupPolicyProtected(
	ctx context.Context,
	prepared etcd.PreparedBackupPolicyReplacement,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.repository.ReplaceBackupPolicyProtected(ctx, prepared, marker)
}
