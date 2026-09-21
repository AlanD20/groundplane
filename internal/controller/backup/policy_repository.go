package backup

import (
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backuppolicymutations "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicymutations"
	backupqueries "github.com/AlanD20/groundplane/internal/infra/etcd/backupqueries"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type PolicyRepository struct {
	repository *etcd.BackupPolicyRepository
}

func NewDurableBackupPolicyRepository(
	repository *etcd.BackupPolicyRepository,
) (*PolicyRepository, error) {
	if repository == nil {
		return nil, errs.New(errs.KindInternal, "backup policy repository is required")
	}
	return &PolicyRepository{repository: repository}, nil
}

func (repository *PolicyRepository) GetBackupPolicyProjection(
	ctx context.Context,
	environmentID string,
) (backupqueries.BackupPolicyProjection, error) {
	return repository.repository.GetBackupPolicyProjection(ctx, environmentID)
}

func (repository *PolicyRepository) PrepareBackupPolicyReplacement(
	ctx context.Context,
	input backuppolicy.BackupPolicyReplacementInput,
) (backuppolicymutations.PreparedBackupPolicyReplacement, bool, error) {
	prepared, err := repository.repository.PrepareBackupPolicyReplacement(ctx, input)
	return prepared, prepared.RequiresInitialKey(), err
}

func (repository *PolicyRepository) SupplyBackupPolicyInitialKey(
	ctx context.Context,
	prepared backuppolicymutations.PreparedBackupPolicyReplacement,
	material backuppolicymutations.BackupPolicyInitialKeyMaterial,
) (backuppolicymutations.PreparedBackupPolicyReplacement, error) {
	return repository.repository.SupplyBackupPolicyInitialKey(ctx, prepared, material)
}

func (repository *PolicyRepository) FinalizeBackupPolicySchedule(
	prepared backuppolicymutations.PreparedBackupPolicyReplacement,
	now time.Time,
) (backuppolicymutations.PreparedBackupPolicyReplacement, backupqueries.BackupPolicyProjection, error) {
	finalized, err := prepared.FinalizeSchedule(now)
	if err != nil {
		return backuppolicymutations.PreparedBackupPolicyReplacement{}, backupqueries.BackupPolicyProjection{}, err
	}
	return finalized, finalized.Projection(), nil
}

func (repository *PolicyRepository) ReplaceBackupPolicyProtected(
	ctx context.Context,
	prepared backuppolicymutations.PreparedBackupPolicyReplacement,
	marker idempotencyrecord.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.repository.ReplaceBackupPolicyProtected(ctx, prepared, marker)
}
