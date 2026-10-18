package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PublishBackupKeyRotation joins captured key authority, the Environment lock,
// one native Task/work item and request replay in one atomic transaction.
func (repository *BackupPolicyRepository) PublishBackupKeyRotation(ctx context.Context,
	prepared PreparedBackupKeyRotation, marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	publication := prepared.Publication
	if publication == nil || publication.state == nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "backup key rotation publication is not prepared")
	}
	publication.state.mu.Lock()
	if publication.state.consumed || publication.state.repository != repository {
		publication.state.mu.Unlock()
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "backup key rotation publication is unavailable")
	}
	publication.state.consumed = true
	plan := publication.state.plan
	publication.state.repository, publication.state.plan = nil, backupKeyRotationPublicationPlan{}
	publication.state.mu.Unlock()
	defer plan.clear()
	reference, err := backupruntime.KeyRotationInput(plan.record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if plan.task.Intent != reference || plan.item.Input == nil || *plan.item.Input != reference || marker.TaskID != plan.task.ID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "backup key rotation publication input changed")
	}
	work, err := workqueue.PrepareSingleTask(plan.task, plan.item, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(work.Mutations)
	conditions := append(work.Conditions, plan.conditions...)
	mutations := append(work.Mutations, plan.mutations...)
	initiation, err := newTaskInitiation(plan.owner, taskjournal.TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	classify := func(_ int64, _ []*keyvalue.KeyValue) error {
		return errs.New(errs.KindStateConflict, "backup key rotation publication state changed")
	}
	conditions, classify, err = prepareTaskInitiationFences(initiation, conditions, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	transaction, err := newIdempotencyMutationPlanForMarker(idempotency.IdempotencyMarkerTask, conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return (&IdempotencyRepository{store: repository.store}).Apply(ctx, marker, transaction)
}

func (plan *backupKeyRotationPublicationPlan) clear() {
	if plan == nil {
		return
	}
	keyvalue.ClearMutationValues(plan.mutations)
	clear(plan.record.NextEncryptedIdentity)
	plan.mutations = nil
}
