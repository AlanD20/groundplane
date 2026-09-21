package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"sync"
)

type PreparedManualBackupRun struct {
	Run         backupruntime.BackupRunRecord
	Owner       taskjournal.TaskOwner
	Publication *PreparedBackupRunPublication
}

type preparedBackupRunState struct {
	mu         sync.Mutex
	repository *BackupRuntimeRepository
	plan       backupRunPublicationPlan
	consumed   bool
}

// PreparedBackupRunPublication is shared-state one-shot authority.
type PreparedBackupRunPublication struct{ state *preparedBackupRunState }

func (publication *PreparedBackupRunPublication) Record() backupruntime.BackupRunRecord {
	if publication == nil || publication.state == nil {
		return backupruntime.BackupRunRecord{}
	}
	publication.state.mu.Lock()
	defer publication.state.mu.Unlock()
	return backupruntime.CloneBackupRunPublicationRecord(publication.state.plan.record)
}

func (publication *PreparedBackupRunPublication) Publish(
	ctx context.Context,
	task TaskRecord,
	sealed *agentpb.ExecutionPlan,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if publication == nil || publication.state == nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"backup run publication is not prepared",
		)
	}
	publication.state.mu.Lock()
	if publication.state.consumed || publication.state.repository == nil {
		publication.state.mu.Unlock()
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict, "backup run publication was already consumed",
		)
	}
	publication.state.consumed = true
	repository := publication.state.repository
	plan := publication.state.plan
	publication.state.repository = nil
	publication.state.plan = backupRunPublicationPlan{}
	publication.state.mu.Unlock()
	defer plan.clear()
	if task.Actor != taskjournal.TaskActorOperator && task.Actor != taskjournal.TaskActorSystem {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "backup run task actor must be operator or system",
		)
	}
	initiation, err := newTaskInitiation(task.Owner, task.Actor)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	mutationPlan, err := plan.taskIdempotencyPlan(task, sealed, marker, initiation)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, mutationPlan)
}

func (publication *PreparedBackupRunPublication) Clear() {
	if publication == nil || publication.state == nil {
		return
	}
	publication.state.mu.Lock()
	defer publication.state.mu.Unlock()
	publication.state.plan.clear()
	publication.state.plan = backupRunPublicationPlan{}
	publication.state.repository = nil
	publication.state.consumed = true
}
