package backup

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"time"
)

// BackupRunPrepareInput contains only identities and the selected fixed
// revision. The repository fills it with immutable policy, hierarchy, source,
// target, connector, credential, key, and config evidence after idempotency
// replay has been resolved.
type BackupRunPrepareInput struct {
	EnvironmentID string
	TaskID        string
	OperationID   string
	PlanID        string
	StepIDs       []string
	FixedRevision int64
	CreatedAt     time.Time
	Initiator     backupruntime.BackupRunInitiator
	ScheduledAt   *time.Time
}

// BackupRunPrepared is returned by the repository only after all fixed-
// revision evidence has been validated. Publication remains opaque and
// one-shot.
type BackupRunPrepared struct {
	Run         backupruntime.BackupRunRecord
	Owner       etcd.TaskOwner
	Publication backupRunPublication
}

type BackupRunRetryPrepareInput struct {
	SourceTaskID string
	TaskID       string
	CreatedAt    time.Time
}

type BackupRunRetryPrepared struct {
	SourceTask  etcd.TaskRecord
	Run         backupruntime.BackupRunRecord
	Owner       etcd.TaskOwner
	Publication backupRunPublication
}

type backupRunPublication interface {
	Publish(
		context.Context,
		etcd.TaskRecord,
		*agentpb.ExecutionPlan,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	Clear()
}

// BackupRunRepository is the narrow durable seam needed by the bodyless
// manual backup service. Implementations must not read state until after the
// idempotency coordinator has completed ResolveExisting.
type BackupRunRepository interface {
	PrepareBackupRun(context.Context, BackupRunPrepareInput) (BackupRunPrepared, error)
	PrepareBackupRunRetry(context.Context, BackupRunRetryPrepareInput) (BackupRunRetryPrepared, error)
}

func (repository *durableBackupRunRepository) PrepareBackupRunRetry(
	ctx context.Context,
	input BackupRunRetryPrepareInput,
) (BackupRunRetryPrepared, error) {
	prepared, err := repository.runtime.PrepareBackupRunRetry(ctx, etcd.BackupRunRetryInput{
		SourceTaskID: input.SourceTaskID,
		TaskID:       input.TaskID,
		CreatedAt:    input.CreatedAt,
	})
	if err != nil {
		return BackupRunRetryPrepared{}, err
	}
	return BackupRunRetryPrepared{
		SourceTask:  prepared.SourceTask,
		Run:         prepared.Run,
		Owner:       prepared.Owner,
		Publication: prepared.Publication,
	}, nil
}

type durableBackupRunRepository struct {
	runtime         *etcd.BackupRuntimeRepository
	resolvePostgres etcd.BackupPostgresIdentityResolver
}

func NewDurableBackupRunRepository(
	runtime *etcd.BackupRuntimeRepository,
	facts etcd.BackupPostgresIdentityResolver,
) (*durableBackupRunRepository, error) {
	if runtime == nil || facts == nil {
		return nil, errs.New(errs.KindInternal, "backup run repository dependencies are required")
	}
	return &durableBackupRunRepository{runtime: runtime, resolvePostgres: facts}, nil
}

func (repository *durableBackupRunRepository) PrepareBackupRun(
	ctx context.Context,
	input BackupRunPrepareInput,
) (BackupRunPrepared, error) {
	prepared, err := repository.runtime.PrepareManualBackupRun(ctx, etcd.ManualBackupRunInput{
		EnvironmentID: input.EnvironmentID,
		TaskID:        input.TaskID, OperationID: input.OperationID, PlanID: input.PlanID,
		FixedRevision: input.FixedRevision, CreatedAt: input.CreatedAt,
		Initiator: input.Initiator, ScheduledAt: input.ScheduledAt,
	}, repository.resolvePostgres)
	if err != nil {
		return BackupRunPrepared{}, err
	}
	return BackupRunPrepared{
		Run: prepared.Run, Owner: prepared.Owner, Publication: prepared.Publication,
	}, nil
}
