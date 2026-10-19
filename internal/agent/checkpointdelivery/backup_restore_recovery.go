package checkpointdelivery

import (
	"context"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type BackupRecoveryExchange func(context.Context, *agentpb.BackupRecoveryAssignment, *agentpb.BackupWorkCheckpointRequest) (*agentpb.BackupWorkCheckpointAck, error)

type BackupRestoreRecovery struct {
	mu         sync.Mutex
	assignment *agentpb.BackupRecoveryAssignment
	attempt    workexecution.Attempt
	journal    *agentworkjournal.Journal
	exchange   BackupRecoveryExchange
}

func NewBackupRestoreRecovery(assignment *agentpb.BackupRecoveryAssignment, attempt workexecution.Attempt,
	journal *agentworkjournal.Journal, exchange BackupRecoveryExchange,
) (*BackupRestoreRecovery, error) {
	owned, captured, _, err := executionplan.OpenBackupRecoveryAssignment(assignment, attempt.Executor)
	if err != nil {
		return nil, err
	}
	if !workexecution.SameAssignment(captured, attempt) || attempt.StartedAt == nil || attempt.FinishedAt != nil ||
		len(owned.Sources) != 1 || owned.Input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE ||
		journal == nil || exchange == nil {
		return nil, errs.New(errs.KindStateConflict, "Restore recovery delivery requires its started attempt")
	}
	return &BackupRestoreRecovery{assignment: owned, attempt: attempt, journal: journal, exchange: exchange}, nil
}

func (delivery *BackupRestoreRecovery) Source() *BackupSource {
	return &BackupSource{recovery: delivery}
}

func (delivery *BackupRestoreRecovery) Resume(ctx context.Context) error {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	_, record, err := delivery.journal.LoadBackupRecovery(ctx, delivery.assignment, delivery.attempt.Executor)
	if err != nil {
		return err
	}
	if record == nil {
		return errs.New(errs.KindStateConflict, "Restore recovery durable start is missing")
	}
	return delivery.deliver(ctx, record)
}

func (delivery *BackupRestoreRecovery) current(ctx context.Context) (*agentpb.BackupStepResume, error) {
	_, record, err := delivery.journal.LoadBackupRecovery(ctx, delivery.assignment, delivery.attempt.Executor)
	if err != nil {
		return nil, err
	}
	if record == nil || record.Completion != nil || record.FailureReason != "" || len(record.Recovery.Report) != 0 {
		return nil, errs.New(errs.KindStateConflict, "Restore recovery is not active")
	}
	saved := record.Recovery.Checkpoint
	if len(saved.Request) == 0 {
		return proto.CloneOf(delivery.assignment.Sources[0]), nil
	}
	ack, err := saved.Ack()
	if err != nil {
		return nil, err
	}
	if ack == nil {
		return nil, errs.New(
			errs.KindStateConflict,
			"Restore recovery has an unacknowledged report; resume delivery first",
		)
	}
	return proto.CloneOf(ack.Source), nil
}

func (delivery *BackupRestoreRecovery) Current(ctx context.Context) (*agentpb.BackupStepResume, error) {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	return delivery.current(ctx)
}

func (delivery *BackupRestoreRecovery) publish(
	ctx context.Context,
	payload *agentpb.BackupWorkCheckpointRequest,
) error {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	cursor, err := delivery.current(ctx)
	if err != nil {
		return err
	}
	progress := cursor.GetRestore()
	if progress == nil || progress.CheckpointSequence == ^uint64(0) || payload == nil {
		return errs.New(errs.KindStateConflict, "Restore recovery progress is unavailable or exhausted")
	}
	request := proto.CloneOf(payload)
	request.Attempt, request.SourceOrdinal = proto.CloneOf(delivery.assignment.Attempt), 0
	request.CheckpointSequence, request.PrecedingCheckpoint = progress.CheckpointSequence+1, proto.CloneOf(
		progress.PrecedingCheckpoint,
	)
	record, err := delivery.journal.PrepareBackupRecoveryCheckpoint(
		ctx,
		delivery.assignment,
		delivery.attempt,
		request,
		time.Now().UTC(),
	)
	if err != nil {
		return err
	}
	return delivery.deliver(ctx, record)
}

func (delivery *BackupRestoreRecovery) deliver(ctx context.Context, record *agentworkjournal.BackupRecord) error {
	saved := record.Recovery.Checkpoint
	if len(saved.Request) == 0 || len(saved.Acknowledgement) != 0 {
		return nil
	}
	request, err := saved.Report()
	if err != nil {
		return err
	}
	ack, err := delivery.exchange(ctx, delivery.assignment, request)
	if err != nil {
		return err
	}
	return delivery.journal.AcknowledgeBackupRecoveryCheckpoint(ctx, delivery.assignment, delivery.attempt, ack)
}
