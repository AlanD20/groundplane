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
)

type BackupExchange func(context.Context, *agentpb.BackupWorkInput, *agentpb.BackupWorkCheckpointRequest) (*agentpb.BackupWorkCheckpointAck, error)

type Backup struct {
	mu         sync.Mutex
	assignment *agentpb.BackupWorkAssignment
	attempt    workexecution.Attempt
	journal    *agentworkjournal.Journal
	exchange   BackupExchange
}

func NewBackup(assignment *agentpb.BackupWorkAssignment, attempt workexecution.Attempt,
	journal *agentworkjournal.Journal, exchange BackupExchange,
) (*Backup, error) {
	owned, captured, err := executionplan.OpenBackupWorkAssignment(assignment, attempt.Executor)
	if err != nil {
		return nil, err
	}
	if attempt.Validate() != nil || !workexecution.SameAssignment(captured, attempt) || attempt.StartedAt == nil ||
		attempt.FinishedAt != nil || journal == nil || exchange == nil {
		return nil, errs.New(errs.KindValidationFailed, "Backup checkpoint delivery requires a started native attempt")
	}
	return &Backup{assignment: owned, attempt: attempt, journal: journal, exchange: exchange}, nil
}

// Checkpoint saves the typed report before sending. On an uncertain response,
// the saved report remains pending and the worker must not repeat its command.
func (delivery *Backup) Checkpoint(ctx context.Context, request *agentpb.BackupWorkCheckpointRequest) error {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	record, err := delivery.journal.PrepareBackup(ctx, delivery.assignment, delivery.attempt, request, time.Now().UTC())
	if err != nil {
		return err
	}
	return delivery.deliver(ctx, record)
}

func (delivery *Backup) Resume(ctx context.Context) error {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	attempt, record, err := delivery.journal.LoadBackup(ctx, delivery.assignment, delivery.attempt.Executor)
	if err != nil || record == nil {
		return err
	}
	if attempt.StartedAt == nil || !attempt.StartedAt.Equal(*delivery.attempt.StartedAt) {
		return errs.New(errs.KindStateConflict, "Backup replay start identity changed")
	}
	return delivery.deliver(ctx, record)
}

// Current returns only acknowledged local evidence. The executor combines it
// with the captured Controller source cursor; absence never proves an effect
// did not occur, and this method does not execute or infer any command.
func (delivery *Backup) Current(
	ctx context.Context,
	ordinal uint32,
) (*agentpb.BackupWorkCheckpointRequest, *agentpb.BackupWorkCheckpointAck, error) {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	_, record, err := delivery.journal.LoadBackup(ctx, delivery.assignment, delivery.attempt.Executor)
	if err != nil || record == nil {
		return nil, nil, err
	}
	if uint64(ordinal) >= uint64(len(record.Sources)) {
		return nil, nil, errs.New(errs.KindValidationFailed, "Backup source is not captured")
	}
	source := record.Sources[ordinal]
	request, err := source.Report()
	if err != nil || request == nil {
		return nil, nil, err
	}
	ack, err := source.Ack()
	if err != nil {
		return nil, nil, err
	}
	if ack == nil {
		return nil, nil, errs.New(errs.KindStateConflict, "Backup command report is not yet acknowledged")
	}
	return request, ack, nil
}

func (delivery *Backup) deliver(ctx context.Context, record *agentworkjournal.BackupRecord) error {
	for _, source := range record.Sources {
		if len(source.Request) == 0 || len(source.Acknowledgement) != 0 {
			continue
		}
		request, err := source.Report()
		if err != nil {
			return err
		}
		ack, err := delivery.exchange(ctx, delivery.assignment.Input, request)
		if err != nil {
			return err
		}
		if err := delivery.journal.AcknowledgeBackup(ctx, delivery.attempt, delivery.assignment.Input, ack); err != nil {
			return err
		}
	}
	return nil
}
