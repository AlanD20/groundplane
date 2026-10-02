package agent

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const maximumControlBurst = 4

// Each reserved assignment can own only one queued frame. Its producer waits
// for the sole channel writer, so the FIFO rotates across active assignments.
// Cancellation never clears a buffer while the writer could still be using it.
type backupBulkFrame struct {
	frame       *agentpb.BackupConfigTransfer
	volume      *agentpb.BackupVolumeManifestTransfer
	ctx         context.Context
	reservation *taskReservation
	completed   chan error
}

func (frame *backupBulkFrame) taskID() string {
	if frame.volume != nil {
		return frame.volume.TaskId
	}
	return frame.frame.TaskId
}

func (pool *WorkerPool) publishBackupVolumeFrame(ctx context.Context,
	frame *agentpb.BackupVolumeManifestTransfer) error {
	if ctx == nil || frame == nil {
		return errs.New(errs.KindValidationFailed, "agent: Volume bulk frame is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	binding, err := pool.volumeBinding(
		frame.TaskId,
		frame.AssignmentId,
		frame.StepId,
		frame.TransferId,
		frame.Direction,
	)
	if err != nil {
		return err
	}
	owned, err := backupvolumetransfer.ValidateFrame(binding, frame)
	if err != nil {
		return err
	}
	pool.mu.Lock()
	reservation := pool.reservations[frame.TaskId]
	if pool.stopped || reservation == nil || reservation.terminalProduced || reservation.ctx.Err() != nil ||
		pool.bulkPending[frame.TaskId] != nil {
		pool.mu.Unlock()
		return errs.New(errs.KindStateConflict, "agent: Volume bulk assignment is not active")
	}
	queued := &backupBulkFrame{volume: owned, ctx: ctx, reservation: reservation, completed: make(chan error, 1)}
	pool.bulkPending[frame.TaskId] = queued
	select {
	case pool.bulkFrames <- queued:
		pool.mu.Unlock()
	default:
		delete(pool.bulkPending, frame.TaskId)
		pool.mu.Unlock()
		return errs.New(errs.KindInternal, "agent: Volume bulk queue exceeds reserved capacity")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-reservation.ctx.Done():
		return reservation.ctx.Err()
	case err := <-queued.completed:
		return err
	}
}

func (pool *WorkerPool) publishBackupConfigFrame(ctx context.Context, frame *agentpb.BackupConfigTransfer) error {
	if ctx == nil || frame == nil {
		return errs.New(errs.KindValidationFailed, "agent: Config bulk frame is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	pool.mu.Lock()
	reservation := pool.reservations[frame.TaskId]
	if pool.stopped || reservation == nil || reservation.terminalProduced ||
		reservation.ctx.Err() != nil || pool.bulkPending[frame.TaskId] != nil ||
		reservation.assignment.AssignmentID != frame.AssignmentId {
		pool.mu.Unlock()
		return errs.New(errs.KindStateConflict, "agent: Config bulk assignment is not active")
	}
	var selected *agentpb.BackupStepAuthority
	for _, step := range reservation.assignment.BackupAuthority.GetSteps() {
		if step.StepId == frame.StepId && step.ExecutionId == frame.ExecutionId &&
			step.GetRestore().GetConfig() != nil {
			selected = step
			break
		}
	}
	if selected == nil || !time.Now().Before(time.Unix(0, int64(selected.StepDeadlineUnixNano))) {
		pool.mu.Unlock()
		return errs.New(errs.KindStateConflict, "agent: Config bulk step is not active")
	}
	binding := backupconfigtransfer.Binding{TaskID: frame.TaskId, AssignmentID: frame.AssignmentId,
		StepID: selected.StepId, ExecutionID: selected.ExecutionId, TransferID: frame.TransferId,
		Direction: agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE}
	owned, err := backupconfigtransfer.ValidateFrame(binding, frame)
	if err != nil {
		pool.mu.Unlock()
		return err
	}
	queued := &backupBulkFrame{frame: owned, ctx: ctx, reservation: reservation, completed: make(chan error, 1)}
	pool.bulkPending[frame.TaskId] = queued
	select {
	case pool.bulkFrames <- queued:
		pool.mu.Unlock()
	default:
		delete(pool.bulkPending, frame.TaskId)
		pool.mu.Unlock()
		backupconfigtransfer.ClearFrame(owned)
		return errs.New(errs.KindInternal, "agent: Config bulk queue exceeds reserved capacity")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-reservation.ctx.Done():
		return reservation.ctx.Err()
	case err := <-queued.completed:
		return err
	}
}

func (pool *WorkerPool) sendBackupBulkFrame(stream agentStream, queued *backupBulkFrame) error {
	err := queued.ctx.Err()
	if err == nil {
		err = queued.reservation.ctx.Err()
	}
	if err != nil {
		pool.finishBackupBulkFrame(queued, err)
		return nil // An aborted producer cannot invalidate other assignments.
	}
	if queued.volume != nil {
		err = stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_BackupVolumeManifestTransfer{
			BackupVolumeManifestTransfer: queued.volume,
		}})
	} else {
		err = stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_BackupConfigTransfer{
			BackupConfigTransfer: queued.frame,
		}})
	}
	pool.finishBackupBulkFrame(queued, err)
	return err
}

func (pool *WorkerPool) finishBackupBulkFrame(queued *backupBulkFrame, err error) {
	pool.mu.Lock()
	if pool.bulkPending[queued.taskID()] == queued {
		delete(pool.bulkPending, queued.taskID())
	}
	pool.mu.Unlock()
	if queued.frame != nil {
		backupconfigtransfer.ClearFrame(queued.frame)
	}
	queued.completed <- err
}

// Run calls this only after cancellation and joining all producers. The session
// writer has returned (or is replacing an already drained pool).
func (pool *WorkerPool) drainBackupBulkFrames() {
	for {
		select {
		case queued := <-pool.bulkFrames:
			pool.finishBackupBulkFrame(queued, context.Canceled)
		default:
			return
		}
	}
}
