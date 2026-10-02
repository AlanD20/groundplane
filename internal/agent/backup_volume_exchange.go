package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"sync"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type backupVolumeExchange struct {
	mu      sync.Mutex
	credits map[string]chan *agentpb.BackupVolumeManifestAckCredit
	frames  map[string]chan *agentpb.BackupVolumeManifestTransfer
}

func newBackupVolumeExchange() *backupVolumeExchange {
	return &backupVolumeExchange{credits: make(map[string]chan *agentpb.BackupVolumeManifestAckCredit),
		frames: make(map[string]chan *agentpb.BackupVolumeManifestTransfer)}
}

func (exchange *backupVolumeExchange) register(authority *agentpb.BackupTaskAuthority) error {
	if authority == nil {
		return nil
	}
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	for _, step := range authority.Steps {
		var directions []agentpb.BackupVolumeManifestDirection
		if step.GetCapture().GetVolume() != nil {
			directions = append(
				directions,
				agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE,
			)
		}
		if step.GetRestore().GetVolume() != nil {
			directions = append(directions,
				agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW,
				agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD)
		}
		for _, direction := range directions {
			transferID, err := backupvolumetransfer.TransferID(step.ExecutionId, direction)
			if err != nil {
				return err
			}
			if exchange.credits[transferID] != nil || exchange.frames[transferID] != nil {
				return errs.New(errs.KindStateConflict, "Volume transfer identity is already reserved")
			}
			if direction == agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW {
				exchange.frames[transferID] = make(chan *agentpb.BackupVolumeManifestTransfer, 1)
			} else {
				exchange.credits[transferID] = make(chan *agentpb.BackupVolumeManifestAckCredit, 1)
			}
		}
	}
	return nil
}

func (exchange *backupVolumeExchange) release(authority *agentpb.BackupTaskAuthority) {
	if exchange == nil || authority == nil {
		return
	}
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	for _, step := range authority.Steps {
		for _, direction := range []agentpb.BackupVolumeManifestDirection{
			agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE,
			agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW,
			agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD,
		} {
			transferID, err := backupvolumetransfer.TransferID(step.ExecutionId, direction)
			if err == nil {
				delete(exchange.credits, transferID)
				delete(exchange.frames, transferID)
			}
		}
	}
}

func (pool *WorkerPool) volumeBinding(taskID, assignmentID, stepID, transferID string,
	direction agentpb.BackupVolumeManifestDirection,
) (backupvolumetransfer.Binding, error) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	reservation := pool.reservations[taskID]
	if pool.stopped || reservation == nil || reservation.terminalProduced || reservation.ctx.Err() != nil ||
		reservation.assignment.AssignmentID != assignmentID {
		return backupvolumetransfer.Binding{}, errs.New(
			errs.KindStateConflict,
			"Volume transfer assignment is not active",
		)
	}
	for _, step := range reservation.assignment.BackupAuthority.GetSteps() {
		if step.StepId != stepID || len(step.StepDigest) != sha256.Size {
			continue
		}
		valid := direction == agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE &&
			step.GetCapture().GetVolume() != nil ||
			step.GetRestore().GetVolume() != nil &&
				(direction == agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW ||
					direction == agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD)
		if !valid {
			continue
		}
		expected, err := backupvolumetransfer.TransferID(step.ExecutionId, direction)
		if err != nil || expected != transferID {
			return backupvolumetransfer.Binding{}, errs.New(
				errs.KindStateConflict,
				"Volume transfer id differs from sealed step",
			)
		}
		binding := backupvolumetransfer.Binding{TaskID: taskID, AssignmentID: assignmentID,
			StepID: stepID, TransferID: transferID, Direction: direction}
		copy(binding.AuthorityDigest[:], step.StepDigest)
		return binding, binding.Validate()
	}
	return backupvolumetransfer.Binding{}, errs.New(errs.KindStateConflict, "Volume transfer has no sealed step")
}

func (pool *WorkerPool) AcceptBackupVolumeManifestCredit(credit *agentpb.BackupVolumeManifestAckCredit) error {
	if credit == nil || pool.backupVolumes == nil {
		return errs.New(errs.KindValidationFailed, "Volume manifest credit is missing")
	}
	for _, direction := range []agentpb.BackupVolumeManifestDirection{
		agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE,
		agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD,
	} {
		binding, err := pool.volumeBinding(
			credit.TaskId,
			credit.AssignmentId,
			credit.StepId,
			credit.TransferId,
			direction,
		)
		if err != nil || backupvolumetransfer.ValidateCredit(binding, credit) != nil {
			continue
		}
		pool.backupVolumes.mu.Lock()
		slot := pool.backupVolumes.credits[credit.TransferId]
		if slot == nil {
			pool.backupVolumes.mu.Unlock()
			return errs.New(errs.KindStateConflict, "Volume manifest credit slot is unavailable")
		}
		select {
		case slot <- proto.CloneOf(credit):
			pool.backupVolumes.mu.Unlock()
			return nil
		default:
			pool.backupVolumes.mu.Unlock()
			return errs.New(errs.KindStateConflict, "Volume manifest credit exceeds its bounded inbox")
		}
	}
	return errs.New(errs.KindValidationFailed, "Volume manifest credit differs from sealed authority")
}

func (pool *WorkerPool) AcceptBackupVolumeManifestFrame(frame *agentpb.BackupVolumeManifestTransfer) error {
	if frame == nil || pool.backupVolumes == nil || frame.Direction !=
		agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW {
		return errs.New(errs.KindValidationFailed, "Volume Restore manifest frame is missing")
	}
	binding, err := pool.volumeBinding(frame.TaskId, frame.AssignmentId, frame.StepId,
		frame.TransferId, frame.Direction)
	if err != nil {
		return err
	}
	owned, err := backupvolumetransfer.ValidateFrame(binding, frame)
	if err != nil {
		return err
	}
	pool.backupVolumes.mu.Lock()
	defer pool.backupVolumes.mu.Unlock()
	slot := pool.backupVolumes.frames[frame.TransferId]
	if slot == nil {
		return errs.New(errs.KindStateConflict, "Volume Restore manifest frame slot is unavailable")
	}
	select {
	case slot <- owned:
		return nil
	default:
		return errs.New(errs.KindStateConflict, "Volume Restore manifest frame exceeds bounded inbox")
	}
}

func (pool *WorkerPool) publishBackupVolumeCredit(ctx context.Context,
	credit *agentpb.BackupVolumeManifestAckCredit,
) error {
	if ctx == nil || credit == nil {
		return errs.New(errs.KindValidationFailed, "Volume manifest credit is missing")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case pool.outputs <- WorkerOutput{BackupVolumeCredit: proto.CloneOf(credit)}:
		return nil
	}
}

func (pool *WorkerPool) sendBackupVolumeManifest(ctx context.Context, assignment taskassignment.Assignment,
	step *agentpb.BackupStepAuthority, direction agentpb.BackupVolumeManifestDirection,
	role agentpb.BackupVolumeManifestRole, entries []backupvolume.Entry,
	archive *backupvolume.ArtifactEvidence,
) error {
	transferID, err := backupvolumetransfer.TransferID(step.ExecutionId, direction)
	if err != nil {
		return err
	}
	binding, err := pool.volumeBinding(assignment.TaskID, assignment.AssignmentID, step.StepId, transferID, direction)
	if err != nil {
		return err
	}
	pointID := step.GetCapture().GetPointId()
	generationID := ""
	if direction != agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE {
		pointID = step.GetRestore().GetPointId()
		generationID = step.ExecutionId
	}
	frames, err := backupvolumetransfer.BuildFrames(binding, pointID, generationID, role, entries, archive)
	if err != nil {
		return err
	}
	pool.backupVolumes.mu.Lock()
	credits := pool.backupVolumes.credits[transferID]
	pool.backupVolumes.mu.Unlock()
	if credits == nil {
		return errs.New(errs.KindStateConflict, "Volume manifest credit slot is unavailable")
	}
	var credit *agentpb.BackupVolumeManifestAckCredit
	select {
	case <-ctx.Done():
		return ctx.Err()
	case credit = <-credits:
	}
	index, err := frames.ResumeIndex(binding, credit)
	if err != nil {
		return err
	}
	for ; index < len(frames.Frames); index++ {
		frame := frames.Frames[index]
		if err := pool.publishBackupVolumeFrame(ctx, frame); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case credit = <-credits:
		}
		if err := backupvolumetransfer.ValidateCredit(binding, credit); err != nil ||
			credit.CommittedRecordSequence != frame.RecordSequence || credit.NextOrdinal != frames.Next[index] ||
			!bytes.Equal(credit.TransferChainSha256, frames.Chains[index][:]) {
			return errs.New(errs.KindStateConflict, "Volume manifest acknowledgement differs from sent frame")
		}
	}
	return nil
}
