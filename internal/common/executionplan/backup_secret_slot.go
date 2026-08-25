package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	MaximumBackupSecretCredentialBytes = uint64(256 << 10)
	MaximumBackupSecretIdentityBytes   = uint64(4 << 10)
	MaximumBackupSecretChunkBytes      = uint64(entrymaterialization.MaximumChunkBytes)
)

// BackupSecretSlotValidator validates one canonical header/chunk/end stream.
// It retains only public framing state and never aliases or stores secret
// content bytes.
type BackupSecretSlotValidator struct {
	taskID       string
	assignmentID string
	stepID       string
	purpose      agentpb.BackupSecretSlotPurpose
	totalBytes   uint64
	chunkCount   uint32
	nextSequence uint32
	received     uint64
	complete     bool
}

// NewBackupSecretSlotValidator validates the assignment-fenced header and
// starts one transient slot stream.
func NewBackupSecretSlotValidator(
	transfer *agentpb.BackupSecretSlotTransfer,
) (*BackupSecretSlotValidator, error) {
	if err := validateBackupSecretSlotIdentity(transfer); err != nil {
		return nil, err
	}
	header := transfer.GetHeader()
	maximum := backupSecretSlotMaximum(transfer.Purpose)
	if header == nil || maximum == 0 || header.TotalBytes == 0 || header.TotalBytes > maximum ||
		header.ChunkCount == 0 || uint64(header.ChunkCount) != backupSecretSlotChunkCount(header.TotalBytes) {
		return nil, errs.New(errs.KindValidationFailed, "backup secret slot header is invalid")
	}
	return &BackupSecretSlotValidator{
		taskID: transfer.TaskId, assignmentID: transfer.AssignmentId, stepID: transfer.StepId,
		purpose: transfer.Purpose, totalBytes: header.TotalBytes, chunkCount: header.ChunkCount, nextSequence: 1,
	}, nil
}

// Accept validates the next chunk or the terminal end record. Callers remain
// responsible for clearing each owned chunk immediately after consumption.
func (validator *BackupSecretSlotValidator) Accept(transfer *agentpb.BackupSecretSlotTransfer) error {
	if validator == nil || validator.complete {
		return errs.New(errs.KindStateConflict, "backup secret slot is not accepting records")
	}
	if err := validateBackupSecretSlotIdentity(transfer); err != nil {
		return err
	}
	if transfer.TaskId != validator.taskID || transfer.AssignmentId != validator.assignmentID ||
		transfer.StepId != validator.stepID || transfer.Purpose != validator.purpose {
		return errs.New(errs.KindStateConflict, "backup secret slot record has a different assignment fence")
	}
	if chunk := transfer.GetChunk(); chunk != nil {
		if chunk.Sequence != validator.nextSequence || chunk.Sequence > validator.chunkCount ||
			len(chunk.Content) == 0 {
			return errs.New(errs.KindValidationFailed, "backup secret slot chunk sequence is invalid")
		}
		remaining := validator.totalBytes - validator.received
		expected := MaximumBackupSecretChunkBytes
		if remaining < expected {
			expected = remaining
		}
		if uint64(len(chunk.Content)) != expected {
			return errs.New(errs.KindValidationFailed, "backup secret slot chunk length is invalid")
		}
		validator.received += uint64(len(chunk.Content))
		validator.nextSequence++
		return nil
	}
	if end := transfer.GetEnd(); end != nil {
		if end.ChunkCount != validator.chunkCount || validator.received != validator.totalBytes ||
			validator.nextSequence != validator.chunkCount+1 {
			return errs.New(errs.KindValidationFailed, "backup secret slot end is invalid")
		}
		validator.complete = true
		return nil
	}
	return errs.New(errs.KindValidationFailed, "backup secret slot record is invalid")
}

// Complete reports whether the exact end record closed the stream.
func (validator *BackupSecretSlotValidator) Complete() bool {
	return validator != nil && validator.complete
}

func validateBackupSecretSlotIdentity(transfer *agentpb.BackupSecretSlotTransfer) error {
	if transfer == nil {
		return errs.New(errs.KindValidationFailed, "backup secret slot transfer is required")
	}
	if err := RejectUnknown(transfer); err != nil {
		return err
	}
	if ids.Validate(ids.KindTask, transfer.TaskId) != nil ||
		ids.Validate(ids.KindAssignment, transfer.AssignmentId) != nil ||
		ids.Validate(ids.KindStep, transfer.StepId) != nil || backupSecretSlotMaximum(transfer.Purpose) == 0 {
		return errs.New(errs.KindValidationFailed, "backup secret slot identity is invalid")
	}
	return nil
}

func backupSecretSlotMaximum(purpose agentpb.BackupSecretSlotPurpose) uint64 {
	switch purpose {
	case agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY:
		return MaximumBackupSecretCredentialBytes
	case agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_OPERATOR_OLD_AGE_IDENTITY:
		return MaximumBackupSecretIdentityBytes
	default:
		return 0
	}
}

func backupSecretSlotChunkCount(total uint64) uint64 {
	return ((total - 1) / MaximumBackupSecretChunkBytes) + 1
}
