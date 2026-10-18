package executionplan

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BackupWorkSecretValidator holds framing metadata only. The receiver owns and
// clears secret bytes; no credential becomes a checkpoint or journal payload.
type BackupWorkSecretValidator struct {
	attempt  *agentpb.WorkAttemptIdentity
	ordinal  uint32
	purpose  agentpb.BackupSecretSlotPurpose
	total    uint64
	received uint64
	chunks   uint32
	next     uint32
	complete bool
}

func NewBackupWorkSecretValidator(transfer *agentpb.BackupWorkSecretTransfer,
	input *agentpb.BackupWorkInput, identity workexecution.Identity,
) (*BackupWorkSecretValidator, error) {
	if transfer == nil || RejectUnknown(transfer) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Backup credential header is invalid")
	}
	attempt, err := workexecution.DecodeAgentAttempt(transfer.Attempt, identity)
	if err != nil {
		return nil, err
	}
	owned, err := openBackupCredentialInput(attempt, input)
	if err != nil {
		return nil, err
	}
	if uint64(transfer.SourceOrdinal) >= uint64(len(owned.Steps)) {
		return nil, errs.New(errs.KindStateConflict, "Backup credential source is not captured")
	}
	purposes, err := backupsecret.SourcePurposes(owned.Steps[transfer.SourceOrdinal])
	if err != nil {
		return nil, err
	}
	header := transfer.GetHeader()
	maximum := backupSecretSlotMaximum(transfer.Purpose)
	if !slices.Contains(purposes, transfer.Purpose) || maximum == 0 || header == nil ||
		header.TotalBytes == 0 || header.TotalBytes > maximum ||
		uint64(header.ChunkCount) != backupSecretSlotChunkCount(header.TotalBytes) {
		return nil, errs.New(errs.KindValidationFailed, "Backup credential size or purpose is invalid")
	}
	return &BackupWorkSecretValidator{attempt: proto.CloneOf(transfer.Attempt),
		ordinal: transfer.SourceOrdinal, purpose: transfer.Purpose,
		total: header.TotalBytes, chunks: header.ChunkCount, next: 1}, nil
}

func openBackupCredentialInput(attempt workexecution.Attempt, input *agentpb.BackupWorkInput) (*agentpb.BackupWorkInput, error) {
	if attempt.Direction == workexecution.Forward {
		return OpenBackupWorkInput(attempt, input)
	}
	if attempt.Validate() != nil || attempt.Direction != workexecution.Recovery || attempt.FinishedAt != nil {
		return nil, errs.New(errs.KindStateConflict, "Backup credential recovery attempt is invalid")
	}
	owned, raw, reference, err := CaptureBackupWorkInput(attempt.Input.ID, input)
	defer clear(raw)
	if err != nil {
		return nil, err
	}
	if reference != attempt.Input || owned.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE {
		return nil, errs.New(errs.KindStateConflict, "Backup recovery credentials differ from captured Restore input")
	}
	return owned, nil
}

func (validator *BackupWorkSecretValidator) Accept(transfer *agentpb.BackupWorkSecretTransfer) error {
	if validator == nil || validator.complete {
		return errs.New(errs.KindStateConflict, "Backup credential stream is closed")
	}
	if transfer == nil || RejectUnknown(transfer) != nil ||
		!proto.Equal(transfer.Attempt, validator.attempt) || transfer.SourceOrdinal != validator.ordinal ||
		transfer.Purpose != validator.purpose {
		return errs.New(errs.KindStateConflict, "Backup credential frame belongs to another attempt or source")
	}
	if chunk := transfer.GetChunk(); chunk != nil {
		expected := min(MaximumBackupSecretChunkBytes, validator.total-validator.received)
		if chunk.Sequence != validator.next || chunk.Sequence > validator.chunks ||
			expected == 0 || uint64(len(chunk.Content)) != expected {
			return errs.New(errs.KindValidationFailed, "Backup credential chunk sequence or size is invalid")
		}
		validator.received += expected
		validator.next++
		return nil
	}
	if end := transfer.GetEnd(); end != nil && end.ChunkCount == validator.chunks &&
		validator.received == validator.total && validator.next == validator.chunks+1 {
		validator.complete = true
		return nil
	}
	return errs.New(errs.KindValidationFailed, "Backup credential stream ended early or contains an unexpected frame")
}

func (validator *BackupWorkSecretValidator) Complete() bool {
	return validator != nil && validator.complete
}
