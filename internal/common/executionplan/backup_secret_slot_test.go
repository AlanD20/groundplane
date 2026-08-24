package executionplan

import (
	"bytes"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	testBackupTaskID       = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupAssignmentID = "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupStepID       = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

// Rationale: one header admits exactly one bounded, contiguous frame sequence
// and the validator retains no secret content after accepting a chunk.
func TestBackupSecretSlotValidatorAcceptsCanonicalFrames(t *testing.T) {
	total := MaximumBackupSecretChunkBytes + 3
	validator, err := NewBackupSecretSlotValidator(backupSecretHeader(
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
		total,
		2,
	))
	if err != nil {
		t.Fatalf("NewBackupSecretSlotValidator() error = %v", err)
	}
	first := bytes.Repeat([]byte{1}, int(MaximumBackupSecretChunkBytes))
	if err := validator.Accept(backupSecretChunk(1, first)); err != nil {
		t.Fatalf("Accept(first) error = %v", err)
	}
	clear(first)
	if err := validator.Accept(backupSecretChunk(2, []byte("key"))); err != nil {
		t.Fatalf("Accept(final) error = %v", err)
	}
	if err := validator.Accept(backupSecretEnd(2)); err != nil {
		t.Fatalf("Accept(end) error = %v", err)
	}
	if !validator.Complete() {
		t.Fatal("Complete() = false")
	}
}

// Rationale: purpose-specific total ceilings are part of the private machine
// contract and cannot be widened by a header claim.
func TestBackupSecretSlotValidatorEnforcesPurposeBounds(t *testing.T) {
	for _, test := range []struct {
		name    string
		purpose agentpb.BackupSecretSlotPurpose
		total   uint64
	}{
		{
			name: "credential", purpose: agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY,
			total: MaximumBackupSecretCredentialBytes + 1,
		},
		{
			name:    "identity",
			purpose: agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_OPERATOR_OLD_AGE_IDENTITY,
			total:   MaximumBackupSecretIdentityBytes + 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			chunks := uint32(backupSecretSlotChunkCount(test.total))
			if _, err := NewBackupSecretSlotValidator(
				backupSecretHeader(test.purpose, test.total, chunks),
			); err == nil {
				t.Fatal("NewBackupSecretSlotValidator(over bound) error = nil")
			}
		})
	}
}

// Rationale: a stale assignment, non-contiguous chunk, short non-final chunk,
// or premature end cannot populate or release an active secret slot.
func TestBackupSecretSlotValidatorRejectsAdversarialFrames(t *testing.T) {
	newValidator := func(t *testing.T) *BackupSecretSlotValidator {
		t.Helper()
		validator, err := NewBackupSecretSlotValidator(backupSecretHeader(
			agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
			MaximumBackupSecretChunkBytes+1,
			2,
		))
		if err != nil {
			t.Fatalf("NewBackupSecretSlotValidator() error = %v", err)
		}
		return validator
	}
	t.Run("stale assignment", func(t *testing.T) {
		validator := newValidator(t)
		chunk := backupSecretChunk(1, bytes.Repeat([]byte{1}, int(MaximumBackupSecretChunkBytes)))
		chunk.AssignmentId = "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		if err := validator.Accept(chunk); err == nil {
			t.Fatal("Accept(stale assignment) error = nil")
		}
	})
	t.Run("out of order", func(t *testing.T) {
		validator := newValidator(t)
		if err := validator.Accept(backupSecretChunk(2, []byte{1})); err == nil {
			t.Fatal("Accept(out of order) error = nil")
		}
	})
	t.Run("short non-final", func(t *testing.T) {
		validator := newValidator(t)
		if err := validator.Accept(backupSecretChunk(1, []byte{1})); err == nil {
			t.Fatal("Accept(short non-final) error = nil")
		}
	})
	t.Run("premature end", func(t *testing.T) {
		validator := newValidator(t)
		if err := validator.Accept(backupSecretEnd(2)); err == nil {
			t.Fatal("Accept(premature end) error = nil")
		}
	})
}

func backupSecretHeader(
	purpose agentpb.BackupSecretSlotPurpose,
	total uint64,
	chunks uint32,
) *agentpb.BackupSecretSlotTransfer {
	return &agentpb.BackupSecretSlotTransfer{
		TaskId: testBackupTaskID, AssignmentId: testBackupAssignmentID, StepId: testBackupStepID,
		Purpose: purpose,
		Record: &agentpb.BackupSecretSlotTransfer_Header{Header: &agentpb.BackupSecretSlotHeader{
			TotalBytes: total, ChunkCount: chunks,
		}},
	}
}

func backupSecretChunk(sequence uint32, content []byte) *agentpb.BackupSecretSlotTransfer {
	return &agentpb.BackupSecretSlotTransfer{
		TaskId: testBackupTaskID, AssignmentId: testBackupAssignmentID, StepId: testBackupStepID,
		Purpose: agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
		Record: &agentpb.BackupSecretSlotTransfer_Chunk{Chunk: &agentpb.BackupSecretSlotChunk{
			Sequence: sequence, Content: content,
		}},
	}
}

func backupSecretEnd(chunks uint32) *agentpb.BackupSecretSlotTransfer {
	return &agentpb.BackupSecretSlotTransfer{
		TaskId: testBackupTaskID, AssignmentId: testBackupAssignmentID, StepId: testBackupStepID,
		Purpose: agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
		Record:  &agentpb.BackupSecretSlotTransfer_End{End: &agentpb.BackupSecretSlotEnd{ChunkCount: chunks}},
	}
}
