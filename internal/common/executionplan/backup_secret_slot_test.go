package executionplan

import (
	"bytes"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: one captured credential header admits exactly one bounded,
// contiguous stream. The receiver may clear accepted content immediately.
func TestBackupWorkSecretValidatorAcceptsCanonicalFrames(t *testing.T) {
	header, input, identity := nativeBackupSecretHeader(
		t,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
		MaximumBackupSecretChunkBytes+3,
	)
	validator, err := NewBackupWorkSecretValidator(header, input, identity)
	if err != nil {
		t.Fatal(err)
	}
	first := bytes.Repeat([]byte{1}, int(MaximumBackupSecretChunkBytes))
	if err := validator.Accept(nativeBackupSecretChunk(header, 1, first)); err != nil {
		t.Fatal(err)
	}
	clear(first)
	if err := validator.Accept(nativeBackupSecretChunk(header, 2, []byte("key"))); err != nil {
		t.Fatal(err)
	}
	if err := validator.Accept(nativeBackupSecretEnd(header)); err != nil {
		t.Fatal(err)
	}
	if !validator.Complete() {
		t.Fatal("complete stream not acknowledged")
	}
	if err := validator.Accept(nativeBackupSecretEnd(header)); err == nil {
		t.Fatal("completed stream accepted another frame")
	}
}

// Rationale: purpose ceilings and captured source membership prevent a stream
// from loading oversized credentials or a key not authorized by this input.
func TestBackupWorkSecretValidatorEnforcesPurposeAndBounds(t *testing.T) {
	tests := []struct {
		name    string
		purpose agentpb.BackupSecretSlotPurpose
		total   uint64
	}{
		{
			"credential",
			agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY,
			MaximumBackupSecretCredentialBytes + 1,
		},
		{
			"identity",
			agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY,
			MaximumBackupSecretIdentityBytes + 1,
		},
		{
			"uncaptured old identity",
			agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_OPERATOR_OLD_AGE_IDENTITY,
			1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			header, input, identity := nativeBackupSecretHeader(t, test.purpose, test.total)
			if _, err := NewBackupWorkSecretValidator(header, input, identity); err == nil {
				t.Fatal("invalid purpose or size accepted")
			}
		})
	}
}

// Rationale: a different attempt/source/purpose, reordered or short chunk, or
// early end cannot populate or release the current credential slot.
func TestBackupWorkSecretValidatorRejectsAdversarialFrames(t *testing.T) {
	tests := []struct {
		name  string
		frame func(*agentpb.BackupWorkSecretTransfer) *agentpb.BackupWorkSecretTransfer
	}{
		{"stale attempt", func(h *agentpb.BackupWorkSecretTransfer) *agentpb.BackupWorkSecretTransfer {
			r := nativeBackupSecretChunk(h, 1, bytes.Repeat([]byte{1}, int(MaximumBackupSecretChunkBytes)))
			r.Attempt.AttemptId = ids.New(ids.KindAttempt)
			return r
		}},
		{"foreign source", func(h *agentpb.BackupWorkSecretTransfer) *agentpb.BackupWorkSecretTransfer {
			r := nativeBackupSecretChunk(h, 1, bytes.Repeat([]byte{1}, int(MaximumBackupSecretChunkBytes)))
			r.SourceOrdinal++
			return r
		}},
		{"different purpose", func(h *agentpb.BackupWorkSecretTransfer) *agentpb.BackupWorkSecretTransfer {
			r := nativeBackupSecretChunk(h, 1, bytes.Repeat([]byte{1}, int(MaximumBackupSecretChunkBytes)))
			r.Purpose = agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY
			return r
		}},
		{"out of order", func(h *agentpb.BackupWorkSecretTransfer) *agentpb.BackupWorkSecretTransfer {
			return nativeBackupSecretChunk(h, 2, []byte{1})
		}},
		{"short nonfinal", func(h *agentpb.BackupWorkSecretTransfer) *agentpb.BackupWorkSecretTransfer {
			return nativeBackupSecretChunk(h, 1, []byte{1})
		}},
		{"premature end", nativeBackupSecretEnd},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			header, input, identity := nativeBackupSecretHeader(
				t,
				agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
				MaximumBackupSecretChunkBytes+1,
			)
			validator, err := NewBackupWorkSecretValidator(header, input, identity)
			if err != nil {
				t.Fatal(err)
			}
			if err := validator.Accept(test.frame(header)); err == nil {
				t.Fatal("adversarial frame accepted")
			}
			if validator.Complete() {
				t.Fatal("invalid stream completed")
			}
		})
	}
}

func nativeBackupSecretHeader(
	t *testing.T,
	purpose agentpb.BackupSecretSlotPurpose,
	total uint64,
) (*agentpb.BackupWorkSecretTransfer, *agentpb.BackupWorkInput, workexecution.Identity) {
	t.Helper()
	input, attempt := nativeBackupFixture(t)
	wire, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		t.Fatal(err)
	}
	return &agentpb.BackupWorkSecretTransfer{Attempt: wire, Purpose: purpose,
		Record: &agentpb.BackupWorkSecretTransfer_Header{
			Header: &agentpb.BackupSecretSlotHeader{
				TotalBytes: total,
				ChunkCount: uint32(backupSecretSlotChunkCount(total)),
			},
		}}, input, attempt.Executor
}

func nativeBackupSecretChunk(
	header *agentpb.BackupWorkSecretTransfer,
	sequence uint32,
	content []byte,
) *agentpb.BackupWorkSecretTransfer {
	return &agentpb.BackupWorkSecretTransfer{
		Attempt:       proto.CloneOf(header.Attempt),
		SourceOrdinal: header.SourceOrdinal,
		Purpose:       header.Purpose,
		Record: &agentpb.BackupWorkSecretTransfer_Chunk{
			Chunk: &agentpb.BackupSecretSlotChunk{Sequence: sequence, Content: content},
		},
	}
}

func nativeBackupSecretEnd(header *agentpb.BackupWorkSecretTransfer) *agentpb.BackupWorkSecretTransfer {
	return &agentpb.BackupWorkSecretTransfer{
		Attempt:       proto.CloneOf(header.Attempt),
		SourceOrdinal: header.SourceOrdinal,
		Purpose:       header.Purpose,
		Record: &agentpb.BackupWorkSecretTransfer_End{
			End: &agentpb.BackupSecretSlotEnd{ChunkCount: header.GetHeader().ChunkCount},
		},
	}
}
