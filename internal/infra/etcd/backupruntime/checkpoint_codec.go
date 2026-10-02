package backupruntime

import (
	"encoding/hex"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"
)

func EncodeBackupCheckpointCursorRecord(record BackupCheckpointCursorRecord) ([]byte, error) {
	return EncodeBackupRuntimeRecord(
		"backup-checkpoint-cursor",
		record,
		validateBackupCheckpointCursorRecord,
	)
}

func DecodeBackupCheckpointCursorRecord(value []byte) (BackupCheckpointCursorRecord, error) {
	return DecodeBackupRuntimeRecord(
		value,
		"backup-checkpoint-cursor",
		validateBackupCheckpointCursorRecord,
	)
}

func validateBackupCheckpointCursorRecord(record BackupCheckpointCursorRecord) error {
	if recordcodec.ValidateID(ids.KindTask, record.TaskID) != nil ||
		recordcodec.ValidateID(ids.KindAssignment, record.AssignmentID) != nil ||
		recordcodec.ValidateID(ids.KindStep, record.StepID) != nil || record.NextSequence < 2 {
		return errs.New(errs.KindValidationFailed, "backup checkpoint cursor is invalid")
	}
	if !validBackupCheckpointExecution(record.ExecutionID) || record.AssignmentGeneration == 0 ||
		!recordcodec.ValidSHA256(record.AuthoritySHA256) {
		return errs.New(errs.KindValidationFailed, "backup checkpoint cursor authority is invalid")
	}
	return nil
}

func EncodeBackupCheckpointDedupRecord(record BackupCheckpointDedupRecord) ([]byte, error) {
	return EncodeBackupRuntimeRecord(
		"backup-checkpoint-dedup",
		record,
		validateBackupCheckpointDedupRecord,
	)
}

func DecodeBackupCheckpointDedupRecord(value []byte) (BackupCheckpointDedupRecord, error) {
	return DecodeBackupRuntimeRecord(
		value,
		"backup-checkpoint-dedup",
		validateBackupCheckpointDedupRecord,
	)
}

func validateBackupCheckpointDedupRecord(record BackupCheckpointDedupRecord) error {
	if recordcodec.ValidateID(ids.KindTask, record.TaskID) != nil ||
		recordcodec.ValidateID(ids.KindAssignment, record.AssignmentID) != nil ||
		recordcodec.ValidateID(ids.KindStep, record.StepID) != nil || record.Sequence == 0 ||
		record.CheckpointTag < 20 || record.CheckpointTag > 32 || !recordcodec.ValidSHA256(record.PayloadSHA256) {
		return errs.New(
			errs.KindValidationFailed,
			"backup checkpoint deduplication record is invalid",
		)
	}
	if !validBackupCheckpointExecution(record.ExecutionID) || record.AssignmentGeneration == 0 ||
		!recordcodec.ValidSHA256(record.AuthoritySHA256) || record.PrecedingCheckpointRevision < 0 ||
		(record.Sequence == 1 && record.PrecedingCheckpointRevision != 0) ||
		(record.Sequence > 1 && record.PrecedingCheckpointRevision == 0) {
		return errs.New(errs.KindValidationFailed, "backup checkpoint deduplication authority is invalid")
	}
	if len(record.Request) == 0 || len(record.Request) > executionplan.MaximumPlanBytes {
		return errs.New(errs.KindValidationFailed, "backup checkpoint request evidence is invalid")
	}
	request := &agentpb.BackupCheckpointRequest{}
	if err := proto.Unmarshal(record.Request, request); err != nil {
		return errs.New(errs.KindValidationFailed, "backup checkpoint request evidence is invalid")
	}
	input := BackupCheckpointInput{TaskID: record.TaskID, AssignmentID: record.AssignmentID, StepID: record.StepID,
		ExecutionID: record.ExecutionID, Sequence: record.Sequence, AssignmentGeneration: record.AssignmentGeneration,
		AuthoritySHA256: record.AuthoritySHA256, PrecedingCheckpointRevision: record.PrecedingCheckpointRevision, Request: request}
	digest, err := BackupCheckpointDigest(input)
	if err != nil || digest != record.PayloadSHA256 || BackupCheckpointTag(request) != record.CheckpointTag ||
		hex.EncodeToString(request.AuthorityDigest) != record.AuthoritySHA256 {
		return errs.New(errs.KindValidationFailed, "backup checkpoint retained request does not match its receipt")
	}
	return nil
}

func validBackupCheckpointExecution(value string) bool {
	if len(value) != 26 || value != strings.ToUpper(value) {
		return false
	}
	_, err := ulid.ParseStrict(value)
	return err == nil
}
