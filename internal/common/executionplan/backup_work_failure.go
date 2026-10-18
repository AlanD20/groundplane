package executionplan

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type BackupFailureReason string

const (
	BackupExecutionFailed      BackupFailureReason = "execution_failed"
	BackupDeadlineExpired      BackupFailureReason = "deadline_expired"
	BackupExecutionInterrupted BackupFailureReason = "execution_interrupted"
)

func (reason BackupFailureReason) Valid() bool {
	return reason == BackupExecutionFailed || reason == BackupDeadlineExpired || reason == BackupExecutionInterrupted
}

// BackupWorkFailure retains bounded execution evidence, not provider errors,
// credentials or source snapshots. Resource cleanup is a separate transaction
// participant; this record alone never proves that remote effects were undone.
type BackupWorkFailure struct {
	AttemptID  string                    `json:"attempt_id"`
	Input      workexecution.Input       `json:"input"`
	Reason     BackupFailureReason       `json:"reason"`
	Checkpoint *workexecution.Checkpoint `json:"checkpoint,omitempty"`
	RecordedAt time.Time                 `json:"recorded_at"`
}

func EncodeBackupWorkFailure(attempt workexecution.Attempt, failure BackupWorkFailure) ([]byte, workexecution.Input, error) {
	invalid := func() ([]byte, workexecution.Input, error) {
		return nil, workexecution.Input{}, errs.New(errs.KindValidationFailed, "Backup failure evidence is invalid")
	}
	if attempt.Validate() != nil || failure.AttemptID != attempt.ID ||
		failure.Input != attempt.Input || !failure.Reason.Valid() || failure.RecordedAt.IsZero() ||
		failure.RecordedAt.Location() != time.UTC || failure.RecordedAt.Before(attempt.AssignedAt) ||
		attempt.StartedAt == nil && failure.Reason != BackupExecutionInterrupted ||
		attempt.StartedAt != nil && failure.RecordedAt.Before(*attempt.StartedAt) ||
		failure.Reason == BackupDeadlineExpired && failure.RecordedAt.Before(attempt.Deadline) {
		return invalid()
	}
	if checkpoint := failure.Checkpoint; checkpoint != nil {
		if attempt.StartedAt == nil || checkpoint.AttemptID != attempt.ID || checkpoint.Sequence == 0 ||
			ids.Validate(ids.KindStep, checkpoint.StageID) != nil || checkpoint.Evidence.Validate() != nil ||
			checkpoint.RecordedAt.Before(*attempt.StartedAt) || checkpoint.RecordedAt.After(failure.RecordedAt) {
			return invalid()
		}
	}
	raw, err := json.Marshal(failure)
	if err != nil {
		return invalid()
	}
	raw, err = jcs.Canonicalize(raw)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	at, err := ids.Timestamp(ids.KindAttempt, attempt.ID)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	digest := sha256.Sum256(raw)
	return raw, workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, at, attempt.ID, "backup/failure"), SHA256: hex.EncodeToString(digest[:])}, nil
}

func DecodeBackupWorkFailure(attempt workexecution.Attempt, raw []byte, evidence workexecution.Input) (BackupWorkFailure, error) {
	if len(raw) == 0 || len(raw) > 16384 {
		return BackupWorkFailure{}, errs.New(errs.KindValidationFailed, "Backup failure evidence is missing or oversized")
	}
	failure, err := jcs.Decode[BackupWorkFailure](raw)
	if err != nil {
		return BackupWorkFailure{}, err
	}
	expected, reference, err := EncodeBackupWorkFailure(attempt, failure)
	if err != nil || reference != evidence || !bytes.Equal(raw, expected) {
		return BackupWorkFailure{}, errs.New(errs.KindStateConflict, "Backup failure evidence differs from its captured attempt")
	}
	return failure, nil
}

func DecodeBackupWorkFailureAck(attempt workexecution.Attempt, reason BackupFailureReason, ack *agentpb.BackupWorkFailureAck) (workexecution.Attempt, error) {
	wire, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if ack == nil || RejectUnknown(ack) != nil || !proto.Equal(wire, ack.Attempt) || ack.Evidence == nil ||
		attempt.StartedAt == nil || ack.StartedAt == nil || ack.StartedAt.CheckValid() != nil ||
		!ack.StartedAt.AsTime().Equal(*attempt.StartedAt) || ack.FinishedAt == nil || ack.FinishedAt.CheckValid() != nil {
		return workexecution.Attempt{}, errs.New(errs.KindStateConflict, "Backup failure acknowledgement differs from its started attempt")
	}
	evidence := workexecution.Input{ID: ack.Evidence.InputId, SHA256: hex.EncodeToString(ack.Evidence.Sha256)}
	failure, err := DecodeBackupWorkFailure(attempt, ack.Failure, evidence)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	outcome := workexecution.Outcome(ack.Outcome)
	if failure.Reason != reason || !failure.RecordedAt.Equal(ack.FinishedAt.AsTime()) ||
		(outcome != workexecution.Failed && outcome != workexecution.Cancelled && outcome != workexecution.Superseded) {
		return workexecution.Attempt{}, errs.New(errs.KindStateConflict, "Backup failure acknowledgement has a different result")
	}
	terminal := attempt
	finished := ack.FinishedAt.AsTime().UTC()
	terminal.FinishedAt, terminal.Outcome, terminal.Result = &finished, outcome, &evidence
	return terminal, terminal.Validate()
}
