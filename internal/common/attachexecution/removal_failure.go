package attachexecution

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// RemovalFailure preserves the last Controller-acknowledged command boundary.
// No private output is retained and no claim is made that an unknown command
// stopped or was undone merely because the attempt failed.
type RemovalFailure struct {
	AttemptID  string                    `json:"attempt_id"`
	Input      workexecution.Input       `json:"input"`
	Authority  RemovalAuthority          `json:"authority"`
	Reason     FailureReason             `json:"reason"`
	Checkpoint *workexecution.Checkpoint `json:"checkpoint,omitempty"`
	RecordedAt time.Time                 `json:"recorded_at"`
}

func RemovalFailureID(attemptID string) (string, error) {
	at, err := ids.Timestamp(ids.KindAttempt, attemptID)
	if err != nil {
		return "", err
	}
	return ids.DeriveAt(ids.KindPlan, at, attemptID, "attach-removal/failure"), nil
}

func EncodeRemovalFailure(attempt workexecution.Attempt, failure RemovalFailure) ([]byte, workexecution.Input, error) {
	if failure.Authority.Validate(attempt) != nil || failure.AttemptID != attempt.ID ||
		failure.Input != attempt.Input ||
		!failure.Reason.Valid() ||
		failure.RecordedAt.IsZero() ||
		failure.RecordedAt.Location() != time.UTC ||
		failure.RecordedAt.Before(attempt.AssignedAt) ||
		attempt.StartedAt != nil && failure.RecordedAt.Before(*attempt.StartedAt) ||
		failure.Reason == DeadlineExpired && failure.RecordedAt.Before(attempt.Deadline) {
		return nil, workexecution.Input{}, invalid("Attach removal failure has invalid assignment or timing")
	}
	if cursor := failure.Checkpoint; cursor != nil {
		if attempt.StartedAt == nil || cursor.AttemptID != attempt.ID || cursor.Sequence == 0 ||
			cursor.Sequence >= uint64(failure.Authority.Commands)*2 ||
			ids.Validate(ids.KindStep, cursor.StageID) != nil ||
			cursor.Evidence.Validate() != nil ||
			cursor.RecordedAt.Before(*attempt.StartedAt) ||
			cursor.RecordedAt.After(failure.RecordedAt) {
			return nil, workexecution.Input{}, invalid("Attach removal failure has an invalid command checkpoint")
		}
	}
	raw, err := json.Marshal(failure)
	if err != nil {
		return nil, workexecution.Input{}, invalid("Attach removal failure cannot be encoded")
	}
	raw, err = jcs.Canonicalize(raw)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	id, err := RemovalFailureID(attempt.ID)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	digest := sha256.Sum256(raw)
	return raw, workexecution.Input{ID: id, SHA256: hex.EncodeToString(digest[:])}, nil
}

func DecodeRemovalFailure(
	attempt workexecution.Attempt,
	raw []byte,
	reference workexecution.Input,
) (RemovalFailure, error) {
	if len(raw) == 0 || len(raw) > MaximumRemovalReportBytes {
		return RemovalFailure{}, invalid("Attach removal failure evidence is missing or oversized")
	}
	failure, err := jcs.Decode[RemovalFailure](raw)
	if err != nil {
		return RemovalFailure{}, invalid("Attach removal failure evidence is malformed")
	}
	expected, evidence, err := EncodeRemovalFailure(attempt, failure)
	if err != nil || !bytes.Equal(expected, raw) || evidence != reference {
		return RemovalFailure{}, invalid("Attach removal failure differs from its retained evidence")
	}
	return failure, nil
}
