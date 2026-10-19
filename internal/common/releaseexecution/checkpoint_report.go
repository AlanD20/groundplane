package releaseexecution

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// CheckpointReport retains the last operation cursor and its exact Controller
// receipt. Sequence is the shared attempt sequence, not Progress.Sequence.
type CheckpointReport struct {
	Sequence uint64                    `json:"sequence"`
	StageID  string                    `json:"stage_id"`
	Progress Progress                  `json:"progress"`
	Ack      *workexecution.Checkpoint `json:"ack,omitempty"`
}

func (record CheckpointReport) ValidateEnvelope(attempt workexecution.Attempt) error {
	if attempt.Validate() != nil || attempt.Executor.Executor != workexecution.Agent || attempt.FinishedAt != nil ||
		record.Sequence == 0 || record.Progress.Sequence == 0 || record.Progress.AttemptID != attempt.ID ||
		ids.Validate(ids.KindStep, record.StageID) != nil || record.Progress.UpdatedAt.IsZero() ||
		attempt.StartedAt == nil || record.Progress.UpdatedAt.Before(*attempt.StartedAt) {
		return invalidProgress()
	}
	if record.Ack != nil {
		expected, err := record.Checkpoint(attempt)
		if err != nil || !EqualCheckpoint(expected, *record.Ack) {
			return invalidProgress()
		}
	}
	return nil
}

func (record CheckpointReport) Checkpoint(attempt workexecution.Attempt) (workexecution.Checkpoint, error) {
	// Bind the receipt to the complete report, including immutable assignment.
	record.Ack = nil
	raw, err := json.Marshal(struct {
		Attempt workexecution.Attempt `json:"attempt"`
		Report  CheckpointReport      `json:"report"`
	}{attempt, record})
	if err != nil {
		return workexecution.Checkpoint{}, invalidProgress()
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	digest := sha256.Sum256(canonical)
	return workexecution.Checkpoint{AttemptID: attempt.ID, Sequence: record.Sequence, StageID: record.StageID,
		RecordedAt: record.Progress.UpdatedAt, Evidence: workexecution.Input{ID: attempt.Input.ID, SHA256: hex.EncodeToString(digest[:])}}, nil
}

func EqualCheckpoint(a, b workexecution.Checkpoint) bool {
	return a.AttemptID == b.AttemptID && a.Sequence == b.Sequence && a.StageID == b.StageID &&
		a.Evidence == b.Evidence &&
		a.RecordedAt.Equal(b.RecordedAt)
}
