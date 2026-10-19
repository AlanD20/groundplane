package backingprovision

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

const MaximumProgressBytes = 8192

func EqualProgress(left, right Progress) bool {
	if left.AttemptID != right.AttemptID || left.Sequence != right.Sequence || left.Stage != right.Stage ||
		left.InFlight != right.InFlight ||
		!left.UpdatedAt.Equal(right.UpdatedAt) ||
		(left.BeganAt == nil) != (right.BeganAt == nil) ||
		(left.Last == nil) != (right.Last == nil) ||
		left.BeganAt != nil && !left.BeganAt.Equal(*right.BeganAt) {
		return false
	}
	return left.Last == nil || *left.Last == *right.Last
}

func ValidateTransition(attempt workexecution.Attempt, layout Layout, previous *Progress, next Progress) error {
	if next.Validate(attempt, layout) != nil {
		return invalid("backing creation checkpoint is invalid")
	}
	var current Progress
	var err error
	if previous == nil {
		current, err = NewProgress(attempt, layout, *attempt.StartedAt)
	} else {
		current = *previous
		err = current.Validate(attempt, layout)
	}
	if err != nil {
		return err
	}
	if previous != nil && EqualProgress(current, next) {
		return nil
	}
	var expected Progress
	if current.InFlight {
		if next.Last == nil {
			return invalid("backing creation confirmation has no observation")
		}
		expected, err = current.Confirm(attempt, layout, *next.Last, next.UpdatedAt)
	} else {
		expected, err = current.Begin(attempt, layout, next.UpdatedAt)
	}
	if err != nil || !EqualProgress(expected, next) {
		return invalid("backing creation checkpoint skipped or rewrote a stage")
	}
	return nil
}

func (progress Progress) Checkpoint(attempt workexecution.Attempt, layout Layout) (workexecution.Checkpoint, error) {
	if progress.Validate(attempt, layout) != nil || progress.Sequence == 0 {
		return workexecution.Checkpoint{}, invalid("backing creation has no acknowledged stage boundary")
	}
	stage := layout.stages()[(progress.Sequence-1)/2]
	stageID, err := StageID(attempt.WorkItemID, stage)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	// Terminal receipt fields are not part of the earlier active checkpoint.
	attempt.FinishedAt, attempt.Result, attempt.Outcome = nil, nil, ""
	raw, err := json.Marshal(struct {
		Attempt  workexecution.Attempt `json:"attempt"`
		Layout   Layout                `json:"layout"`
		Progress Progress              `json:"progress"`
	}{attempt, layout, progress})
	if err != nil {
		return workexecution.Checkpoint{}, invalid("backing creation checkpoint cannot be encoded")
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	digest := sha256.Sum256(canonical)
	return workexecution.Checkpoint{AttemptID: attempt.ID, Sequence: progress.Sequence, StageID: stageID,
		RecordedAt: progress.UpdatedAt, Evidence: workexecution.Input{ID: attempt.Input.ID, SHA256: hex.EncodeToString(digest[:])}}, nil
}

func EncodeProgress(attempt workexecution.Attempt, layout Layout, progress Progress) ([]byte, error) {
	if _, err := progress.Checkpoint(attempt, layout); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(progress)
	if err != nil || len(raw) > MaximumProgressBytes {
		return nil, invalid("backing creation progress exceeds its bounded format")
	}
	return jcs.Canonicalize(raw)
}

func DecodeProgress(attempt workexecution.Attempt, layout Layout, raw []byte) (Progress, error) {
	if len(raw) == 0 || len(raw) > MaximumProgressBytes {
		return Progress{}, invalid("backing creation progress is missing or oversized")
	}
	progress, err := jcs.Decode[Progress](raw)
	if err != nil {
		return Progress{}, invalid("backing creation progress cannot be decoded")
	}
	canonical, err := EncodeProgress(attempt, layout, progress)
	if err != nil || !bytes.Equal(canonical, raw) {
		return Progress{}, invalid("backing creation progress is not its canonical checkpoint")
	}
	return progress, nil
}
