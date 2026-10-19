package componentexecution

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const MaximumProgressBytes = 4096

func EqualProgress(left, right Progress) bool {
	if left.AttemptID != right.AttemptID || left.Sequence != right.Sequence || left.Stage != right.Stage ||
		left.InFlight != right.InFlight || !left.UpdatedAt.Equal(right.UpdatedAt) ||
		(left.BeganAt == nil) != (right.BeganAt == nil) || (left.Observation == nil) != (right.Observation == nil) {
		return false
	}
	if left.BeganAt != nil && !left.BeganAt.Equal(*right.BeganAt) {
		return false
	}
	if left.Observation != nil {
		a, b := *left.Observation, *right.Observation
		return a.ComponentID == b.ComponentID && a.Input == b.Input && a.Stage == b.Stage &&
			a.ConfigurationWritten == b.ConfigurationWritten && a.RuntimeReconciled == b.RuntimeReconciled &&
			a.ConfigurationActivated == b.ConfigurationActivated && a.RuntimeAbsent == b.RuntimeAbsent &&
			a.RuntimeReady == b.RuntimeReady && a.At.Equal(b.At)
	}
	return true
}

func ValidateTransition(
	attempt workexecution.Attempt,
	input *agentpb.ComponentWorkInput,
	previous *Progress,
	next Progress,
) error {
	if next.Validate(attempt, input) != nil {
		return invalidProgress()
	}
	var current Progress
	var err error
	if previous == nil {
		current, err = NewProgress(attempt, input, *attempt.StartedAt)
	} else {
		current = *previous
		err = current.Validate(attempt, input)
	}
	if err != nil {
		return err
	}
	if previous != nil && EqualProgress(current, next) {
		return nil
	}
	var expected Progress
	if current.InFlight {
		if next.Observation == nil {
			return invalidProgress()
		}
		expected, err = current.Confirm(attempt, input, *next.Observation, next.UpdatedAt)
	} else {
		expected, err = current.Begin(attempt, input, next.UpdatedAt)
	}
	if err != nil || !EqualProgress(expected, next) {
		return invalidProgress()
	}
	return nil
}

func (progress Progress) Checkpoint(
	attempt workexecution.Attempt,
	input *agentpb.ComponentWorkInput,
) (workexecution.Checkpoint, error) {
	layout, err := CaptureLayout(attempt, input)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	return progress.LayoutCheckpoint(attempt, layout)
}

func (progress Progress) LayoutCheckpoint(
	attempt workexecution.Attempt,
	layout Layout,
) (workexecution.Checkpoint, error) {
	if progress.ValidateLayout(attempt, layout) != nil || progress.Sequence == 0 {
		return workexecution.Checkpoint{}, invalidProgress()
	}
	at, err := ids.Timestamp(ids.KindWorkItem, attempt.WorkItemID)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	stage := layout.stages()[(progress.Sequence-1)/2]
	stageID := ids.DeriveAt(ids.KindStep, at, attempt.WorkItemID, "component/"+string(stage))
	attempt.FinishedAt, attempt.Result, attempt.Outcome = nil, nil, ""
	raw, err := json.Marshal(struct {
		Attempt  workexecution.Attempt `json:"attempt"`
		Progress Progress              `json:"progress"`
	}{attempt, progress})
	if err != nil {
		return workexecution.Checkpoint{}, invalidProgress()
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	digest := sha256.Sum256(canonical)
	return workexecution.Checkpoint{AttemptID: attempt.ID, Sequence: progress.Sequence, StageID: stageID,
		RecordedAt: progress.UpdatedAt, Evidence: workexecution.Input{ID: attempt.Input.ID, SHA256: hex.EncodeToString(digest[:])}}, nil
}

func EncodeProgress(
	attempt workexecution.Attempt,
	input *agentpb.ComponentWorkInput,
	progress Progress,
) ([]byte, error) {
	if _, err := progress.Checkpoint(attempt, input); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(progress)
	if err != nil || len(raw) > MaximumProgressBytes {
		return nil, invalidProgress()
	}
	return jcs.Canonicalize(raw)
}

func DecodeProgress(attempt workexecution.Attempt, input *agentpb.ComponentWorkInput, raw []byte) (Progress, error) {
	if len(raw) == 0 || len(raw) > MaximumProgressBytes {
		return Progress{}, invalidProgress()
	}
	progress, err := jcs.Decode[Progress](raw)
	if err != nil {
		return Progress{}, invalidProgress()
	}
	canonical, err := EncodeProgress(attempt, input, progress)
	if err != nil || !bytes.Equal(canonical, raw) {
		return Progress{}, invalidProgress()
	}
	return progress, nil
}
