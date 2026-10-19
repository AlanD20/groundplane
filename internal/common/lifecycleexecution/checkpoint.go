package lifecycleexecution

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// The journal retains one bounded progress record, not a growing event stream.
const MaximumProgressBytes = 16 * 1024

func EqualProgress(left, right Progress) bool {
	if left.AttemptID != right.AttemptID || left.Sequence != right.Sequence || left.Inherited != right.Inherited ||
		!left.UpdatedAt.Equal(right.UpdatedAt) || (left.BeganAt == nil) != (right.BeganAt == nil) ||
		len(left.Completed) != len(right.Completed) {
		return false
	}
	if left.BeganAt != nil && !left.BeganAt.Equal(*right.BeganAt) {
		return false
	}
	for index, receipt := range left.Completed {
		other := right.Completed[index]
		if receipt.StageID != other.StageID || receipt.Member != other.Member || receipt.State != other.State ||
			receipt.Evidence != other.Evidence || !receipt.ObservedAt.Equal(other.ObservedAt) {
			return false
		}
	}
	return true
}

// ValidateTransition admits exactly one stage boundary or an identical replay.
// A report cannot skip stages, replace earlier receipts, or clear an in-flight
// effect without supplying the corresponding observation.
func ValidateTransition(
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	previous *Progress,
	next Progress,
) error {
	if next.Validate(attempt, input) != nil {
		return invalid()
	}
	var current Progress
	var err error
	if previous == nil {
		// The assignment owner independently authenticates this prefix against
		// the captured continuation before admitting its first report.
		if next.Inherited > 0 {
			if next.Sequence != 1 || next.BeganAt != nil || len(next.Completed) != next.Inherited {
				return invalid()
			}
			return nil
		}
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
	if current.BeganAt != nil {
		if len(next.Completed) != len(current.Completed)+1 {
			return invalid()
		}
		expected, err = current.Confirm(attempt, input, next.Completed[len(current.Completed)], next.UpdatedAt)
	} else {
		expected, err = current.Begin(attempt, input, next.UpdatedAt)
	}
	if err != nil || !EqualProgress(expected, next) {
		return invalid()
	}
	return nil
}

func (progress Progress) Checkpoint(
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
) (workexecution.Checkpoint, error) {
	if progress.Validate(attempt, input) != nil || progress.Sequence == 0 {
		return workexecution.Checkpoint{}, invalid()
	}
	stages, err := Stages(input)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	return progress.CheckpointStages(attempt, stages)
}

// CheckpointStages hashes journal metadata after validating its bounded stages.
func (progress Progress) CheckpointStages(
	attempt workexecution.Attempt,
	stages []Stage,
) (workexecution.Checkpoint, error) {
	if progress.ValidateStages(attempt, stages) != nil || progress.Sequence == 0 {
		return workexecution.Checkpoint{}, invalid()
	}
	// Completion metadata does not change the identity of the executed attempt.
	attempt.FinishedAt, attempt.Result, attempt.Outcome = nil, nil, ""
	raw, err := json.Marshal(struct {
		Attempt  workexecution.Attempt `json:"attempt"`
		Progress Progress              `json:"progress"`
	}{attempt, progress})
	if err != nil {
		return workexecution.Checkpoint{}, invalid()
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	digest := sha256.Sum256(canonical)
	stageIndex := len(progress.Completed)
	if progress.BeganAt == nil {
		stageIndex--
	}
	return workexecution.Checkpoint{AttemptID: attempt.ID, Sequence: progress.Sequence,
		StageID: stages[stageIndex].ID, RecordedAt: progress.UpdatedAt,
		Evidence: workexecution.Input{ID: attempt.Input.ID, SHA256: hex.EncodeToString(digest[:])}}, nil
}

func EncodeProgress(
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	progress Progress,
) ([]byte, error) {
	if _, err := progress.Checkpoint(attempt, input); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(progress)
	if err != nil || len(raw) > MaximumProgressBytes {
		return nil, invalid()
	}
	return jcs.Canonicalize(raw)
}

func DecodeProgress(
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	raw []byte,
) (Progress, error) {
	if len(raw) == 0 || len(raw) > MaximumProgressBytes {
		return Progress{}, invalid()
	}
	progress, err := jcs.Decode[Progress](raw)
	if err != nil {
		return Progress{}, invalid()
	}
	canonical, err := EncodeProgress(attempt, input, progress)
	if err != nil || !bytes.Equal(canonical, raw) {
		return Progress{}, invalid()
	}
	return progress, nil
}
