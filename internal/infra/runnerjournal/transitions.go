package runnerjournal

import (
	"encoding/json"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
)

func validLifecycleState(progress runnerallocation.RunnerRuntimeProgress,
	steps []runnerallocation.RunnerRuntimeStep,
) bool {
	if progress.Operation == runnerallocation.RuntimeOperationCreate {
		if len(progress.CleanupReceipts) != 0 {
			return false
		}
		if progress.NextStep == len(steps) {
			if progress.Evidence == nil || !progress.Evidence.Valid() {
				return false
			}
		} else if progress.Evidence != nil {
			return false
		}
	} else {
		if progress.Evidence != nil || len(progress.CleanupReceipts) != progress.NextStep {
			return false
		}
		for index, evidence := range progress.CleanupReceipts {
			if !evidence.Valid(progress.Operation, steps[index]) {
				return false
			}
		}
	}
	switch progress.Status {
	case runnerallocation.RuntimeStatusRunning, runnerallocation.RuntimeStatusFailed:
		return true
	case runnerallocation.RuntimeStatusReady:
		return progress.Operation == runnerallocation.RuntimeOperationCreate &&
			progress.NextStep == len(steps) && progress.ActiveStep == nil
	case runnerallocation.RuntimeStatusRemoved:
		return progress.Operation == runnerallocation.RuntimeOperationRemove &&
			progress.NextStep == len(steps) && progress.ActiveStep == nil
	default:
		return false
	}
}

// A valid hash chain is not sufficient authority: each revision must be exactly
// one allowed transition from its committed predecessor, with sealed inputs and
// already-recorded observations unchanged.
func validTransition(previous, next runnerallocation.RunnerRuntimeProgress, exists bool) bool {
	if !exists {
		return next.Revision == 0 && next.Status == runnerallocation.RuntimeStatusRunning &&
			next.NextStep == 0 && next.ActiveStep == nil && next.Evidence == nil && len(next.CleanupReceipts) == 0
	}
	if previous.Status != runnerallocation.RuntimeStatusRunning || next.Revision != previous.Revision+1 {
		return false
	}
	expected := previous
	expected.Revision = next.Revision
	expected.PredecessorSHA256 = next.PredecessorSHA256
	switch {
	case next.Status == runnerallocation.RuntimeStatusFailed,
		next.Status == runnerallocation.RuntimeStatusReady,
		next.Status == runnerallocation.RuntimeStatusRemoved:
		expected.Status = next.Status
	case previous.ActiveStep == nil && next.ActiveStep != nil:
		expected.ActiveStep = next.ActiveStep
	case previous.ActiveStep != nil && next.ActiveStep == nil && next.NextStep == previous.NextStep+1:
		expected.NextStep++
		expected.ActiveStep = nil
		if previous.Operation == runnerallocation.RuntimeOperationRemove {
			if len(next.CleanupReceipts) != len(previous.CleanupReceipts)+1 {
				return false
			}
			expected.CleanupReceipts = append(
				slices.Clone(previous.CleanupReceipts),
				next.CleanupReceipts[previous.NextStep],
			)
		} else if *previous.ActiveStep == runnerallocation.StepStartRunner {
			expected.Evidence = next.Evidence
		}
	default:
		return false
	}
	// Typed records contain no secret bytes. This is equality of canonical
	// persistence encodings, not a conversion between models.
	expectedBytes, err := json.Marshal(expected)
	if err != nil {
		return false
	}
	nextBytes, err := json.Marshal(next)
	return err == nil && string(expectedBytes) == string(nextBytes)
}
