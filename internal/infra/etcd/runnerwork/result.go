package runnerwork

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EffectState string

const (
	RuntimeReady   EffectState = "ready"
	RuntimeRemoved EffectState = "removed"
	NoEffects      EffectState = "none"
	EffectsUnknown EffectState = "unknown"
)

// Result is immutable attempt evidence, separate from resource applied state and
// diagnostic text. Unknown effects retain claims even though execution ends.
type Result struct {
	Attempt    workexecution.Attempt                        `json:"attempt"`
	Outcome    workexecution.Outcome                        `json:"outcome"`
	Effects    EffectState                                  `json:"effects"`
	Evidence   *runnerallocation.RunnerRuntimeEvidence      `json:"evidence,omitempty"`
	Cleanup    []runnerallocation.RunnerRuntimeStepEvidence `json:"cleanup,omitempty"`
	Failure    string                                       `json:"failure,omitempty"`
	RecordedAt time.Time                                    `json:"recorded_at"`
}

func ResultKey(attemptID string) string { return "/v1/execution/runner/results/" + attemptID }

func (result Result) Validate(input Input) error {
	ref, err := input.Reference()
	attempt := result.Attempt
	if err != nil || attempt.Validate() != nil || attempt.Executor.Executor != workexecution.Controller ||
		attempt.TaskID != input.TaskID || attempt.WorkItemID != input.WorkItemID || attempt.Input != ref ||
		attempt.FinishedAt != nil || !recordcodec.IsCanonicalUTC(result.RecordedAt) || result.RecordedAt.Before(attempt.AssignedAt) {
		return invalidResult()
	}
	switch result.Outcome {
	case workexecution.Succeeded:
		if result.Failure != "" || attempt.StartedAt == nil {
			return invalidResult()
		}
	case workexecution.Failed, workexecution.Cancelled, workexecution.Superseded:
		switch result.Failure {
		case "registration_token_required",
			"runtime_failed",
			"interrupted",
			"cancelled",
			"deadline",
			"ownership_changed":
		default:
			return invalidResult()
		}
	default:
		return invalidResult()
	}
	switch result.Effects {
	case RuntimeReady:
		if result.Outcome != workexecution.Succeeded || attempt.Direction != workexecution.Forward ||
			input.Operation != runnerallocation.RuntimeOperationCreate || result.Evidence == nil ||
			!result.Evidence.Valid() || result.Evidence.Software.Image != input.Desired.ImageRef || len(result.Cleanup) != 0 {
			return invalidResult()
		}
	case RuntimeRemoved:
		if attempt.StartedAt == nil || result.Evidence != nil ||
			(input.Operation != runnerallocation.RuntimeOperationRemove && attempt.Direction != workexecution.Recovery) {
			return invalidResult()
		}
		steps := runnerallocation.RemovalRuntimeSteps()
		if len(result.Cleanup) != len(steps) {
			return invalidResult()
		}
		for index, evidence := range result.Cleanup {
			if !evidence.Valid(runnerallocation.RuntimeOperationRemove, steps[index]) {
				return invalidResult()
			}
		}
	case NoEffects:
		if result.Outcome == workexecution.Succeeded || result.Evidence != nil || len(result.Cleanup) != 0 ||
			attempt.Direction != workexecution.Forward {
			return invalidResult()
		}
	case EffectsUnknown:
		if result.Outcome == workexecution.Succeeded || result.Evidence != nil || len(result.Cleanup) != 0 {
			return invalidResult()
		}
	default:
		return invalidResult()
	}
	return nil
}

func EncodeResult(input Input, result Result) ([]byte, error) {
	if err := result.Validate(input); err != nil {
		return nil, err
	}
	value, err := recordcodec.Encode("runner_execution_result", result)
	if err != nil {
		return nil, err
	}
	if len(value) > maximumInputBytes {
		return nil, invalidResult()
	}
	return value, nil
}

func DecodeResult(input Input, value []byte) (Result, error) {
	if len(value) > maximumInputBytes {
		return Result{}, invalidResult()
	}
	result, err := recordcodec.Decode[Result](value, "runner_execution_result")
	if err != nil || result.Validate(input) != nil {
		return Result{}, errs.New(errs.KindInternal, "Runner execution result is corrupt")
	}
	return result, nil
}

func (result Result) Completion(input Input) (workexecution.Completion, error) {
	value, err := EncodeResult(input, result)
	if err != nil {
		return workexecution.Completion{}, err
	}
	hash := sha256.Sum256(value)
	return workexecution.Completion{Outcome: result.Outcome, EffectsSettled: result.Effects != EffectsUnknown,
		Evidence: workexecution.Input{
			ID:     ids.DeriveAt(ids.KindPlan, input.CreatedAt, result.Attempt.ID, "runner-result"),
			SHA256: hex.EncodeToString(hash[:]),
		}}, nil
}

func invalidResult() error {
	return errs.New(errs.KindValidationFailed, "Runner result does not prove this attempt's outcome")
}
