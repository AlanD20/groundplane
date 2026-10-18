package releaseexecution

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// RecoveryInput binds the shared restoration procedure to a captured cause:
// failed-attempt cleanup or a separate Group switch-back operation. Cleanup
// retains the original failure; Group restoration retains the source member's
// actual success and names the later failure that requires its restoration.
type RecoveryInput struct {
	Input   Input
	Forward workexecution.Attempt
	Failure Failure
	Group   *GroupRestorationAuthority
}

func (input RecoveryInput) Validate() error {
	if err := input.ValidateIdentity(); err != nil {
		return err
	}
	_, err := executionplan.OpenReleaseWorkArtifacts(input.Input.Attempt, input.Input.Release)
	return err
}

// ValidateIdentity checks the compact journal binding without requiring a copy
// of configuration/artifact bytes. Effect execution also requires Validate.
func (input RecoveryInput) ValidateIdentity() error {
	recovery, forward := input.Input.Attempt, input.Forward
	if input.Group != nil {
		if input.Failure != (Failure{}) {
			return invalidProgress()
		}
		return input.Group.ValidateFor(recovery, forward)
	}
	if recovery.Validate() != nil || forward.Validate() != nil || recovery.Direction != workexecution.Recovery ||
		forward.Direction != workexecution.Forward || recovery.FinishedAt != nil || forward.FinishedAt == nil || forward.Result == nil ||
		forward.Outcome == workexecution.Succeeded || recovery.TaskID != forward.TaskID || recovery.WorkItemID != forward.WorkItemID ||
		recovery.Number <= forward.Number || recovery.Input != forward.Input || recovery.Executor.Executor != workexecution.Agent ||
		forward.Executor.Executor != workexecution.Agent || recovery.Executor.HostID != forward.Executor.HostID ||
		recovery.AssignedAt.Before(*forward.FinishedAt) || !input.Failure.RecordedAt.Equal(*forward.FinishedAt) {
		return invalidProgress()
	}
	_, reference, err := EncodeFailure(forward, input.Failure)
	if err != nil || reference != *forward.Result {
		return invalidProgress()
	}
	return nil
}

type RecoveryPhase string

const (
	QuiesceForward        RecoveryPhase = "quiesce_forward"
	InspectFailedRuntime  RecoveryPhase = "inspect_failed_runtime"
	RestoreConfiguration  RecoveryPhase = "restore_configuration"
	RestoreServingRuntime RecoveryPhase = "restore_serving_runtime"
	RemoveFailedCandidate RecoveryPhase = "remove_failed_candidate"
	VerifyRestoration     RecoveryPhase = "verify_restoration"
	RecoveryComplete      RecoveryPhase = "complete"
)

// RecoveryProgress is separate from forward progress and never runs deployment
// hooks. Quiescence must be proved before any restorative write; merely replacing
// an Agent generation or observing a timeout is not that proof.
type RecoveryProgress struct {
	AttemptID string               `json:"attempt_id"`
	Phase     RecoveryPhase        `json:"phase"`
	Sequence  uint64               `json:"sequence"`
	InFlight  bool                 `json:"in_flight"`
	UpdatedAt time.Time            `json:"updated_at"`
	Evidence  *workexecution.Input `json:"evidence,omitempty"`
}

func NewRecoveryProgress(input RecoveryInput, at time.Time) (RecoveryProgress, error) {
	if input.Validate() != nil || input.Input.Attempt.StartedAt == nil || at.Before(*input.Input.Attempt.StartedAt) || !at.Before(input.Input.Attempt.Deadline) {
		return RecoveryProgress{}, invalidProgress()
	}
	return RecoveryProgress{AttemptID: input.Input.Attempt.ID, Phase: QuiesceForward, UpdatedAt: at.UTC()}, nil
}

func recoveryPhaseIndex(phase RecoveryPhase) (uint64, bool) {
	switch phase {
	case QuiesceForward:
		return 0, true
	case InspectFailedRuntime:
		return 1, true
	case RestoreConfiguration:
		return 2, true
	case RestoreServingRuntime:
		return 3, true
	case RemoveFailedCandidate:
		return 4, true
	case VerifyRestoration:
		return 5, true
	case RecoveryComplete:
		return 6, true
	default:
		return 0, false
	}
}

func (progress RecoveryProgress) Validate(input RecoveryInput) error {
	if err := input.Validate(); err != nil {
		return err
	}
	return progress.validateEnvelope(input)
}

func (progress RecoveryProgress) validateEnvelope(input RecoveryInput) error {
	if input.ValidateIdentity() != nil || input.Input.Attempt.StartedAt == nil || progress.AttemptID != input.Input.Attempt.ID ||
		progress.UpdatedAt.IsZero() || progress.UpdatedAt.Location() != time.UTC || progress.UpdatedAt.Before(*input.Input.Attempt.StartedAt) ||
		progress.Evidence != nil && progress.Evidence.Validate() != nil {
		return invalidProgress()
	}
	index, valid := recoveryPhaseIndex(progress.Phase)
	sequence := index * 2
	if progress.InFlight {
		sequence++
	}
	if !valid || progress.Sequence != sequence || progress.Phase == RecoveryComplete && progress.InFlight ||
		(progress.Sequence >= 2) != (progress.Evidence != nil) {
		return invalidProgress()
	}
	return nil
}

func (progress RecoveryProgress) Begin(input RecoveryInput, at time.Time) (RecoveryProgress, error) {
	if progress.Validate(input) != nil || progress.InFlight || progress.Phase == RecoveryComplete || at.Before(progress.UpdatedAt) || !at.Before(input.Input.Attempt.Deadline) {
		return RecoveryProgress{}, invalidProgress()
	}
	progress.Sequence++
	progress.InFlight, progress.UpdatedAt = true, at.UTC()
	return progress, nil
}

func (progress RecoveryProgress) Confirm(input RecoveryInput, evidence workexecution.Input, at time.Time) (RecoveryProgress, error) {
	if progress.Validate(input) != nil || !progress.InFlight || evidence.Validate() != nil || at.Before(progress.UpdatedAt) || !at.Before(input.Input.Attempt.Deadline) {
		return RecoveryProgress{}, invalidProgress()
	}
	switch progress.Phase {
	case QuiesceForward:
		progress.Phase = InspectFailedRuntime
	case InspectFailedRuntime:
		progress.Phase = RestoreConfiguration
	case RestoreConfiguration:
		progress.Phase = RestoreServingRuntime
	case RestoreServingRuntime:
		progress.Phase = RemoveFailedCandidate
	case RemoveFailedCandidate:
		progress.Phase = VerifyRestoration
	case VerifyRestoration:
		progress.Phase = RecoveryComplete
	default:
		return RecoveryProgress{}, invalidProgress()
	}
	progress.Sequence++
	progress.InFlight, progress.UpdatedAt, progress.Evidence = false, at.UTC(), &evidence
	return progress, nil
}
