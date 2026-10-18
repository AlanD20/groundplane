// Package lifecycleexecution owns bounded Service lifecycle checkpoints. A
// saved in-flight stage authorizes inspection, never automatic effect replay.
package lifecycleexecution

import (
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type State string

const (
	Running       State = "running"
	Stopped       State = "stopped"
	Absent        State = "absent"
	HookSucceeded State = "hook_succeeded"
)

type Stage struct {
	ID       string `json:"id"`
	Member   string `json:"member,omitempty"`
	Expected State  `json:"expected"`
}

type Receipt struct {
	StageID    string              `json:"stage_id"`
	Member     string              `json:"member,omitempty"`
	State      State               `json:"state"`
	Evidence   workexecution.Input `json:"evidence"`
	ObservedAt time.Time           `json:"observed_at"`
}

type Progress struct {
	Inherited int        `json:"inherited,omitempty"`
	AttemptID string     `json:"attempt_id"`
	Sequence  uint64     `json:"sequence"`
	BeganAt   *time.Time `json:"began_at,omitempty"`
	UpdatedAt time.Time  `json:"updated_at"`
	Completed []Receipt  `json:"completed,omitempty"`
}

func Stages(input *agentpb.ServiceLifecycleWorkInput) ([]Stage, error) {
	if input == nil || len(input.Members) == 0 || len(input.Members) > rollingrelease.MaximumReplicas+1 {
		return nil, invalid()
	}
	state := Running
	switch input.Action {
	case agentpb.ServiceLifecycleAction_SERVICE_LIFECYCLE_ACTION_START:
	case agentpb.ServiceLifecycleAction_SERVICE_LIFECYCLE_ACTION_STOP:
		state = Stopped
	case agentpb.ServiceLifecycleAction_SERVICE_LIFECYCLE_ACTION_DESTROY:
		state = Absent
	default:
		return nil, invalid()
	}
	stages := make([]Stage, 0, len(input.Members)+1)
	if hook := input.Hook; hook != nil && input.Action != agentpb.ServiceLifecycleAction_SERVICE_LIFECYCLE_ACTION_START {
		if hook.Event != agentpb.BackingHookEvent_BACKING_HOOK_EVENT_BEFORE_STOP {
			return nil, invalid()
		}
		stages = append(stages, Stage{ID: hook.StageId, Expected: HookSucceeded})
	}
	for _, member := range input.Members {
		if member == nil || member.Ownership == nil || len(member.Ownership.ComposeNames) != 1 || member.Ownership.ComposeNames[0] == "" {
			return nil, invalid()
		}
		stages = append(stages, Stage{ID: member.Ownership.StepId, Member: member.Ownership.ComposeNames[0], Expected: state})
	}
	if hook := input.Hook; hook != nil && input.Action == agentpb.ServiceLifecycleAction_SERVICE_LIFECYCLE_ACTION_START {
		if hook.Event != agentpb.BackingHookEvent_BACKING_HOOK_EVENT_AFTER_START {
			return nil, invalid()
		}
		stages = append(stages, Stage{ID: hook.StageId, Expected: HookSucceeded})
	}
	seen := make(map[string]bool, len(stages))
	for _, stage := range stages {
		if ids.Validate(ids.KindStep, stage.ID) != nil || seen[stage.ID] {
			return nil, invalid()
		}
		seen[stage.ID] = true
	}
	return stages, nil
}

func NewProgress(attempt workexecution.Attempt, input *agentpb.ServiceLifecycleWorkInput, at time.Time) (Progress, error) {
	progress := Progress{AttemptID: attempt.ID, UpdatedAt: at.UTC()}
	if !at.Before(attempt.Deadline) {
		return Progress{}, invalid()
	}
	return progress, progress.Validate(attempt, input)
}

func (progress Progress) Validate(attempt workexecution.Attempt, input *agentpb.ServiceLifecycleWorkInput) error {
	stages, err := Stages(input)
	if err != nil {
		return err
	}
	if input.CapturedSource == nil || input.CapturedSource.InputId != attempt.Input.ID || hex.EncodeToString(input.CapturedSource.Sha256) != attempt.Input.SHA256 {
		return invalid()
	}
	return progress.ValidateStages(attempt, stages)
}

// ValidateStages validates journal metadata without retaining Compose files or
// private hook inputs. Callers must bind the stages to the captured assignment
// again before executing work.
func (progress Progress) ValidateStages(attempt workexecution.Attempt, stages []Stage) error {
	if err := validateStages(stages); err != nil {
		return err
	}
	return progress.validateReceipts(attempt, stages)
}

func validateStages(stages []Stage) error {
	if len(stages) == 0 || len(stages) > rollingrelease.MaximumReplicas+2 {
		return invalid()
	}
	seen := make(map[string]bool, len(stages))
	for _, stage := range stages {
		if ids.Validate(ids.KindStep, stage.ID) != nil || seen[stage.ID] ||
			(stage.Expected != Running && stage.Expected != Stopped && stage.Expected != Absent && stage.Expected != HookSucceeded) ||
			(stage.Expected == HookSucceeded) != (stage.Member == "") {
			return invalid()
		}
		seen[stage.ID] = true
	}
	return nil
}

func (progress Progress) validateReceipts(attempt workexecution.Attempt, stages []Stage) error {
	if attempt.Validate() != nil || attempt.Direction != workexecution.Forward || attempt.StartedAt == nil ||
		progress.AttemptID != attempt.ID || len(progress.Completed) > len(stages) ||
		progress.Inherited < 0 || progress.Inherited > len(progress.Completed) ||
		(progress.BeganAt != nil && len(progress.Completed) == len(stages)) ||
		progress.Sequence != progress.expectedSequence() || progress.UpdatedAt.Location() != time.UTC ||
		progress.UpdatedAt.Before(*attempt.StartedAt) {
		return invalid()
	}
	previous := *attempt.StartedAt
	for index, receipt := range progress.Completed {
		if index == 0 && progress.Inherited > 0 {
			previous = time.Time{}
		}
		if index == progress.Inherited && previous.Before(*attempt.StartedAt) {
			previous = *attempt.StartedAt
		}
		stage := stages[index]
		if receipt.StageID != stage.ID || receipt.Member != stage.Member || receipt.State != stage.Expected || receipt.Evidence.Validate() != nil ||
			(index < progress.Inherited && receipt.ObservedAt.After(*attempt.StartedAt)) ||
			receipt.ObservedAt.Location() != time.UTC || receipt.ObservedAt.Before(previous) || receipt.ObservedAt.After(progress.UpdatedAt) {
			return invalid()
		}
		previous = receipt.ObservedAt
	}
	if previous.Before(*attempt.StartedAt) {
		previous = *attempt.StartedAt
	}
	if began := progress.BeganAt; began != nil && (began.Location() != time.UTC || began.Before(previous) || !began.Before(attempt.Deadline) || began.After(progress.UpdatedAt)) {
		return invalid()
	}
	return nil
}

func (progress Progress) expectedSequence() uint64 {
	sequence := uint64(2 * (len(progress.Completed) - progress.Inherited))
	if progress.Inherited > 0 {
		sequence++ // The first report acknowledges the inherited prefix.
	}
	if progress.BeganAt != nil {
		sequence++
	}
	return sequence
}

func (progress Progress) Begin(attempt workexecution.Attempt, input *agentpb.ServiceLifecycleWorkInput, at time.Time) (Progress, error) {
	if progress.Validate(attempt, input) != nil || progress.BeganAt != nil || at.Before(progress.UpdatedAt) || !at.Before(attempt.Deadline) {
		return Progress{}, invalid()
	}
	stages, err := Stages(input)
	if err != nil || len(progress.Completed) == len(stages) {
		return Progress{}, invalid()
	}
	next := progress
	began := at.UTC()
	next.BeganAt, next.UpdatedAt, next.Sequence = &began, began, progress.Sequence+1
	return next, next.Validate(attempt, input)
}

func (progress Progress) Confirm(attempt workexecution.Attempt, input *agentpb.ServiceLifecycleWorkInput, receipt Receipt, at time.Time) (Progress, error) {
	if progress.Validate(attempt, input) != nil || progress.BeganAt == nil || at.Before(progress.UpdatedAt) || receipt.ObservedAt.Before(*progress.BeganAt) || receipt.ObservedAt.After(at) {
		return Progress{}, invalid()
	}
	next := progress
	next.Completed = append(append([]Receipt(nil), progress.Completed...), receipt)
	next.BeganAt, next.UpdatedAt, next.Sequence = nil, at.UTC(), progress.Sequence+1
	return next, next.Validate(attempt, input)
}

func invalid() error {
	return errs.New(errs.KindStateConflict, "Service lifecycle checkpoint does not match its assigned procedure")
}
