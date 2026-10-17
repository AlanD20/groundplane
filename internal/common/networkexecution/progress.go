package networkexecution

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Stage string

const (
	EnsureNetwork Stage = "ensure_network"
	RemoveNetwork Stage = "remove_network"
	Complete      Stage = "complete"
)

// Observation comes from ownership-checked inspection of the captured bridge.
// Ready means its full configuration matches, not merely that its name exists.
type Observation struct {
	NetworkID string              `json:"network_id"`
	Input     workexecution.Input `json:"input"`
	Ready     bool                `json:"ready"`
	Absent    bool                `json:"absent"`
	At        time.Time           `json:"at"`
}
type Progress struct {
	AttemptID   string       `json:"attempt_id"`
	Sequence    uint64       `json:"sequence"`
	Stage       Stage        `json:"stage"`
	InFlight    bool         `json:"in_flight"`
	BeganAt     *time.Time   `json:"began_at,omitempty"`
	UpdatedAt   time.Time    `json:"updated_at"`
	Observation *Observation `json:"observation,omitempty"`
}

func stages(action agentpb.NetworkWorkAction) []Stage {
	switch action {
	case agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_ENSURE:
		return []Stage{EnsureNetwork}
	case agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_REMOVE:
		return []Stage{RemoveNetwork}
	default:
		return nil
	}
}

func NewProgress(attempt workexecution.Attempt, action agentpb.NetworkWorkAction, networkID string, at time.Time) (Progress, error) {
	steps := stages(action)
	if len(steps) == 0 || !at.Before(attempt.Deadline) {
		return Progress{}, invalidProgress()
	}
	progress := Progress{AttemptID: attempt.ID, Stage: steps[0], UpdatedAt: at.UTC()}
	return progress, progress.Validate(attempt, action, networkID)
}

func (progress Progress) Validate(attempt workexecution.Attempt, action agentpb.NetworkWorkAction, networkID string) error {
	steps := stages(action)
	if attempt.Validate() != nil || attempt.Direction != workexecution.Forward || attempt.StartedAt == nil ||
		ids.Validate(ids.KindNetwork, networkID) != nil || len(steps) == 0 || progress.AttemptID != attempt.ID ||
		progress.Sequence > uint64(2*len(steps)) || progress.InFlight != (progress.Sequence%2 == 1) ||
		progress.InFlight != (progress.BeganAt != nil) || progress.UpdatedAt.IsZero() || progress.UpdatedAt.Before(*attempt.StartedAt) {
		return invalidProgress()
	}
	if progress.BeganAt != nil && (progress.BeganAt.Before(*attempt.StartedAt) ||
		!progress.BeganAt.Before(attempt.Deadline) || progress.BeganAt.After(progress.UpdatedAt)) {
		return invalidProgress()
	}
	index := progress.Sequence / 2
	expected := Complete
	if index < uint64(len(steps)) {
		expected = steps[index]
	}
	if progress.Stage != expected || (progress.Observation != nil) != (index > 0) {
		return invalidProgress()
	}
	if observation := progress.Observation; observation != nil {
		if observation.NetworkID != networkID || observation.Input != attempt.Input ||
			observation.At.IsZero() || observation.At.Before(*attempt.StartedAt) || observation.At.After(progress.UpdatedAt) ||
			!observation.proves(steps[index-1]) {
			return invalidProgress()
		}
	}
	return nil
}

// Begin is persisted and acknowledged before starting the named effect. An
// in-flight checkpoint can only be resolved by inspection, never by Begin again.
func (progress Progress) Begin(attempt workexecution.Attempt, action agentpb.NetworkWorkAction, networkID string, at time.Time) (Progress, error) {
	if progress.Validate(attempt, action, networkID) != nil || progress.InFlight || progress.Stage == Complete ||
		at.Before(progress.UpdatedAt) || !at.Before(attempt.Deadline) {
		return Progress{}, invalidProgress()
	}
	next := progress
	began := at.UTC()
	next.Sequence++
	next.InFlight, next.BeganAt, next.UpdatedAt = true, &began, began
	return next, next.Validate(attempt, action, networkID)
}

func (progress Progress) Confirm(attempt workexecution.Attempt, action agentpb.NetworkWorkAction, networkID string, observation Observation, at time.Time) (Progress, error) {
	if progress.Validate(attempt, action, networkID) != nil || !progress.InFlight || at.Before(progress.UpdatedAt) ||
		observation.At.Before(*progress.BeganAt) || observation.At.After(at) || !observation.proves(progress.Stage) {
		return Progress{}, invalidProgress()
	}
	next := progress
	next.Sequence++
	next.InFlight, next.BeganAt, next.UpdatedAt = false, nil, at.UTC()
	next.Observation = &observation
	next.Stage = Complete
	steps := stages(action)
	if index := next.Sequence / 2; index < uint64(len(steps)) {
		next.Stage = steps[index]
	}
	return next, next.Validate(attempt, action, networkID)
}

func (observation Observation) proves(stage Stage) bool {
	if observation.Ready && observation.Absent {
		return false
	}
	switch stage {
	case EnsureNetwork:
		return observation.Ready
	case RemoveNetwork:
		return observation.Absent
	default:
		return false
	}
}

// Satisfies grants no mutation authority; it only checks fresh observed state.
func (observation Observation) Satisfies(stage Stage) bool { return observation.proves(stage) }

func invalidProgress() error {
	return errs.New(errs.KindStateConflict, "Network progress does not prove the captured bridge configuration or removal")
}
