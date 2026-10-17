package volumeexecution

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Stage string

const (
	PrepareDirectory Stage = "prepare_directory"
	PrepareVolume    Stage = "prepare_volume"
	VerifyVolume     Stage = "verify_volume"
	DetachConsumers  Stage = "detach_consumers"
	RemoveVolume     Stage = "remove_volume"
	RemoveDirectory  Stage = "remove_directory"
	Complete         Stage = "complete"
)

// Observation is an ownership-checked host inspection, not a command exit code.
// A removal observation must keep proving earlier absence/detachment conditions.
type Observation struct {
	VolumeID          string              `json:"volume_id"`
	Input             workexecution.Input `json:"input"`
	DirectoryReady    bool                `json:"directory_ready"`
	VolumeReady       bool                `json:"volume_ready"`
	ConsumersDetached bool                `json:"consumers_detached"`
	VolumeAbsent      bool                `json:"volume_absent"`
	DirectoryAbsent   bool                `json:"directory_absent"`
	At                time.Time           `json:"at"`
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

func stages(action agentpb.VolumeWorkAction) []Stage {
	switch action {
	case agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_CREATE:
		return []Stage{PrepareDirectory, PrepareVolume}
	case agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_VERIFY:
		return []Stage{VerifyVolume}
	case agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE:
		return []Stage{DetachConsumers, RemoveVolume, RemoveDirectory}
	default:
		return nil
	}
}

func NewProgress(attempt workexecution.Attempt, action agentpb.VolumeWorkAction, volumeID string, at time.Time) (Progress, error) {
	steps := stages(action)
	if len(steps) == 0 || !at.Before(attempt.Deadline) {
		return Progress{}, invalidProgress()
	}
	progress := Progress{AttemptID: attempt.ID, Stage: steps[0], UpdatedAt: at.UTC()}
	return progress, progress.Validate(attempt, action, volumeID)
}

func (progress Progress) Validate(attempt workexecution.Attempt, action agentpb.VolumeWorkAction, volumeID string) error {
	steps := stages(action)
	if attempt.Validate() != nil || attempt.Direction != workexecution.Forward || attempt.StartedAt == nil ||
		ids.Validate(ids.KindVolume, volumeID) != nil || len(steps) == 0 || progress.AttemptID != attempt.ID ||
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
		if observation.VolumeID != volumeID || observation.Input != attempt.Input ||
			observation.At.IsZero() || observation.At.Before(*attempt.StartedAt) || observation.At.After(progress.UpdatedAt) ||
			!observation.proves(steps[index-1]) {
			return invalidProgress()
		}
	}
	return nil
}

// Begin is persisted and acknowledged before starting the named effect. An
// in-flight checkpoint can only be resolved by inspection, never by Begin again.
func (progress Progress) Begin(attempt workexecution.Attempt, action agentpb.VolumeWorkAction, volumeID string, at time.Time) (Progress, error) {
	if progress.Validate(attempt, action, volumeID) != nil || progress.InFlight || progress.Stage == Complete ||
		at.Before(progress.UpdatedAt) || !at.Before(attempt.Deadline) {
		return Progress{}, invalidProgress()
	}
	next := progress
	began := at.UTC()
	next.Sequence++
	next.InFlight, next.BeganAt, next.UpdatedAt = true, &began, began
	return next, next.Validate(attempt, action, volumeID)
}

func (progress Progress) Confirm(attempt workexecution.Attempt, action agentpb.VolumeWorkAction, volumeID string, observation Observation, at time.Time) (Progress, error) {
	if progress.Validate(attempt, action, volumeID) != nil || !progress.InFlight || at.Before(progress.UpdatedAt) ||
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
	return next, next.Validate(attempt, action, volumeID)
}

func (observation Observation) proves(stage Stage) bool {
	if observation.DirectoryReady && observation.DirectoryAbsent || observation.VolumeReady && observation.VolumeAbsent {
		return false
	}
	switch stage {
	case PrepareDirectory:
		return observation.DirectoryReady
	case PrepareVolume, VerifyVolume:
		return observation.DirectoryReady && observation.VolumeReady
	case DetachConsumers:
		return observation.ConsumersDetached
	case RemoveVolume:
		return observation.ConsumersDetached && observation.VolumeAbsent
	case RemoveDirectory:
		return observation.ConsumersDetached && observation.VolumeAbsent && observation.DirectoryAbsent
	default:
		return false
	}
}

// Satisfies reports whether fresh ownership-checked state already meets a
// stage's postcondition. It grants no authority to perform an effect.
func (observation Observation) Satisfies(stage Stage) bool { return observation.proves(stage) }

func invalidProgress() error {
	return errs.New(errs.KindStateConflict, "Volume progress does not prove its required ownership, consumer and storage checks")
}
