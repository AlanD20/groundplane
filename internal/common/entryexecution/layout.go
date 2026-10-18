// Package entryexecution owns the fixed execution procedure for captured Entry
// configuration changes. It cannot select desired state or start dependencies.
package entryexecution

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Stage string

const (
	WriteConfiguration  Stage = "write_configuration"
	VerifyConfiguration Stage = "verify_configuration"
	ReconcileConsumers  Stage = "reconcile_consumers"
	VerifyConsumers     Stage = "verify_consumers"
	Complete            Stage = "complete"
)

// Layout is checkpoint metadata, not executable input. Assignments retain the
// artifacts and exact file sources separately rather than copying them into
// every progress record. A files-only operation has no runtime stages.
type Layout struct {
	EnvironmentID string              `json:"environment_id"`
	Input         workexecution.Input `json:"input"`
	Removal       bool                `json:"removal"`
	FileCount     int                 `json:"file_count"`
	Consumers     []string            `json:"consumers,omitempty"`
}

func CaptureLayout(attempt workexecution.Attempt, input *agentpb.EntryConfigurationWorkInput) (Layout, error) {
	_, raw, reference, err := executionplan.CaptureEntryConfigurationWorkInput(attempt.Input.ID, input)
	clear(raw)
	if err != nil || reference != attempt.Input {
		return Layout{}, invalidLayout()
	}
	layout := Layout{EnvironmentID: input.EnvironmentId, Input: reference, Removal: input.Removal,
		FileCount: len(input.Files), Consumers: slices.Clone(input.RunningConsumerIds)}
	return layout, layout.Validate(attempt)
}

func (layout Layout) Validate(attempt workexecution.Attempt) error {
	if attempt.Validate() != nil || attempt.Executor.Executor != workexecution.Agent ||
		ids.Validate(ids.KindEnvironment, layout.EnvironmentID) != nil || layout.Input != attempt.Input ||
		layout.FileCount < 1 || layout.FileCount > 512 || len(layout.Consumers) > 512 ||
		len(layout.Consumers) != 0 && (layout.Removal || layout.FileCount == 0) {
		return invalidLayout()
	}
	previous := ""
	for _, consumer := range layout.Consumers {
		if ids.Validate(ids.KindService, consumer) != nil || consumer <= previous {
			return invalidLayout()
		}
		previous = consumer
	}
	return nil
}

func (layout Layout) Matches(attempt workexecution.Attempt, input *agentpb.EntryConfigurationWorkInput) error {
	captured, err := CaptureLayout(attempt, input)
	if err != nil || captured.EnvironmentID != layout.EnvironmentID || captured.Input != layout.Input ||
		captured.Removal != layout.Removal || captured.FileCount != layout.FileCount ||
		!slices.Equal(captured.Consumers, layout.Consumers) {
		return invalidLayout()
	}
	return nil
}

func (layout Layout) stages() []Stage {
	var stages []Stage
	if layout.FileCount != 0 {
		stages = append(stages, WriteConfiguration, VerifyConfiguration)
	}
	if len(layout.Consumers) != 0 {
		stages = append(stages, ReconcileConsumers, VerifyConsumers)
	}
	return stages
}

func invalidLayout() error {
	return errs.New(errs.KindStateConflict, "Entry execution differs from its captured file and consumer selection")
}
