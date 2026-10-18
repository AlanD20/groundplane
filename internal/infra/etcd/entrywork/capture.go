package entrywork

import (
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Capture retains file sources and the exact acknowledged consumers changed by
// this operation. Completion never reconstructs them from current desired state.
type Capture struct {
	Configuration runtimeconfiguration.WorkCapture `json:"configuration"`
	Secrets       *tasksecretpins.Binding          `json:"secrets,omitempty"`
	Consumers     []Consumer                       `json:"consumers"`
	Removal       *Removal                         `json:"removal,omitempty"`
}

type Consumer struct {
	Previous           serviceruntimerecord.Record `json:"previous"`
	Revision           int64                       `json:"revision"`
	CurrentArtifactID  string                      `json:"current_artifact_id"`
	RetainedArtifactID string                      `json:"retained_artifact_id,omitempty"`
}

func (capture Capture) matches(view workqueue.View, input *agentpb.EntryConfigurationWorkInput) error {
	task, item := view.Task.Task, view.Item.Item
	if input == nil || item.Operation != workexecution.EntryConfiguration ||
		input.Removal != (task.Action == workexecution.ActionRemove) ||
		input.Removal != (capture.Removal != nil) ||
		item.Placement.Executor != workexecution.Agent || item.TargetID != task.Scope.EnvironmentID ||
		input.EnvironmentId != task.Scope.EnvironmentID || capture.Configuration.Matches(task, item) != nil ||
		capture.Configuration.ReadRevision > view.ReadRevision || input.RenderGeneration != capture.Configuration.Generation ||
		!configurationMatches(input.Configuration, capture.Configuration.Current) ||
		(input.PriorConfiguration == nil) != (capture.Configuration.Prior == nil) ||
		capture.Configuration.Prior != nil && !configurationMatches(input.PriorConfiguration, *capture.Configuration.Prior) {
		return conflict("Entry configuration capture differs from its operation or file sources")
	}
	if capture.Removal != nil {
		if err := capture.Removal.matches(view, input.EnvironmentId, input.RenderGeneration); err != nil {
			return err
		}
	}
	destinations := make([]string, len(input.Files))
	for index, file := range input.Files {
		if file == nil {
			return conflict("Entry configuration file descriptor is missing")
		}
		destinations[index] = file.Destination
	}
	if !slices.Equal(destinations, capture.Configuration.Destinations) ||
		len(capture.Consumers) != len(input.RunningConsumerIds) || input.Removal && len(capture.Consumers) != 0 {
		return conflict("Entry configuration capture changes different files or consumers")
	}
	for index, consumer := range capture.Consumers {
		if serviceruntimerecord.Validate(consumer.Previous) != nil ||
			consumer.Previous.EnvironmentID != input.EnvironmentId ||
			consumer.Previous.Runtime.ServiceID != input.RunningConsumerIds[index] ||
			consumer.Previous.Source.Executor.HostID != item.Placement.HostID ||
			consumer.Revision <= 0 || consumer.Revision > capture.Configuration.ReadRevision {
			return conflict("Entry consumer has no matching captured acknowledged runtime")
		}
		if _, err := executionplan.PrepareEntryRuntime(input.Candidate, consumer.Previous.Runtime,
			consumer.CurrentArtifactID, consumer.RetainedArtifactID); err != nil {
			return err
		}
	}
	if capture.Secrets != nil && (capture.Secrets.Validate() != nil ||
		capture.Secrets.OperationID != input.OperationId || capture.Secrets.TaskID != task.ID ||
		capture.Secrets.WorkItemID != item.ID) {
		return conflict("Entry Secret retention belongs to another operation")
	}
	return nil
}

func configurationMatches(wire *agentpb.WorkConfigurationReference, reference runtimeconfiguration.Reference) bool {
	return wire != nil && wire.ConfigurationId == reference.ID && wire.EnvironmentId == reference.EnvironmentID &&
		wire.Generation == reference.Generation && hex.EncodeToString(wire.Sha256) == reference.SHA256
}
