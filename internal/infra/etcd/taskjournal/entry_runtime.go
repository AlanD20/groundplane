package taskjournal

import (
	"slices"
)

const TaskEntryRuntimeEpochParam = "entry_runtime_epoch_revision"

// EntryTaskRuntime pins operational intent separately from desired input. An
// explicit empty set means materialization only; absent capture is not authority
// to reconstruct or publish an Entry update.
type EntryTaskRuntime struct {
	RunningServiceIDs []string             `json:"running_service_ids"`
	Updates           []EntryRuntimeUpdate `json:"updates"`
}

// EntryRuntimeUpdate is the bounded durable recipe for one selected Service.
// Runtime bytes remain in the prior receipt and immutable Entry projection.
type EntryRuntimeUpdate struct {
	ServiceID               string `json:"service_id"`
	PreviousRevision        int64  `json:"previous_revision"`
	CurrentArtifactID       string `json:"current_artifact_id"`
	RetainedPriorArtifactID string `json:"retained_prior_artifact_id,omitempty"`
}

func CloneEntryTaskRuntime(runtime *EntryTaskRuntime) *EntryTaskRuntime {
	if runtime == nil {
		return nil
	}
	cloned := &EntryTaskRuntime{RunningServiceIDs: slices.Clone(runtime.RunningServiceIDs),
		Updates: slices.Clone(runtime.Updates)}
	return cloned
}
