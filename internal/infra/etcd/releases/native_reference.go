package releases

import domain "github.com/AlanD20/groundplane/internal/core/release"

type BlueprintNativeServingPredecessor struct {
	ServingReleaseID       string                `json:"serving_release_id"`
	Target                 domain.WorkloadTarget `json:"target"`
	RetainedPriorReleaseID string                `json:"retained_prior_release_id,omitempty"`
}

// BlueprintNativePredecessorReference binds the compact marker to the exact
// prior-runtime bytes in its candidate's immutable, manifest-owned render input.
// No reference points at a separately prunable predecessor Release input.
type BlueprintNativePredecessorReference struct {
	ServiceID          string                             `json:"service_id"`
	FixedReadRevision  int64                              `json:"fixed_read_revision"`
	ProjectionRevision int64                              `json:"projection_revision"`
	RuntimeRevision    int64                              `json:"runtime_revision,omitempty"`
	Serving            *BlueprintNativeServingPredecessor `json:"serving,omitempty"`
	PriorRuntimeSHA256 string                             `json:"prior_runtime_sha256,omitempty"`
}
