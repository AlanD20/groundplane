package releases

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
)

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

// Validate includes explicit absence: a first deployment still has a captured
// read revision, but cannot claim a runtime revision or predecessor digest.
func (reference BlueprintNativePredecessorReference) Validate() error {
	if ids.Validate(ids.KindService, reference.ServiceID) != nil || reference.FixedReadRevision <= 0 ||
		reference.ProjectionRevision < 0 || reference.ProjectionRevision > reference.FixedReadRevision {
		return CorruptReleaseRecord()
	}
	if reference.Serving == nil {
		if reference.RuntimeRevision != 0 || reference.PriorRuntimeSHA256 != "" {
			return CorruptReleaseRecord()
		}
		return nil
	}
	digest, err := hex.DecodeString(reference.PriorRuntimeSHA256)
	if err != nil || len(digest) != 32 || hex.EncodeToString(digest) != reference.PriorRuntimeSHA256 ||
		reference.ProjectionRevision <= 0 || reference.RuntimeRevision <= 0 || reference.RuntimeRevision > reference.FixedReadRevision ||
		ids.Validate(ids.KindDeployment, reference.Serving.ServingReleaseID) != nil || reference.Serving.Target.Validate() != nil ||
		reference.Serving.RetainedPriorReleaseID != "" &&
			ids.Validate(ids.KindDeployment, reference.Serving.RetainedPriorReleaseID) != nil {
		return CorruptReleaseRecord()
	}
	return nil
}
