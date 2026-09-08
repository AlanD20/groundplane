package release

import (
	"strings"

	"github.com/distribution/reference"
)

// WorkloadSeal is immutable execution authority captured before publication.
// RequestedReference is provenance; only LocalImageID selects runtime bytes.
type WorkloadSeal struct {
	RequestedReference string `json:"requested_reference"`
	LocalImageID       string `json:"local_image_id"`
	ReplicaCount       uint32 `json:"replica_count"`
}

func ValidateWorkloadSeal(value WorkloadSeal) error {
	if value.ReplicaCount == 0 || len(value.RequestedReference) > 512 ||
		!strings.HasPrefix(value.LocalImageID, "sha256:") ||
		!validSHA256(strings.TrimPrefix(value.LocalImageID, "sha256:")) {
		return invalid("release workload seal is invalid")
	}
	for _, char := range value.RequestedReference {
		if char > 127 {
			return invalid("release requested image reference is invalid")
		}
	}
	named, err := reference.ParseNormalizedNamed(value.RequestedReference)
	if err != nil || reference.IsNameOnly(named) {
		return invalid("release requested image reference is invalid")
	}
	return nil
}
