package rollingrelease

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Completion identifies the final native runtime whose workloads, predecessor
// absence and optional proxy were proved by the assigned Agent. A phase cursor
// alone is not serving evidence.
type Completion struct {
	ServiceID         string `json:"service_id"`
	ReleaseID         string `json:"release_id"`
	ArtifactID        string `json:"artifact_id"`
	RuntimeSHA256     string `json:"runtime_sha256"`
	Replicas          uint32 `json:"replicas"`
	ProxyGeneration   uint64 `json:"proxy_generation"`
	ProxyConfigSHA256 string `json:"proxy_config_sha256,omitempty"`
	Compensated       bool   `json:"compensated"`
}

func ValidateCompletion(value Completion) error {
	if ids.Validate(ids.KindService, value.ServiceID) != nil ||
		ids.Validate(ids.KindDeployment, value.ReleaseID) != nil ||
		ids.Validate(ids.KindConfig, value.ArtifactID) != nil ||
		!completionDigest(value.RuntimeSHA256) ||
		value.Replicas == 0 ||
		value.Replicas > MaximumReplicas ||
		value.ProxyGeneration == 0 && value.ProxyConfigSHA256 != "" ||
		value.ProxyGeneration != 0 && !completionDigest(value.ProxyConfigSHA256) {
		return errs.New(
			errs.KindValidationFailed,
			"rolling completion identity, replica count or configuration digest is invalid",
		)
	}
	return nil
}

func completionDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == value
}
