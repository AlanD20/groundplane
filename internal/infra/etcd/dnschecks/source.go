// Package dnschecks owns captured, read-only resolver checks and their results.
package dnschecks

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Source identifies the acknowledged configuration that was queried. Desired
// settings are a separate revision: checking applied files never validates a
// newer desired configuration that has not been activated successfully.
type Source struct {
	ComponentID         string `json:"component_id"`
	ComponentRevision   int64  `json:"component_revision"`
	ObservationRevision int64  `json:"observation_revision"`
	SingletonRevision   int64  `json:"singleton_revision"`
	AgentID             string `json:"agent_id"`
	AgentRevision       int64  `json:"agent_revision"`
	AgentGeneration     uint64 `json:"agent_generation"`
	AppliedTaskID       string `json:"applied_task_id"`
	ServiceID           string `json:"service_id"`
	ArtifactID          string `json:"artifact_id"`
	ArtifactSHA256      string `json:"artifact_sha256"`
	ImageReference      string `json:"image_reference"`
	ImageConfigDigest   string `json:"image_config_digest"`
	RenderGeneration    uint64 `json:"render_generation"`
}

func (source Source) Validate() error {
	if ids.Validate(ids.KindComponent, source.ComponentID) != nil ||
		ids.Validate(ids.KindAgent, source.AgentID) != nil ||
		ids.Validate(ids.KindTask, source.AppliedTaskID) != nil ||
		ids.Validate(ids.KindService, source.ServiceID) != nil ||
		ids.Validate(ids.KindConfig, source.ArtifactID) != nil ||
		source.ComponentRevision <= 0 || source.ObservationRevision <= 0 ||
		source.SingletonRevision <= 0 || source.AgentRevision <= 0 ||
		source.AgentGeneration == 0 || source.RenderGeneration == 0 ||
		!recordcodec.ValidSHA256(source.ArtifactSHA256) ||
		!recordcodec.ValidSHA256(source.ImageConfigDigest) || source.ImageReference == "" {
		return errs.New(errs.KindValidationFailed, "DNS check captured source is invalid")
	}
	return nil
}

func RecordKey(taskID string) string { return "/v1/runtime/dns-checks/" + taskID }
func LatestKey(componentID string) string {
	return "/v1/indexes/dns-checks/latest/" + componentID
}
func ActiveKey(componentID string) string {
	return "/v1/indexes/dns-checks/active/" + componentID
}
func OperationKey(operationID string) string {
	return "/v1/indexes/dns-checks/by-operation/" + operationID
}
