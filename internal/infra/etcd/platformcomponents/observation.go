package platformcomponents

import (
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const PlatformComponentOwnerPrefix = "/v1/indexes/components/by-owner/platform/"
const componentObservationPrefix = "/v1/observed/platform/components/"

func PlatformComponentOwnerKey(id string) string { return PlatformComponentOwnerPrefix + id }
func PlatformComponentKindKey(kind core.ComponentKind) string {
	return "/v1/indexes/components/by-kind/platform/" + recordcodec.EncodeKeySegment(string(kind))
}
func ComponentObservationKey(id string) string { return componentObservationPrefix + id }

func ValidatePlatformComponentRecord(record componentrecord.Record) error {
	if err := componentrecord.ValidateRecord(record); err != nil {
		return err
	}
	if record.Desired.Owner != core.ComponentOwnerPlatform || record.Desired.OwnerID != "" {
		return errs.New(errs.KindValidationFailed, "platform Component ownership is invalid")
	}
	return nil
}

// ComponentObservationRecord is replaceable Agent evidence, separate from desired state.
type ComponentObservationRecord struct {
	ComponentID         string                                          `json:"component_id"`
	ServiceID           string                                          `json:"service_id"`
	PlanID              string                                          `json:"plan_id"`
	ComposeArtifactID   string                                          `json:"compose_artifact_id"`
	ComposeArtifact     *agentpb.ComposeArtifact                        `json:"compose_artifact,omitempty"`
	Enabled             bool                                            `json:"enabled"`
	Healthy             bool                                            `json:"healthy"`
	DesiredGeneration   uint64                                          `json:"desired_generation"`
	RenderGeneration    uint64                                          `json:"render_generation"`
	AgentID             string                                          `json:"agent_id"`
	AgentGeneration     uint64                                          `json:"agent_generation"`
	BaselineGeneration  uint64                                          `json:"baseline_generation"`
	OwnershipGeneration uint64                                          `json:"ownership_generation"`
	CorefileSHA256      string                                          `json:"corefile_sha256,omitempty"`
	InputSHA256         string                                          `json:"input_sha256,omitempty"`
	ObservedAt          time.Time                                       `json:"observed_at"`
	TaskID              string                                          `json:"task_id"`
	StepID              string                                          `json:"step_id"`
	Revision            uint64                                          `json:"revision"`
	PredecessorTaskID   string                                          `json:"predecessor_task_id,omitempty"`
	DNSResolverProof    *taskjournal.TaskDNSResolverObservationEvidence `json:"dns_resolver_proof,omitempty"`
}

func ValidateComponentObservation(record ComponentObservationRecord) error {
	if ids.Validate(ids.KindComponent, record.ComponentID) != nil ||
		ids.Validate(ids.KindService, record.ServiceID) != nil ||
		ids.Validate(ids.KindPlan, record.PlanID) != nil ||
		ids.Validate(ids.KindConfig, record.ComposeArtifactID) != nil ||
		ids.Validate(ids.KindAgent, record.AgentID) != nil ||
		ids.Validate(ids.KindTask, record.TaskID) != nil ||
		ids.Validate(ids.KindStep, record.StepID) != nil ||
		record.Revision == 0 ||
		(record.PredecessorTaskID != "" && ids.Validate(ids.KindTask, record.PredecessorTaskID) != nil) ||
		(record.Revision == 1 && record.PredecessorTaskID != "") ||
		(record.Revision > 1 && record.PredecessorTaskID == "") ||
		record.DesiredGeneration == 0 ||
		record.RenderGeneration == 0 ||
		record.AgentGeneration == 0 ||
		record.OwnershipGeneration == 0 ||
		!recordcodec.IsCanonicalUTC(record.ObservedAt) {
		return errs.New(errs.KindValidationFailed, "Component observation identity or generation is invalid")
	}
	if record.Enabled {
		if !recordcodec.ValidSHA256(record.CorefileSHA256) || !recordcodec.ValidSHA256(record.InputSHA256) ||
			record.DNSResolverProof == nil || taskjournal.ValidateTaskResult(taskjournal.TaskResultRecord{
			Kind: taskjournal.TaskResultCompose, Diagnostic: taskjournal.TaskResultDiagnosticNone,
			DNSResolverCandidateObservation: record.DNSResolverProof,
		}, nil, taskjournal.TaskStatusCompleted) != nil || record.DNSResolverProof.ComponentID != record.ComponentID ||
			record.DNSResolverProof.ServiceID != record.ServiceID ||
			record.DNSResolverProof.RenderGeneration != record.RenderGeneration ||
			record.DNSResolverProof.ArtifactSHA256 != record.CorefileSHA256 ||
			record.ComposeArtifact == nil || !platformComponentArtifactMatches(
			record.ComposeArtifact,
			record.ComposeArtifactID,
			record.ServiceID,
			record.DNSResolverProof.ImageConfigDigest,
		) {
			return errs.New(errs.KindValidationFailed, "enabled Component observation digests are invalid")
		}
	} else if record.CorefileSHA256 != "" && !recordcodec.ValidSHA256(record.CorefileSHA256) ||
		record.InputSHA256 != "" || record.DNSResolverProof != nil || record.ComposeArtifact != nil {
		return errs.New(errs.KindValidationFailed, "disabled Component observation carries rendered digests")
	}
	return nil
}
func EncodeComponentObservation(record ComponentObservationRecord) ([]byte, error) {
	if err := ValidateComponentObservation(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("component_observation", record)
}
func DecodeComponentObservation(value []byte) (ComponentObservationRecord, error) {
	record, err := recordcodec.Decode[ComponentObservationRecord](value, "component_observation")
	if err != nil || ValidateComponentObservation(record) != nil {
		return ComponentObservationRecord{}, errs.New(errs.KindInternal, "Component observation is corrupt")
	}
	return record, nil
}
