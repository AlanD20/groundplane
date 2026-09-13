// Package serviceruntimerecord owns the self-contained acknowledged native
// runtime value. It does not select desired state or change Release history.
package serviceruntimerecord

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Record is current applied authority, separate from the immutable Release
// that originally selected the workload. Artifact bytes survive Task pruning.
type Record struct {
	EnvironmentID string                         `json:"environment_id"`
	Runtime       executionplan.CandidateRuntime `json:"runtime"`
	Source        Acknowledgement                `json:"source"`
}

type Acknowledgement struct {
	TaskID           string    `json:"task_id"`
	PlanID           string    `json:"plan_id"`
	PlanHash         string    `json:"plan_hash"`
	StepID           string    `json:"step_id"`
	AgentID          string    `json:"agent_id"`
	AssignmentID     string    `json:"assignment_id"`
	ExecutionEpoch   uint32    `json:"execution_epoch"`
	RenderGeneration uint64    `json:"render_generation"`
	EffectDigest     string    `json:"effect_digest"`
	AcknowledgedAt   time.Time `json:"acknowledged_at"`
}

// Observation contains only the strategy-specific serving proof already
// validated against the owning Task assignment, never a fresh host discovery.
type Observation struct {
	ReleaseID          string
	Target             string
	Proxy              *ProxyObservation
	RecreateArtifactID string
}

type ProxyObservation struct {
	ProxyGeneration uint64
	ProxyConfigHash string
}

func Key(serviceID string) string { return "/v1/runtime/service-acknowledged-runtimes/" + serviceID }

// AcknowledgeRelease binds prepared post-activation bytes to the successful
// selected member's exact proof. Calling code publishes this with that member's
// checkpoint and serving projection in one fenced transaction.
func AcknowledgeRelease(
	environmentID string,
	runtime executionplan.CandidateRuntime,
	source Acknowledgement,
	observed Observation,
) (Record, error) {
	if runtime.ReleaseID != observed.ReleaseID || runtime.Target != observed.Target ||
		(observed.Proxy == nil) == (observed.RecreateArtifactID == "") {
		return Record{}, errs.New(
			errs.KindStateConflict,
			"release runtime acknowledgement differs from sealed activation",
		)
	}
	if observed.Proxy != nil && (runtime.ProxyGeneration != observed.Proxy.ProxyGeneration ||
		hex.EncodeToString(runtime.ProxyConfigSHA256) != observed.Proxy.ProxyConfigHash) {
		return Record{}, errs.New(errs.KindStateConflict, "release runtime proxy proof differs from sealed activation")
	}
	record := Record{EnvironmentID: environmentID, Runtime: runtime, Source: source}
	if err := Validate(record); err != nil {
		return Record{}, err
	}
	if observed.RecreateArtifactID != "" {
		artifact := &agentpb.ComposeArtifact{}
		if proto.Unmarshal(runtime.CurrentArtifact, artifact) != nil ||
			artifact.GetArtifactId() != observed.RecreateArtifactID {
			return Record{}, errs.New(errs.KindStateConflict, "release runtime recreate proof names another artifact")
		}
	}
	return record, nil
}

func Validate(record Record) error {
	source := record.Source
	if ids.Validate(ids.KindTask, source.TaskID) != nil || ids.Validate(ids.KindPlan, source.PlanID) != nil ||
		ids.Validate(ids.KindStep, source.StepID) != nil || ids.Validate(ids.KindAgent, source.AgentID) != nil ||
		ids.Validate(
			ids.KindAssignment,
			source.AssignmentID,
		) != nil || source.ExecutionEpoch == 0 || source.RenderGeneration == 0 ||
		!validDigest(source.PlanHash) || !validDigest(source.EffectDigest) ||
		source.AcknowledgedAt.IsZero() || source.AcknowledgedAt.Location() != time.UTC ||
		ids.Validate(ids.KindDeployment, record.Runtime.ReleaseID) != nil {
		return errs.New(errs.KindValidationFailed, "acknowledged Service runtime source is invalid")
	}
	if err := executionplan.ValidateNativePredecessorWitness(record.EnvironmentID, record.Runtime.ServiceID,
		record.Runtime.CurrentArtifact, record.Runtime.RetainedPriorArtifact); err != nil {
		return err
	}
	return validateRuntimeIdentity(record.Runtime)
}

func validateRuntimeIdentity(runtime executionplan.CandidateRuntime) error {
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(runtime.CurrentArtifact, artifact) != nil {
		return invalidRuntimeIdentity()
	}
	yamlHash := sha256.Sum256(artifact.GetCanonicalYaml())
	if !bytes.Equal(yamlHash[:], artifact.GetYamlSha256()) {
		return invalidRuntimeIdentity()
	}
	hasProxy := false
	for _, service := range artifact.GetServices() {
		if service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			hasProxy = true
			digest := sha256.Sum256(service.GetProxyConfigJson())
			generation, err := executionplan.ProxyConfigGeneration(service.GetProxyConfigJson(), runtime.ReleaseID)
			if err != nil || generation != runtime.ProxyGeneration ||
				!bytes.Equal(
					digest[:],
					runtime.ProxyConfigSHA256,
				) || !bytes.Equal(digest[:], service.GetProxyConfigSha256()) {
				return invalidRuntimeIdentity()
			}
			continue
		}
		target := "singleton"
		if service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT {
			target = service.GetSlot()
		}
		matchedRelease := false
		for _, label := range service.GetExpectedLabels() {
			if label.GetKey() == "com.groundplane.release-id" && label.GetValue() == runtime.ReleaseID {
				matchedRelease = true
			}
		}
		if !matchedRelease || target != runtime.Target {
			return invalidRuntimeIdentity()
		}
	}
	if !hasProxy && (runtime.ProxyGeneration != 0 || len(runtime.ProxyConfigSHA256) != 0) {
		return invalidRuntimeIdentity()
	}
	return nil
}

func invalidRuntimeIdentity() error {
	return errs.New(errs.KindValidationFailed, "acknowledged Service runtime artifact identity is invalid")
}

func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == value
}
