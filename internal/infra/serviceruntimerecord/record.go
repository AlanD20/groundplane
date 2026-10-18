// Package serviceruntimerecord owns the self-contained acknowledged native
// runtime value. It does not select desired state or change Release history.
package serviceruntimerecord

import (
	"bytes"
	"crypto/sha256"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Record is current applied authority, separate from the immutable Release
// that originally selected the workload. Artifact bytes survive Task pruning.
type Record struct {
	EnvironmentID string                          `json:"environment_id"`
	Runtime       executionplan.CandidateRuntime  `json:"runtime"`
	Source        Acknowledgement                 `json:"source"`
	Configuration *runtimeconfiguration.Reference `json:"configuration,omitempty"`
}

type Acknowledgement struct {
	TaskID           string                   `json:"task_id"`
	WorkItemID       string                   `json:"work_item_id"`
	AttemptID        string                   `json:"attempt_id"`
	Executor         workexecution.Identity   `json:"executor"`
	Input            workexecution.Input      `json:"input"`
	Checkpoint       workexecution.Checkpoint `json:"checkpoint"`
	RenderGeneration uint64                   `json:"render_generation"`
	AcknowledgedAt   time.Time                `json:"acknowledged_at"`
}

func Key(serviceID string) string { return "/v1/runtime/service-acknowledged-runtimes/" + serviceID }

func Validate(record Record) error {
	source := record.Source
	if record.Configuration != nil &&
		(runtimeconfiguration.ValidateReference(*record.Configuration) != nil ||
			record.Configuration.EnvironmentID != record.EnvironmentID ||
			record.Configuration.Generation > source.RenderGeneration) {
		return errs.New(errs.KindValidationFailed, "acknowledged configuration source is invalid")
	}
	if ids.Validate(ids.KindTask, source.TaskID) != nil || ids.Validate(ids.KindWorkItem, source.WorkItemID) != nil ||
		ids.Validate(ids.KindAttempt, source.AttemptID) != nil || source.Executor.Validate() != nil ||
		source.Executor.Executor != workexecution.Agent || source.Input.Validate() != nil || source.RenderGeneration == 0 ||
		source.Checkpoint.AttemptID != source.AttemptID || source.Checkpoint.Sequence == 0 ||
		ids.Validate(ids.KindStep, source.Checkpoint.StageID) != nil || source.Checkpoint.Evidence.Validate() != nil ||
		source.Checkpoint.RecordedAt.IsZero() || source.Checkpoint.RecordedAt.Location() != time.UTC ||
		source.Checkpoint.RecordedAt.After(source.AcknowledgedAt) ||
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
			generation, err := executionplan.ProxyConfigGeneration(
				service.GetProxyImplementation(),
				service.GetProxyConfigJson(),
				runtime.ReleaseID,
			)
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
		} else if service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_ROLLING_REPLICA {
			target = "rolling"
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
