package environmentprojection

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/backingruntimefact"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BackingRuntimeReceipt seals the actual native image and exact artifact from
// successful assigned provisioning. It exists only in acknowledged applied state.
type BackingRuntimeReceipt struct {
	ServiceID            string                         `json:"service_id"`
	ArtifactSHA256       string                         `json:"artifact_sha256"`
	LocalImageID         string                         `json:"local_image_id"`
	ManagedReleaseSHA256 string                         `json:"managed_release_sha256"`
	Adapter              string                         `json:"adapter"`
	Attempt              workexecution.Attempt          `json:"attempt"`
	Checkpoint           workexecution.Checkpoint       `json:"checkpoint"`
	Observation          backingruntimefact.Observation `json:"observation"`
	RenderGeneration     uint64                         `json:"render_generation"`
	AcknowledgedAt       time.Time                      `json:"acknowledged_at"`
}

// ValidateBackingProvisioningReceipt validates self-contained native execution
// proof. No Task journal or live executor slot is needed after publication.
func ValidateBackingProvisioningReceipt(source BackingRuntimeReceipt) error {
	attempt, checkpoint := source.Attempt, source.Checkpoint
	if attempt.Validate() != nil || attempt.Direction != workexecution.Forward ||
		attempt.Executor.Executor != workexecution.Agent ||
		attempt.StartedAt == nil ||
		attempt.FinishedAt == nil ||
		attempt.Result == nil ||
		attempt.Outcome != workexecution.Succeeded && attempt.Outcome != workexecution.Cancelled &&
			attempt.Outcome != workexecution.Superseded ||
		checkpoint.AttemptID != attempt.ID ||
		checkpoint.Sequence == 0 ||
		checkpoint.Evidence.Validate() != nil ||
		checkpoint.Evidence.ID != attempt.Input.ID ||
		!source.AcknowledgedAt.Equal(*attempt.FinishedAt) ||
		checkpoint.RecordedAt.IsZero() ||
		checkpoint.RecordedAt.Location() != time.UTC ||
		checkpoint.RecordedAt.Before(*attempt.StartedAt) ||
		checkpoint.RecordedAt.After(source.AcknowledgedAt) ||
		backingruntimefact.ValidateObservation(source.Observation) != nil ||
		source.Observation.ServiceID != source.ServiceID ||
		source.Observation.ObservedAt.Before(*attempt.StartedAt) ||
		source.Observation.ObservedAt.After(source.AcknowledgedAt) ||
		source.Observation.LocalImageID != source.LocalImageID {
		return invalidBackingRuntime()
	}
	stageID, err := backingprovision.StageID(attempt.WorkItemID, backingprovision.VerifyRuntime)
	if err != nil || stageID != checkpoint.StageID {
		return invalidBackingRuntime()
	}
	evidence, err := backingprovision.CompletionEvidence(attempt, checkpoint, source.Observation)
	if err != nil || evidence != *attempt.Result {
		return invalidBackingRuntime()
	}
	return nil
}

func CloneBackingRuntimeReceipt(source *BackingRuntimeReceipt) *BackingRuntimeReceipt {
	if source == nil {
		return nil
	}
	copy := *source
	if source.Attempt.StartedAt != nil {
		at := *source.Attempt.StartedAt
		copy.Attempt.StartedAt = &at
	}
	if source.Attempt.FinishedAt != nil {
		at := *source.Attempt.FinishedAt
		copy.Attempt.FinishedAt = &at
	}
	if source.Attempt.Result != nil {
		result := *source.Attempt.Result
		copy.Attempt.Result = &result
	}
	copy.Observation = backingruntimefact.CloneObservations([]backingruntimefact.Observation{source.Observation})[0]
	return &copy
}

func ValidateBackingRuntimeReceipt(projection EnvironmentComposeProjection) error {
	if projection.BackingRuntime == nil {
		return nil
	}
	_, _, err := SelectBackingRuntime(projection, projection.BackingRuntime.ServiceID)
	return err
}

// SelectBackingRuntime never substitutes a prepared projection for a receipt.
// Metadata may advance the aggregate generation while preserving these bytes.
func SelectBackingRuntime(
	projection EnvironmentComposeProjection,
	serviceID string,
) (*agentpb.ComposeArtifact, *agentpb.ComposeService, error) {
	source := projection.BackingRuntime
	if source == nil || source.ServiceID != serviceID ||
		ValidateBackingProvisioningReceipt(*source) != nil || !recordcodec.ValidSHA256(source.ArtifactSHA256) ||
		(source.Adapter != "postgres" && source.Adapter != "mysql") ||
		(source.Adapter == "postgres" && !recordcodec.ValidSHA256(source.ManagedReleaseSHA256)) ||
		(source.Adapter == "mysql" && source.ManagedReleaseSHA256 != "") ||
		!workloadimage.LocalIDValid(source.LocalImageID) || source.RenderGeneration > projection.RenderGeneration ||
		source.AcknowledgedAt.IsZero() || source.AcknowledgedAt.Location() != time.UTC {
		return nil, nil, invalidBackingRuntime()
	}
	member := false
	for _, desired := range projection.DesiredServices {
		if desired.Desired.ID == serviceID && desired.EnvironmentID == projection.EnvironmentID &&
			desired.Desired.Adapter == source.Adapter && desired.BackingNetworkID != "" {
			member = true
		}
	}
	digest := sha256.Sum256(projection.ComposeArtifact)
	if !member || hex.EncodeToString(digest[:]) != source.ArtifactSHA256 {
		return nil, nil, invalidBackingRuntime()
	}
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(projection.ComposeArtifact, artifact) != nil || artifact.OwnerId != projection.EnvironmentID {
		return nil, nil, invalidBackingRuntime()
	}
	workload, err := backingruntimefact.Workload(artifact, serviceID, source.Attempt.Input.ID, source.RenderGeneration)
	if err != nil {
		return nil, nil, err
	}
	if !backingruntimefact.MatchesObservation(source.Observation, artifact, workload) {
		return nil, nil, invalidBackingRuntime()
	}
	return artifact, workload, nil
}

func invalidBackingRuntime() error {
	return errs.New(errs.KindStateConflict, "acknowledged Backing runtime receipt is unavailable or changed")
}
