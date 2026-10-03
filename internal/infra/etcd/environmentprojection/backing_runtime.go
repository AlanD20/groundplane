package environmentprojection

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingruntimefact"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BackingRuntimeReceipt seals the actual native image and exact artifact from
// successful assigned provisioning. It exists only in acknowledged applied state.
type BackingRuntimeReceipt struct {
	ServiceID            string    `json:"service_id"`
	ArtifactSHA256       string    `json:"artifact_sha256"`
	LocalImageID         string    `json:"local_image_id"`
	ManagedReleaseSHA256 string    `json:"managed_release_sha256"`
	TaskID               string    `json:"task_id"`
	PlanID               string    `json:"plan_id"`
	PlanHash             string    `json:"plan_hash"`
	AgentID              string    `json:"agent_id"`
	AssignmentID         string    `json:"assignment_id"`
	ExecutionEpoch       uint32    `json:"execution_epoch"`
	RenderGeneration     uint64    `json:"render_generation"`
	AcknowledgedAt       time.Time `json:"acknowledged_at"`
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
		ids.Validate(ids.KindTask, source.TaskID) != nil || ids.Validate(ids.KindAgent, source.AgentID) != nil ||
		ids.Validate(ids.KindAssignment, source.AssignmentID) != nil || source.ExecutionEpoch == 0 ||
		!recordcodec.ValidSHA256(source.PlanHash) || !recordcodec.ValidSHA256(source.ArtifactSHA256) ||
		!recordcodec.ValidSHA256(source.ManagedReleaseSHA256) ||
		!workloadimage.LocalIDValid(source.LocalImageID) || source.RenderGeneration > projection.RenderGeneration ||
		source.AcknowledgedAt.IsZero() || source.AcknowledgedAt.Location() != time.UTC {
		return nil, nil, invalidBackingRuntime()
	}
	member := false
	for _, desired := range projection.DesiredServices {
		if desired.Desired.ID == serviceID && desired.EnvironmentID == projection.EnvironmentID &&
			desired.Desired.Adapter == "postgres:16" && desired.BackingNetworkID != "" {
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
	workload, err := backingruntimefact.Workload(artifact, serviceID, source.PlanID, source.RenderGeneration)
	if err != nil {
		return nil, nil, err
	}
	return artifact, workload, nil
}

func invalidBackingRuntime() error {
	return errs.New(errs.KindStateConflict, "acknowledged Backing runtime receipt is unavailable or changed")
}
