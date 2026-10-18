package releaserender

import (
	"bytes"
	"crypto/sha256"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// OpenBlueprintCandidateArtifact opens the exact pre-execution artifact bound
// into the rollout digest. It never renders desired state or reads an old plan.
// The artifact can retain unselected Services; only this work item's selected
// Service is an executable Release target.
func OpenBlueprintCandidateArtifact(capture BlueprintServiceRolloutCapture) (*agentpb.ComposeArtifact, error) {
	render := capture.Render
	encoded := capture.CandidateArtifact
	if len(encoded) == 0 || len(encoded) > keyvalue.MaximumBytes {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint candidate artifact is missing or oversized")
	}
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(encoded, artifact) != nil || executionplan.RejectUnknown(artifact) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint candidate artifact cannot be decoded")
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		return nil, errs.Wrap(errs.KindValidationFailed, err)
	}
	defer clear(canonical)
	digest := sha256.Sum256(artifact.CanonicalYaml)
	if !bytes.Equal(canonical, encoded) || artifact.ArtifactId != render.ArtifactID ||
		artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.OwnerId != render.EnvironmentID || artifact.ProjectName != "gp-"+strings.ToLower(render.EnvironmentID) ||
		artifact.AuthorizedVolumeDir != render.AuthorizedVolumeDir || len(artifact.CanonicalYaml) == 0 ||
		!bytes.Equal(digest[:], artifact.YamlSha256) {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint candidate artifact differs from its captured identity or content")
	}
	role := agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON
	expectedMembers := 1
	expectedSlot := ""
	if render.Strategy == domain.StrategyBlueGreen {
		role = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT
		expectedSlot = string(render.CandidateTarget)
	} else if render.Strategy == domain.StrategyRolling {
		role = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_ROLLING_REPLICA
		if render.Rolling == nil {
			return nil, errs.New(errs.KindValidationFailed, "Blueprint rolling candidate has no replica plan")
		}
		expectedMembers = len(render.Rolling.Candidate)
	}
	count := 0
	for _, service := range artifact.Services {
		if service.GetServiceId() != render.ServiceID || service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		if service.GetRole() != role || service.GetImageReference() != render.CandidateWorkload.LocalImageID ||
			service.GetOwnerComponentId() != "" {
			return nil, errs.New(errs.KindValidationFailed, "Blueprint candidate workload differs from its captured Release")
		}
		releaseID := ""
		for _, label := range service.GetExpectedLabels() {
			if label.GetKey() == "com.groundplane.release-id" {
				if releaseID != "" {
					return nil, errs.New(errs.KindValidationFailed, "Blueprint candidate repeats its Release label")
				}
				releaseID = label.GetValue()
			}
		}
		if releaseID != render.ReleaseID {
			return nil, errs.New(errs.KindValidationFailed, "Blueprint candidate belongs to another Release")
		}
		if render.Strategy != domain.StrategyRolling &&
			(service.GetExpectedReplicas() != render.CandidateWorkload.ReplicaCount ||
				service.GetSlot() != expectedSlot) {
			return nil, errs.New(errs.KindValidationFailed, "Blueprint candidate count or target differs from its captured Release")
		}
		count++
	}
	if count != expectedMembers {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint candidate workload membership is incomplete")
	}
	if render.Rolling != nil {
		for _, replica := range render.Rolling.Candidate {
			if _, err := executionplan.RollingReplicaService(artifact, render.ServiceID, replica); err != nil {
				return nil, err
			}
		}
	}
	return artifact, nil
}
