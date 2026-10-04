// Package backingruntimefact validates native, non-Release Backing workload
// evidence shared by terminal publication and Backup execution.
package backingruntimefact

import (
	"bytes"
	"crypto/sha256"
	"strconv"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupservicefact"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// Observation is retained from the assigned Task's terminal container inspection.
// It is not a later host discovery or a desired image selection.
type Observation struct {
	ProjectName    string                          `json:"project_name"`
	ObservedAt     time.Time                       `json:"observed_at"`
	ContainerID    string                          `json:"container_id"`
	ServiceID      string                          `json:"service_id"`
	ImageReference string                          `json:"image_reference"`
	LocalImageID   string                          `json:"local_image_id"`
	State          agentpb.ObservedContainerState  `json:"state"`
	Health         agentpb.ObservedContainerHealth `json:"health"`
	Labels         []*agentpb.LabelPair            `json:"labels"`
}

func ValidateObservation(value Observation) error {
	if value.ProjectName == "" || value.ContainerID == "" ||
		ids.Validate(ids.KindService, value.ServiceID) != nil || value.ImageReference == "" ||
		!workloadimage.LocalIDValid(value.LocalImageID) || value.ObservedAt.IsZero() ||
		value.ObservedAt.Location() != time.UTC || len(value.Labels) == 0 || len(value.Labels) > 128 ||
		value.State < agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_CREATED ||
		value.State > agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_DEAD ||
		value.Health < agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_NONE ||
		value.Health > agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_UNHEALTHY {
		return invalid()
	}
	_, err := backupservicefact.LabelsDigest(value.Labels)
	return err
}

func CloneObservations(source []Observation) []Observation {
	if source == nil {
		return nil
	}
	result := append([]Observation(nil), source...)
	for index := range result {
		result[index].Labels = make([]*agentpb.LabelPair, len(source[index].Labels))
		for labelIndex, pair := range source[index].Labels {
			result[index].Labels[labelIndex] = proto.CloneOf(pair)
		}
	}
	return result
}

// Workload selects the exact native Backing service. No release, slot, proxy,
// tenant or Component identity can masquerade as this provisioning workload.
func Workload(
	artifact *agentpb.ComposeArtifact,
	serviceID, planID string,
	generation uint64,
) (*agentpb.ComposeService, error) {
	if artifact == nil || artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		ids.Validate(ids.KindEnvironment, artifact.OwnerId) != nil || ids.Validate(ids.KindService, serviceID) != nil ||
		ids.Validate(ids.KindPlan, planID) != nil || generation == 0 {
		return nil, invalid()
	}
	digest := sha256.Sum256(artifact.CanonicalYaml)
	if !bytes.Equal(digest[:], artifact.YamlSha256) {
		return nil, invalid()
	}
	var selected *agentpb.ComposeService
	for _, candidate := range artifact.Services {
		if candidate.GetServiceId() == serviceID {
			if selected != nil {
				return nil, invalid()
			}
			selected = candidate
		}
	}
	if selected == nil || selected.ComposeName == "" || selected.ExpectedReplicas != 1 ||
		selected.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED ||
		selected.Slot != "" || selected.OwnerComponentId != "" ||
		!(imageref.IsDigestPinned(selected.ImageReference) ||
			postgres16protocol.ValidDatabaseImage(selected.ImageReference) && strings.Contains(selected.ImageReference, "@sha256:")) {
		return nil, invalid()
	}
	if _, err := backupservicefact.LabelsDigest(selected.ExpectedLabels); err != nil {
		return nil, err
	}
	labels := make(map[string]string, len(selected.ExpectedLabels))
	for _, pair := range selected.ExpectedLabels {
		labels[pair.Key] = pair.Value
	}
	if labels["com.groundplane.managed"] != "true" || labels["com.groundplane.kind"] != "service" ||
		ids.Validate(ids.KindProject, labels["com.groundplane.project-id"]) != nil ||
		labels["com.groundplane.service-id"] != serviceID || labels["com.groundplane.environment-id"] != artifact.OwnerId ||
		labels["com.groundplane.plan-id"] != planID ||
		labels["com.groundplane.render-generation"] != strconv.FormatUint(generation, 10) ||
		labels["com.groundplane.release-id"] != "" || labels["com.groundplane.slot"] != "" ||
		labels["com.groundplane.runtime-role"] != "" || labels["com.groundplane.component-id"] != "" ||
		labels["com.groundplane.tenant-id"] != "" {
		return nil, invalid()
	}
	var document struct {
		Services map[string]struct {
			Image string `yaml:"image"`
		} `yaml:"services"`
	}
	if yaml.Unmarshal(artifact.CanonicalYaml, &document) != nil ||
		document.Services[selected.ComposeName].Image != selected.ImageReference {
		return nil, invalid()
	}
	return selected, nil
}

// SelectWorkload recovers only identity already sealed into the artifact labels.
func SelectWorkload(artifact *agentpb.ComposeArtifact, serviceID string) (*agentpb.ComposeService, error) {
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() != serviceID {
			continue
		}
		planID, encodedGeneration := "", ""
		for _, pair := range service.GetExpectedLabels() {
			if pair.GetKey() == "com.groundplane.plan-id" {
				planID = pair.GetValue()
			}
			if pair.GetKey() == "com.groundplane.render-generation" {
				encodedGeneration = pair.GetValue()
			}
		}
		generation, err := strconv.ParseUint(encodedGeneration, 10, 64)
		if err != nil || strconv.FormatUint(generation, 10) != encodedGeneration {
			return nil, invalid()
		}
		return Workload(artifact, serviceID, planID, generation)
	}
	return nil, invalid()
}

func MatchesObservation(value Observation, artifact *agentpb.ComposeArtifact, workload *agentpb.ComposeService) bool {
	if ValidateObservation(value) != nil || value.ProjectName != artifact.ProjectName ||
		value.ServiceID != workload.ServiceId ||
		value.ImageReference != workload.ImageReference ||
		value.State != agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING ||
		workload.HasHealthcheck && value.Health != agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY {
		return false
	}
	labels := make(map[string]string, len(value.Labels))
	for _, pair := range value.Labels {
		labels[pair.Key] = pair.Value
	}
	// The Compose observer verifies Docker's project/service labels before
	// emitting typed identity; its report contains only sealed ownership labels.
	for _, pair := range workload.ExpectedLabels {
		if labels[pair.Key] != pair.Value {
			return false
		}
	}
	for _, key := range []string{"com.groundplane.release-id", "com.groundplane.slot", "com.groundplane.runtime-role", "com.groundplane.component-id", "com.groundplane.tenant-id"} {
		if labels[key] != "" {
			return false
		}
	}
	return true
}

func invalid() error {
	return errs.New(errs.KindStateConflict, "native Backing runtime evidence is invalid")
}
