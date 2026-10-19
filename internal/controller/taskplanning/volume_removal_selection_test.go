package taskplanning

import (
	"bytes"
	"crypto/sha256"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// VOL-03 / TASK-76: a logical consumer includes a stable proxy and serving slot.
// Native removal selects physical mounted workloads, not their proxy/dependency
// or an unmounted desired consumer. This checks capture, not actual detachment.
func TestVolumeRemovalSelectsOnlyMountedNativeWorkload(t *testing.T) {
	for _, mounted := range []bool{true, false} {
		input, workload := nativeVolumeRemovalInput(t, mounted)
		if err := executionplan.ValidateVolumeWorkInput(input); err != nil {
			t.Fatal(err)
		}
		services, err := (volumeexecution.Removal{VolumeID: input.VolumeId, Key: input.ComposeKey,
			Baseline: input.Baseline, Candidate: input.Candidate, ConsumerIDs: input.ConsumerIds}).Consumers()
		if err != nil {
			t.Fatal(err)
		}
		if !mounted {
			if len(services) != 0 {
				t.Fatal("undeployed desired mount started workloads")
			}
			continue
		}
		if !slices.Equal(services, []string{workload}) {
			t.Fatalf("removal selected proxy/dependency instead of mounted workload: %v", services)
		}
	}
}

// VOL-03 / TASK-76: both captured artifacts, selected consumers and immutable
// Volume identity must agree; removing a mount grants no unrelated runtime edit.
func TestVolumeRemovalRejectsChangedConsumerAuthority(t *testing.T) {
	input, _ := nativeVolumeRemovalInput(t, true)
	for _, change := range []string{"baseline", "target", "consumer", "mount", "metadata", "digest", "approval"} {
		t.Run(change, func(t *testing.T) {
			changed := proto.CloneOf(input)
			switch change {
			case "baseline":
				changed.Baseline = proto.CloneOf(changed.Candidate)
			case "target":
				changed.VolumeId = "vol_01ARZ3NDEKTSV4RRFFQ69G5FC9"
			case "consumer":
				changed.ConsumerIds = nil
			case "mount":
				changed.Candidate.CanonicalYaml = bytes.Clone(changed.Baseline.CanonicalYaml)
				digest := sha256.Sum256(changed.Candidate.CanonicalYaml)
				changed.Candidate.YamlSha256 = digest[:]
			case "metadata":
				changed.Candidate.Services[0].ExpectedReplicas++
			case "digest":
				changed.Baseline.YamlSha256[0] ^= 1
			case "approval":
				changed.RemovalImpactSha256 = nil
			}
			if err := executionplan.ValidateVolumeWorkInput(changed); err == nil {
				t.Fatal("accepted changed removal authority")
			}
		})
	}
}

func nativeVolumeRemovalInput(t *testing.T, mounted bool) (*agentpb.VolumeWorkInput, string) {
	t.Helper()
	render := nativeRetainedRenderFixture(t, domain.StrategyBlueGreen)
	baseline := nativeRuntimeFixture(t, render, true)
	volume := baseline.Volumes[0]
	var workload, proxy string
	for _, service := range baseline.Services {
		if service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT {
			workload = service.ComposeName
		} else {
			proxy = service.ComposeName
		}
	}
	if workload == "" || proxy == "" {
		t.Fatal("fixture has no retained native workload/proxy")
	}
	var document yaml.Node
	if err := yaml.Unmarshal(baseline.CanonicalYaml, &document); err != nil {
		t.Fatal(err)
	}
	root := document.Content[0]
	services := root.Content[testcomposerender.MappingIndex(root, "services")+1]
	service := services.Content[testcomposerender.MappingIndex(services, workload)+1]
	var additions yaml.Node
	content := "depends_on:\n  " + proxy + ":\n    condition: service_started\n"
	if mounted {
		content += "volumes:\n  - type: volume\n    source: " + volume.ComposeName + "\n    target: /data\n"
	}
	if err := yaml.Unmarshal([]byte(content), &additions); err != nil {
		t.Fatal(err)
	}
	service.Content = append(service.Content, additions.Content[0].Content...)
	var err error
	baseline.CanonicalYaml, err = yaml.Marshal(&document)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(baseline.CanonicalYaml)
	baseline.YamlSha256 = digest[:]
	const planID = "plan_01ARZ3NDEKTSV4RRFFQ69G5FC4"
	const artifactID = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FC5"
	candidate, err := testcomposerender.MutateEnvironmentVolumeArtifact(
		baseline,
		testcomposerender.VolumeArtifactMutation{Action: testcomposerender.VolumeArtifactRemove,
			VolumeID: volume.VolumeId, Key: volume.ComposeName, ArtifactID: artifactID, PlanID: planID,
			TenantID: render.TenantID, ProjectID: render.ProjectID, RenderGeneration: 3},
	)
	if err != nil {
		t.Fatal(err)
	}
	return &agentpb.VolumeWorkInput{Action: agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE,
		EnvironmentId: baseline.OwnerId, VolumeId: volume.VolumeId, ComposeKey: volume.ComposeName,
		IntentSha256: bytes.Repeat([]byte{1}, 32), RemovalImpactSha256: bytes.Repeat([]byte{2}, 32),
		Baseline: baseline, Candidate: candidate, ConsumerIds: []string{render.ServiceID}}, workload
}
