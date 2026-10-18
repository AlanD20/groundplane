package volume

import (
	"crypto/sha256"
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// Rationale: native removal retains the exact accepted mount configuration and
// consumer set. Caller mutation cannot change that authority after capture.
func TestNativeVolumeRemovalCapturesMountsAndRejectsUnapprovedConsumer(t *testing.T) {
	tenantID, projectID, environment, projection := volumePolicyProjectionFixture(t)
	selected := projection.Volumes[0]
	baseline := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(projection.ComposeArtifact, baseline); err != nil {
		t.Fatal(err)
	}
	serviceID := ids.NewAt(ids.KindService, environment.CreatedAt, 50)
	priorPlanID := ids.NewAt(ids.KindPlan, environment.CreatedAt, 56)
	baseline.Services = []*agentpb.ComposeService{
		{ServiceId: serviceID, ComposeName: "api", ExpectedLabels: []*agentpb.LabelPair{
			{
				Key:   "com.groundplane.plan-id",
				Value: priorPlanID,
			}, {Key: "com.groundplane.render-generation", Value: "2"},
		}},
	}
	var document, services yaml.Node
	if err := yaml.Unmarshal(baseline.CanonicalYaml, &document); err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal([]byte("api:\n  image: alpine:latest\n  volumes: [data:/a, data:/z:ro, scratch:/unrelated]\n  labels:\n    com.groundplane.plan-id: "+priorPlanID+"\n    com.groundplane.render-generation: '2'\n"), &services); err != nil {
		t.Fatal(err)
	}
	root := document.Content[0]
	for index := 0; index < len(root.Content); index += 2 {
		if root.Content[index].Value == "services" {
			root.Content[index+1] = services.Content[0]
		}
	}
	var err error
	baseline.CanonicalYaml, err = yaml.Marshal(&document)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(baseline.CanonicalYaml)
	baseline.YamlSha256 = digest[:]
	candidate, err := composerender.MutateEnvironmentVolumeArtifact(baseline, composerender.VolumeArtifactMutation{
		Action: composerender.VolumeArtifactRemove, VolumeID: selected.ID, Key: selected.Key,
		ArtifactID: ids.NewAt(
			ids.KindConfig,
			environment.CreatedAt,
			51,
		), PlanID: ids.NewAt(ids.KindPlan, environment.CreatedAt, 52),
		TenantID: tenantID, ProjectID: projectID, RenderGeneration: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	before := proto.CloneOf(baseline)
	request := volumeMutationRequest{action: volumeMutationActionRemove, environmentID: environment.ID,
		volumeID: selected.ID, key: selected.Key, slug: selected.Slug, impactToken: strings.Repeat("a", 64)}
	scope := workexecution.Scope{TenantID: tenantID, ProjectID: projectID, EnvironmentID: environment.ID}
	placement := workexecution.Placement{
		Executor: workexecution.Agent,
		HostID:   ids.NewAt(ids.KindHost, environment.CreatedAt, 53),
	}
	claim := volumeWorkTestClaim(ids.NewAt(ids.KindTask, environment.CreatedAt, 54), environment.CreatedAt)
	if _, _, _, err := buildVolumeWork(scope, placement, claim, request, baseline, candidate, make([]byte, 32), nil); err == nil {
		t.Fatal("mounted workload was removed without its captured consumer approval")
	}
	consumers := []string{serviceID}
	_, _, input, err := buildVolumeWork(
		scope,
		placement,
		claim,
		request,
		baseline,
		candidate,
		make([]byte, 32),
		consumers,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(baseline, before) || !proto.Equal(input.Baseline, before) {
		t.Fatal("capture changed the source mount configuration")
	}
	baseline.CanonicalYaml[0] ^= 0xff
	baseline.Services[0].ComposeName = "changed"
	consumers[0] = ids.NewAt(ids.KindService, environment.CreatedAt, 55)
	if err := executionplan.ValidateVolumeWorkInput(input); err != nil {
		t.Fatal(err)
	}
	names, err := (volumeexecution.Removal{
		VolumeID: input.VolumeId, Key: input.ComposeKey, Baseline: input.Baseline,
		Candidate: input.Candidate, ConsumerIDs: input.ConsumerIds,
	}).Consumers()
	if err != nil || !slices.Equal(names, []string{"api"}) || !proto.Equal(input.Baseline, before) {
		t.Fatalf("captured removal changed after caller mutation: consumers=%v, %v", names, err)
	}
}
