package composehelper

import (
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: the resource-only helper must create a sealed bind volume without
// invoking Compose or starting any service before the global Script barrier.
func TestManagedVolumeEnsureExecutesOnlySealedResource(t *testing.T) {
	request := volumeEnsureRequest(t)
	fake := &volumePreparationRunner{}
	response, err := Execute(t.Context(), fake, request)
	if err != nil || response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED {
		t.Fatalf("response=%v error=%v", response, err)
	}
	if fake.creates != 1 || fake.volume.Options["device"] != request.Plan.Artifacts[0].AuthorizedVolumeDir+"/data" {
		t.Fatalf("wrong resource effect: %+v", fake)
	}
	if fake.volume.Labels["operator-label"] != "preserved" {
		t.Fatal("resource preparation dropped a rendered authored label")
	}
	response, err = Execute(t.Context(), fake, request)
	if err != nil || response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED ||
		fake.creates != 1 {
		t.Fatalf("retry mutated an existing owned volume: response=%v error=%v creates=%d", response, err, fake.creates)
	}
}

// Rationale: the exact physical bind, not a matching resource name, is the
// postcondition; existing data must remain untouched when that check fails.
func TestManagedVolumeEnsureNeverRecreatesDivergentStorage(t *testing.T) {
	for _, name := range []string{"driver", "device", "default-volume"} {
		t.Run(name, func(t *testing.T) {
			request := volumeEnsureRequest(t)
			fake := &volumePreparationRunner{}
			if _, err := Execute(t.Context(), fake, request); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "driver":
				fake.volume.Driver = "foreign"
			case "device":
				fake.volume.Options["device"] = "/elsewhere"
			case "default-volume":
				fake.volume.Options = nil
				fake.volume.Labels = nil
			}
			response, err := Execute(t.Context(), fake, request)
			if err == nil && response.GetOutcome() == agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED ||
				fake.creates != 1 {
				t.Fatalf(
					"divergent storage accepted or recreated: response=%v error=%v creates=%d",
					response,
					err,
					fake.creates,
				)
			}
		})
	}
}

// Rationale: rehashing divergent YAML must not turn declared ownership into
// authority to create an external, foreign, or differently backed volume.
func TestManagedVolumeEnsureRejectsDivergentRenderedAuthority(t *testing.T) {
	for _, name := range []string{"external", "driver", "device", "owner", "missing", "docker-name"} {
		t.Run(name, func(t *testing.T) {
			request := volumeEnsureRequest(t)
			artifact := request.Plan.Artifacts[0]
			content := string(artifact.CanonicalYaml)
			switch name {
			case "external":
				content += "    external: true\n"
			case "driver":
				content = strings.Replace(content, "driver: local", "driver: foreign", 1)
			case "device":
				content = strings.Replace(content, artifact.AuthorizedVolumeDir+"/data", "/elsewhere", 1)
			case "owner":
				content = strings.Replace(
					content,
					"com.groundplane.managed: 'true'",
					"com.groundplane.managed: 'false'",
					1,
				)
			case "missing":
				content = "volumes: {}\n"
			case "docker-name":
				content = strings.Replace(content, artifact.Volumes[0].DockerName, "foreign", 1)
			}
			artifact.CanonicalYaml = []byte(content)
			hash := sha256.Sum256(artifact.CanonicalYaml)
			artifact.YamlSha256 = hash[:]
			request.Plan.PlanHash = nil
			var err error
			request.Plan, err = executionplan.Seal(request.Plan)
			if err != nil {
				t.Fatal(err)
			}
			fake := &volumePreparationRunner{}
			if _, err := Execute(t.Context(), fake, request); err == nil || fake.creates != 0 {
				t.Fatalf("divergent authority reached mutation: error=%v creates=%d", err, fake.creates)
			}
		})
	}
}

func volumeEnsureRequest(t *testing.T) *agentpb.ComposeHelperRequest {
	t.Helper()
	request, _ := networkEnsureRequest(t)
	request.Plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
	artifact := request.Plan.Artifacts[0]
	const id = "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	name := "gp_vol_" + strings.ToLower(id)
	artifact.Volumes = []*agentpb.ComposeVolume{{VolumeId: id, ComposeName: "data", DockerName: name,
		ExpectedLabels: []*agentpb.LabelPair{{Key: "com.groundplane.environment-id", Value: helperEnvironmentID},
			{Key: "com.groundplane.kind", Value: "volume"}, {Key: "com.groundplane.managed", Value: "true"}}}}
	artifact.CanonicalYaml = []byte(
		"volumes:\n  data:\n    name: " + name + "\n    driver: local\n    driver_opts:\n      type: none\n      o: bind\n      device: " + artifact.AuthorizedVolumeDir + "/data\n    labels:\n      com.groundplane.environment-id: " + helperEnvironmentID + "\n      com.groundplane.kind: volume\n      com.groundplane.managed: 'true'\n",
	)
	artifact.CanonicalYaml = append(artifact.CanonicalYaml, []byte("      operator-label: preserved\n")...)
	hash := sha256.Sum256(artifact.CanonicalYaml)
	artifact.YamlSha256 = hash[:]
	request.Plan.Steps[0].Payload = &agentpb.ExecutionStep_ManagedVolumeEnsure{
		ManagedVolumeEnsure: &agentpb.ManagedVolumeEnsure{ArtifactId: artifact.ArtifactId, VolumeId: id},
	}
	request.Plan.PlanHash = nil
	var err error
	request.Plan, err = executionplan.Seal(request.Plan)
	if err != nil {
		t.Fatal(err)
	}
	return request
}
