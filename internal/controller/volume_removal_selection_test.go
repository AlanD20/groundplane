package controller

import (
	"bytes"
	"crypto/sha256"
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// Rationale: a logical consumer includes a stable proxy and retained slot. Only
// the actual mounted workload may be started, with no dependency expansion; a
// desired mount that was never deployed must not become an unqualified up.
func TestVolumeRemovalSelectsOnlyMountedNativeWorkload(t *testing.T) {
	for _, mounted := range []bool{true, false} {
		request, workload := nativeVolumeRemovalRequest(t, mounted)
		services, err := composehelper.StartupServices(request)
		if err != nil {
			t.Fatal(err)
		}
		fake := runner.NewFake()
		if _, err := composehelper.Execute(t.Context(), fake, request); err != nil {
			t.Fatal(err)
		}
		calls := fake.RecordedCalls()
		if !mounted {
			if len(services) != 0 || len(calls) != 0 {
				t.Fatal("undeployed desired mount started workloads")
			}
			continue
		}
		if len(services) != 1 || services[0].ComposeName != workload || len(calls) != 2 {
			t.Fatalf("startup=%v calls=%v", services, calls)
		}
		args := calls[1].Args
		up := slices.Index(args, "up")
		if up < 0 || !slices.Equal(args[up:], []string{"up", "--detach", "--no-deps", workload}) {
			t.Fatalf("removal command selected a proxy/dependency: %v", args)
		}
		if !bytes.Equal(calls[1].Stdin, request.Plan.Artifacts[0].CanonicalYaml) {
			t.Fatal("helper did not execute the sealed candidate")
		}
	}
}

// Rationale: no-deps is not general removal authority; both immutable artifacts
// and the ordered single-Volume cleanup steps must agree before admission.
func TestVolumeRemovalRejectsChangedConsumerAuthority(t *testing.T) {
	request, _ := nativeVolumeRemovalRequest(t, true)
	for _, change := range []string{"baseline", "target", "steps", "mount", "metadata", "dependencies"} {
		t.Run(change, func(t *testing.T) {
			plan := proto.CloneOf(request.Plan)
			plan.PlanHash = nil
			switch change {
			case "baseline":
				plan.Steps[2].GetManagedVolumeDirectoryRemove().ArtifactId = plan.Artifacts[0].ArtifactId
			case "target":
				plan.Steps[1].GetManagedVolumeRemove().VolumeId = "vol_01ARZ3NDEKTSV4RRFFQ69G5FC9"
			case "steps":
				plan.Steps[1], plan.Steps[2] = plan.Steps[2], plan.Steps[1]
			case "mount":
				plan.Artifacts[0].CanonicalYaml = append([]byte(nil), plan.Artifacts[1].CanonicalYaml...)
				digest := sha256.Sum256(plan.Artifacts[0].CanonicalYaml)
				plan.Artifacts[0].YamlSha256 = digest[:]
			case "metadata":
				plan.Artifacts[0].Services[0].ExpectedReplicas++
			case "dependencies":
				plan.Steps[0].GetComposeApply().FullReconcile = true
			}
			if _, err := executionplan.Seal(plan); err == nil {
				t.Fatal("accepted changed removal authority")
			}
		})
	}
}

func nativeVolumeRemovalRequest(t *testing.T, mounted bool) (*agentpb.ComposeHelperRequest, string) {
	t.Helper()
	_, _, input := redeployRestorationInput(t, domain.StrategyBlueGreen)
	render := input.Members[0].Render
	baseline := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(render.PriorRuntime.CurrentArtifact, baseline); err != nil {
		t.Fatal(err)
	}
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
	services := root.Content[mappingIndex(root, "services")+1]
	service := services.Content[mappingIndex(services, workload)+1]
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
	candidate, err := MutateEnvironmentVolumeArtifact(baseline, VolumeArtifactMutation{Action: VolumeArtifactRemove,
		VolumeID: volume.VolumeId, Key: volume.ComposeName, ArtifactID: artifactID, PlanID: planID,
		TenantID: render.TenantID, ProjectID: render.ProjectID, RenderGeneration: 3})
	if err != nil {
		t.Fatal(err)
	}
	steps := []*agentpb.ExecutionStep{
		{StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FC6", TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
				ArtifactId: artifactID, ServiceIds: []string{render.ServiceID}, NoDependencies: true,
			}}},
		{StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FC7", TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ManagedVolumeRemove{ManagedVolumeRemove: &agentpb.ManagedVolumeRemove{
				VolumeId: volume.VolumeId, DockerName: "gp_vol_" + strings.ToLower(volume.VolumeId),
			}}},
		{StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FC8", TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoryRemove{
				ManagedVolumeDirectoryRemove: &agentpb.ManagedVolumeDirectoryRemove{
					ArtifactId: baseline.ArtifactId, VolumeId: volume.VolumeId, ComposeKey: volume.ComposeName,
					IntentSha256: bytes.Repeat([]byte{1}, 32),
				}}},
	}
	plan, err := BuildPlan(PlanBuildInput{VolumeRoot: "/var/lib/groundplane/vol", PlanID: planID,
		RenderGeneration: 3, Operation: agentpb.PlanOperation_PLAN_OPERATION_REMOVE, TargetID: volume.VolumeId,
		Artifacts: []*agentpb.ComposeArtifact{candidate, baseline}, Steps: steps})
	if err != nil {
		t.Fatal(err)
	}
	return &agentpb.ComposeHelperRequest{Schema: composehelper.SchemaVersion, Plan: plan, StepId: steps[0].StepId,
		TimeoutSeconds: 30, AssignmentId: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		TaskId: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", OperationId: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"}, workload
}
