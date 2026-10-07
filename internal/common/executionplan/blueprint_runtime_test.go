package executionplan

import (
	"bytes"
	"crypto/sha256"
	"sort"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// BP-04: a Blueprint blue-green receipt must retain the actual proxy ownership,
// switched destination and old slot, not record an unexecuted proxy recreation.
func TestBlueprintRuntimeRecordsBlueGreenActivation(t *testing.T) {
	for _, withPrior := range []bool{false, true} {
		plan := candidateRuntimeBlueGreenPlan(t, withPrior)
		plan.PlanHash = nil
		plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
		plan.TargetId = candidateRuntimeEnvironmentID
		for _, artifact := range plan.Artifacts {
			for _, service := range artifact.Services {
				if service.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
					continue
				}
				service.ImageRepository = "docker.io/library/caddy"
				service.ImageIndexDigest = bytes.Repeat([]byte{0xaa}, 32)
				service.ImageChildDigest = bytes.Repeat([]byte{0xbb}, 32)
				service.ImageConfigDigest = bytes.Repeat([]byte{0xcc}, 32)
				service.ImageOs, service.ImageArchitecture = "linux", "amd64"
				service.ImageReference = service.ImageRepository + "@sha256:" + strings.Repeat("b", 64)
				service.ExpectedLabels = append(service.ExpectedLabels,
					&agentpb.LabelPair{Key: labelImageIndexDigest, Value: "sha256:" + strings.Repeat("a", 64)},
					&agentpb.LabelPair{Key: labelImageChildDigest, Value: "sha256:" + strings.Repeat("b", 64)},
					&agentpb.LabelPair{Key: labelImageConfigDigest, Value: "sha256:" + strings.Repeat("c", 64)},
					&agentpb.LabelPair{Key: labelImagePlatform, Value: "linux/amd64"})
				sort.Slice(
					service.ExpectedLabels,
					func(i, j int) bool { return service.ExpectedLabels[i].Key < service.ExpectedLabels[j].Key },
				)
				artifact.CanonicalYaml = bytes.ReplaceAll(
					artifact.CanonicalYaml,
					[]byte("proxy:sealed"),
					[]byte(service.ImageReference),
				)
				digest := sha256.Sum256(artifact.CanonicalYaml)
				artifact.YamlSha256 = digest[:]
			}
		}
		member := plan.CandidateReleaseProcedure.Members[0]
		probeID, compensateID := plan.Steps[3].StepId, plan.Steps[4].StepId
		if member.ServingPredecessor == nil {
			member.ServingPredecessor = &agentpb.ServingPredecessorRestoration{
				ProbeStepId: probeID, CompensateStepId: compensateID,
			}
		}
		member.CandidateAbsence = &agentpb.CandidateAbsenceRestoration{
			ComposeProjectName: plan.Artifacts[0].ProjectName,
			ProbeStepId:        probeID, CompensateStepId: compensateID,
			Services: []*agentpb.CandidateReleaseService{
				{ServiceId: member.ServiceId, ReleaseId: member.CandidateReleaseId},
			},
		}
		plan.Steps[3], plan.Steps[4] = candidateRuntimeAbsenceSteps(probeID, compensateID)[0], candidateRuntimeAbsenceSteps(probeID, compensateID)[1]
		sealed, err := Seal(plan)
		if err != nil {
			t.Fatal(err)
		}
		inputs, err := PrepareBlueprintRuntimeInputs(sealed, candidateRuntimeArtifactID)
		if err != nil || len(inputs) != 1 {
			t.Fatalf("prepare switched Blueprint runtime: %v", err)
		}
		encoded, err := marshalCandidateRuntimeArtifact(sealed.Artifacts[0])
		if err != nil {
			t.Fatal(err)
		}
		runtime, err := OpenBlueprintRuntime(encoded, inputs[0])
		if err != nil || runtime.Target != "blue" || runtime.ProxyGeneration != 7 ||
			!bytes.Equal(runtime.ProxyConfigSHA256, sealed.Steps[2].GetServiceProxySwitch().GetConfigSha256()) ||
			(len(runtime.RetainedPriorArtifact) != 0) != withPrior {
			t.Fatalf("post-switch runtime (prior=%t) = %#v, %v", withPrior, runtime, err)
		}
		current := candidateRuntimeOpenArtifact(t, runtime.CurrentArtifact)
		proxy := candidateRuntimeService(
			current,
			member.ServiceId,
			"",
			agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
		)
		source := sealed.Artifacts[0]
		if withPrior {
			source = sealed.Artifacts[1]
		}
		priorProxy := candidateRuntimeService(
			source,
			member.ServiceId,
			"",
			agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
		)
		if nativePredecessorReleaseID(proxy) != nativePredecessorReleaseID(priorProxy) {
			t.Fatal("switched proxy ownership was rewritten")
		}
	}
}
