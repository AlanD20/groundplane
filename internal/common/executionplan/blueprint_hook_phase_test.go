package executionplan

import (
	"crypto/sha256"
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: an initial pre-deploy Script uses the prepublished workload ID
// before any candidate exists, and must not depend on a post-start image ACK.
func TestSealBlueprintPreHookBeforeCandidateApply(t *testing.T) {
	sealed, err := Seal(blueprintPreHookPlan(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Validate(sealed); err != nil {
		t.Fatal(err)
	}
	if sealed.Steps[0].GetRunScript() == nil || sealed.Steps[1].GetComposeApply() == nil {
		t.Fatal("pre-hook did not precede candidate apply")
	}
}

// Rationale: new prehooks retain candidate identity and phase authorization
// even after all nested hashes are refreshed to match malicious input.
func TestSealBlueprintPreHookRejectsForgedCandidateAndPhase(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*agentpb.ExecutionPlan)
	}{
		{"after apply", func(p *agentpb.ExecutionPlan) {
			p.Steps[0], p.Steps[1] = p.Steps[1], p.Steps[0]
			chainBlueprintForwardForTest(p)
		}},
		{"failure phase", func(p *agentpb.ExecutionPlan) {
			p.Steps[0].Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FAILURE_HOOK
		}},
		{"different sealed image", func(p *agentpb.ExecutionPlan) {
			p.ScriptRunnerSnapshots[0].LocalImageId = "sha256:" + strings.Repeat("b", 64)
			p.ScriptRunnerProjections[0].Image = p.ScriptRunnerSnapshots[0].LocalImageId
			refreshScriptProjectionImageDigestForTest(t, p)
		}},
		{"different Release", func(p *agentpb.ExecutionPlan) {
			p.ScriptRunnerSnapshots[0].ReleaseId = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAZ"
		}},
		{"different Service", func(p *agentpb.ExecutionPlan) {
			p.ScriptRunnerSnapshots[0].ServiceId = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAZ"
		}},
		{"decoy artifact", func(p *agentpb.ExecutionPlan) {
			decoy := proto.CloneOf(p.Artifacts[0])
			decoy.ArtifactId = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAZ"
			decoy.Services = nil
			p.Artifacts = append(p.Artifacts, decoy)
			p.Steps[1].GetComposeApply().ArtifactId = decoy.ArtifactId
		}},
		{"ambiguous apply", func(p *agentpb.ExecutionPlan) {
			duplicate := proto.CloneOf(p.Steps[1])
			duplicate.StepId = "step_01ARZ3NDEKTSV4RRFFQ69G5FC1"
			p.Steps = append(p.Steps[:2], append([]*agentpb.ExecutionStep{duplicate}, p.Steps[2:]...)...)
			chainBlueprintForwardForTest(p)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := blueprintPreHookPlan(t)
			if _, err := Seal(plan); err != nil {
				t.Fatalf("invalid baseline: %v", err)
			}
			test.mutate(plan)
			refreshBlueprintSnapshotDigestForTest(t, plan)
			if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("forged prehook accepted: %v", err)
			}
		})
	}
}

// Rationale: a posthook's preceding apply may belong to another Service, but
// every candidate must be applied before any posthook and before health.
func TestSealBlueprintGlobalHookBarrierAcrossServices(t *testing.T) {
	plan := blueprintTwoServicePostHookPlan(t)
	if _, err := Seal(plan); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		order []int
	}{
		{"post before other apply", []int{0, 2, 1, 3, 4}},
		{"health before post", []int{0, 1, 3, 2, 4}},
		{"health before other apply", []int{0, 3, 1, 2, 4}},
	} {
		t.Run(test.name, func(t *testing.T) {
			forged := proto.CloneOf(plan)
			for index, source := range test.order {
				forged.Steps[index] = proto.CloneOf(plan.Steps[source])
			}
			chainBlueprintForwardForTest(forged)
			if _, err := Seal(forged); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("global barrier bypass accepted: %v", err)
			}
		})
	}
}

// Rationale: a prehook cannot be delayed until after a different Service has
// started, even when its own matching candidate apply is still in the future.
func TestSealBlueprintPreHookBlocksEveryCandidate(t *testing.T) {
	plan := blueprintTwoServicePostHookPlan(t)
	apply, otherApply, run := plan.Steps[0], plan.Steps[1], plan.Steps[2]
	run.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_PRE_HOOK
	plan.Steps[0], plan.Steps[1], plan.Steps[2] = run, apply, otherApply
	chainBlueprintForwardForTest(plan)
	if _, err := Seal(plan); err != nil {
		t.Fatal(err)
	}
	plan.Steps[0], plan.Steps[1], plan.Steps[2] = otherApply, run, apply
	chainBlueprintForwardForTest(plan)
	if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("another candidate started before prehook: %v", err)
	}
}

func chainBlueprintForwardForTest(plan *agentpb.ExecutionPlan) {
	previous := ""
	for _, step := range plan.Steps {
		if step.GetCandidateRestorationProbe() != nil || step.GetCandidateRestorationCompensate() != nil {
			continue
		}
		step.PrerequisiteStepId = previous
		previous = step.StepId
	}
}

func blueprintTwoServicePostHookPlan(t *testing.T) *agentpb.ExecutionPlan {
	t.Helper()
	plan := validBlueprintScriptReconcilePlan(t)
	serviceID, releaseID := "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW", "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	service := proto.CloneOf(plan.Artifacts[0].Services[0])
	service.ServiceId, service.ComposeName = serviceID, "worker"
	for _, label := range service.ExpectedLabels {
		if label.Key == labelServiceID {
			label.Value = serviceID
		}
		if label.Key == labelReleaseID {
			label.Value = releaseID
		}
	}
	plan.Artifacts[0].Services = append(plan.Artifacts[0].Services, service)
	plan.Artifacts[0].CanonicalYaml = append(plan.Artifacts[0].CanonicalYaml,
		[]byte("  worker:\n    image: "+service.ImageReference+"\n")...)
	digest := sha256.Sum256(plan.Artifacts[0].CanonicalYaml)
	plan.Artifacts[0].YamlSha256 = digest[:]
	apply, health, probe, compensate := proto.CloneOf(
		plan.Steps[0],
	), proto.CloneOf(
		plan.Steps[2],
	), proto.CloneOf(
		plan.Steps[3],
	), proto.CloneOf(
		plan.Steps[4],
	)
	apply.StepId = "step_01ARZ3NDEKTSV4RRFFQ69G5FC1"
	apply.GetComposeApply().ServiceIds = []string{serviceID}
	health.StepId = "step_01ARZ3NDEKTSV4RRFFQ69G5FC2"
	health.GetWaitHealthy().ServiceIds = []string{serviceID}
	probe.StepId = "step_01ARZ3NDEKTSV4RRFFQ69G5FC3"
	probe.GetCandidateRestorationProbe().ServiceId = serviceID
	probe.GetCandidateRestorationProbe().CandidateReleaseId = releaseID
	compensate.StepId = "step_01ARZ3NDEKTSV4RRFFQ69G5FC4"
	compensate.GetCandidateRestorationCompensate().ServiceId = serviceID
	compensate.GetCandidateRestorationCompensate().CandidateReleaseId = releaseID
	compensate.PrerequisiteStepId = apply.StepId
	member := plan.CandidateReleaseProcedure.Members[0]
	member.CandidateAbsence.Services = append(
		member.CandidateAbsence.Services,
		&agentpb.CandidateReleaseService{ServiceId: serviceID, ReleaseId: releaseID},
	)
	other := proto.CloneOf(member)
	other.ServiceId, other.CandidateReleaseId = serviceID, releaseID
	other.ForwardStepIds = []string{apply.StepId, health.StepId}
	other.ServingPredecessor.ProbeStepId, other.ServingPredecessor.CompensateStepId = probe.StepId, compensate.StepId
	other.CandidateAbsence.ProbeStepId, other.CandidateAbsence.CompensateStepId = probe.StepId, compensate.StepId
	plan.CandidateReleaseProcedure.Members = append(plan.CandidateReleaseProcedure.Members, other)
	plan.Steps = []*agentpb.ExecutionStep{
		plan.Steps[0],
		apply,
		plan.Steps[1],
		plan.Steps[2],
		health,
		plan.Steps[3],
		plan.Steps[4],
		probe,
		compensate,
	}
	chainBlueprintForwardForTest(plan)
	return plan
}

func blueprintPreHookPlan(t *testing.T) *agentpb.ExecutionPlan {
	t.Helper()
	plan := validBlueprintScriptReconcilePlan(t)
	apply, run, health := plan.Steps[0], plan.Steps[1], plan.Steps[2]
	run.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_PRE_HOOK
	run.PrerequisiteStepId = ""
	apply.PrerequisiteStepId = run.StepId
	health.PrerequisiteStepId = apply.StepId
	plan.Steps[0], plan.Steps[1] = run, apply
	return plan
}
