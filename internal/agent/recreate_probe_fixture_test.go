package agent

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// The fixture seals an ordinary Release with explicit historical ownership.
// Its applied witness happens to equal the selected native predecessor; it is
// not a fallback selector or a relabeled current-plan artifact.
func sealedRecreateProbeAssignment(
	t *testing.T,
	replicas uint32,
	priorTarget string,
) (Assignment, *agentpb.ExecutionStep) {
	t.Helper()
	now := time.Date(2026, time.September, 12, 12, 0, 0, 0, time.UTC)
	serviceID := ids.NewAt(ids.KindService, now, 1)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 2)
	planID, priorPlanID := ids.NewAt(ids.KindPlan, now, 3), ids.NewAt(ids.KindPlan, now, 4)
	candidateRelease, priorRelease := ids.NewAt(ids.KindDeployment, now, 5), ids.NewAt(ids.KindDeployment, now, 6)
	networkID := ids.NewAt(ids.KindNetwork, now, 16)
	candidate := recreateProbeArtifact(ids.NewAt(ids.KindConfig, now, 7), environmentID, serviceID,
		candidateRelease, planID, networkID, 2, replicas, "singleton")
	prior := recreateProbeArtifact(ids.NewAt(ids.KindConfig, now, 8), environmentID, serviceID,
		priorRelease, priorPlanID, networkID, 1, replicas, priorTarget)
	applyID, acknowledgeID := ids.NewAt(ids.KindStep, now, 9), ids.NewAt(ids.KindStep, now, 10)
	probeID, compensateID := ids.NewAt(ids.KindStep, now, 11), ids.NewAt(ids.KindStep, now, 12)
	probe := &agentpb.ExecutionStep{
		StepId: probeID, TimeoutSeconds: 30,
		Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
		Payload: &agentpb.ExecutionStep_ServiceRecreateProbe{ServiceRecreateProbe: &agentpb.ServiceRecreateProbe{
			CandidateArtifactId: candidate.ArtifactId, PriorArtifactId: prior.ArtifactId,
			ServiceId: serviceID, CandidateReleaseId: candidateRelease, PriorReleaseId: priorRelease,
		}},
	}
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: planID, RenderGeneration: 2,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, TargetId: serviceID,
		Artifacts: []*agentpb.ComposeArtifact{candidate, prior},
		CandidateReleaseProcedure: &agentpb.CandidateReleaseProcedure{Members: []*agentpb.CandidateReleaseMember{{
			ServiceId: serviceID, CandidateReleaseId: candidateRelease, CandidateArtifactId: candidate.ArtifactId,
			ForwardStepIds: []string{applyID, acknowledgeID},
			ServingPredecessor: &agentpb.ServingPredecessorRestoration{
				ProbeStepId: probeID, CompensateStepId: compensateID,
				PriorArtifactId: prior.ArtifactId, PriorReleaseId: priorRelease, PriorTarget: priorTarget,
			},
		}}},
		Steps: []*agentpb.ExecutionStep{
			{StepId: applyID, TimeoutSeconds: 30,
				Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
				Payload: &agentpb.ExecutionStep_ComposeWorkloadApply{
					ComposeWorkloadApply: &agentpb.ComposeWorkloadApply{
						ArtifactId: candidate.ArtifactId, ServiceId: serviceID, Target: "singleton",
					},
				}},
			{StepId: acknowledgeID, TimeoutSeconds: 30, PrerequisiteStepId: applyID,
				Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
				Payload: &agentpb.ExecutionStep_ServiceRecreateAcknowledge{
					ServiceRecreateAcknowledge: &agentpb.ServiceRecreateAcknowledge{
						ArtifactId: candidate.ArtifactId, ServiceId: serviceID, ReleaseId: candidateRelease,
					},
				}},
			probe,
			{StepId: compensateID, TimeoutSeconds: 30, PrerequisiteStepId: applyID,
				Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
				Payload: &agentpb.ExecutionStep_ServiceRecreateCompensate{
					ServiceRecreateCompensate: &agentpb.ServiceRecreateCompensate{
						CandidateArtifactId: candidate.ArtifactId, ArtifactId: prior.ArtifactId, ServiceId: serviceID,
						CandidateReleaseId: candidateRelease, PriorReleaseId: priorRelease, PriorTarget: priorTarget, Enabled: true,
					},
				}},
		},
	})
	if err != nil {
		t.Fatalf("seal recreate probe plan: %v", err)
	}
	witness := marshalNativeAssignmentArtifact(t, prior)
	assignment := Assignment{
		TaskID: ids.NewAt(ids.KindTask, now, 13), OperationID: ids.NewAt(ids.KindOperation, now, 14),
		ExecutionMode: agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD, Plan: plan,
	}
	assignment.RestorationAuthority = &agentpb.ReleaseRestorationAuthority{
		TaskId: assignment.TaskID, OperationId: assignment.OperationID, PlanHash: plan.PlanHash,
		EnvironmentId: environmentID, CandidateArtifactId: candidate.ArtifactId,
		Candidates: []*agentpb.ReleaseRestorationCandidate{{ServiceId: serviceID, ReleaseId: candidateRelease,
			Target: agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR}},
		AppliedPredecessor: &agentpb.ReleaseAppliedPredecessorAuthority{
			KeyRevision: 17, RevisionId: ids.NewAt(ids.KindTask, now, 15), RenderGeneration: 1,
		},
		NativePredecessors: []*agentpb.ReleaseNativePredecessorAuthority{
			{ServiceId: serviceID, CurrentArtifact: witness},
		},
	}
	sealAssignmentWitness(assignment.RestorationAuthority, witness)
	// The Controller's digest is opaque to the Agent; only the plan and witness
	// hashes are computed and independently checked at this unit-test boundary.
	digest := sha256.Sum256([]byte("controller-owned recreate fixture authority"))
	assignment.RestorationAuthority.AuthoritySha256 = digest[:]
	if err := validateCandidateReleaseAssignmentAuthority(assignment, plan); err != nil {
		t.Fatalf("validate recreate probe assignment: %v", err)
	}
	return assignment, plan.Steps[2]
}

func recreateProbeArtifact(
	artifactID, environmentID, serviceID, releaseID, planID, networkID string,
	generation uint64, replicas uint32, target string,
) *agentpb.ComposeArtifact {
	artifact := recreateTestArtifact(artifactID, releaseID, replicas)
	artifact.OwnerKind, artifact.OwnerId = agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT, environmentID
	artifact.ProjectName = "gp-" + strings.ToLower(environmentID)
	artifact.AuthorizedVolumeDir = "/var/lib/groundplane/volumes/" + environmentID
	artifact.Networks = []*agentpb.ComposeNetwork{{
		NetworkId: networkID, ComposeName: "frontend", DockerName: "gp_net_" + networkID,
		ExpectedLabels: []*agentpb.LabelPair{
			{Key: "com.groundplane.environment-id", Value: environmentID},
			{Key: "com.groundplane.kind", Value: "network"},
			{Key: "com.groundplane.managed", Value: "true"},
		},
	}}
	service := artifact.Services[0]
	service.ServiceId = serviceID
	service.ExpectedLabels = append(service.ExpectedLabels,
		&agentpb.LabelPair{Key: "com.groundplane.environment-id", Value: environmentID},
		&agentpb.LabelPair{Key: "com.groundplane.kind", Value: "service"},
		&agentpb.LabelPair{Key: "com.groundplane.managed", Value: "true"},
		&agentpb.LabelPair{Key: "com.groundplane.plan-id", Value: planID},
		&agentpb.LabelPair{Key: "com.groundplane.render-generation", Value: strconv.FormatUint(generation, 10)},
		&agentpb.LabelPair{Key: "com.groundplane.service-id", Value: serviceID},
	)
	if target != "singleton" {
		service.Role, service.Slot = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT, target
		service.ComposeName = "api-" + target
		service.ExpectedLabels[1].Value = "slot"
		service.ExpectedLabels = append(
			service.ExpectedLabels,
			&agentpb.LabelPair{Key: "com.groundplane.slot", Value: target},
		)
	}
	sort.Slice(
		service.ExpectedLabels,
		func(i, j int) bool { return service.ExpectedLabels[i].Key < service.ExpectedLabels[j].Key },
	)
	yaml := fmt.Sprintf(
		"services:\n  %s:\n    image: %s\n    deploy:\n      replicas: %d\n    healthcheck:\n      test: [CMD, \"true\"]\n    labels:\n",
		service.ComposeName,
		service.ImageReference,
		replicas,
	)
	for _, label := range service.ExpectedLabels {
		yaml += fmt.Sprintf("      %s: %q\n", label.Key, label.Value)
	}
	yaml += "    networks: [frontend]\nnetworks:\n  frontend:\n    external: true\n    name: gp_net_" + networkID + "\n"
	artifact.CanonicalYaml = []byte(yaml)
	digest := sha256.Sum256(artifact.CanonicalYaml)
	artifact.YamlSha256 = digest[:]
	return artifact
}
