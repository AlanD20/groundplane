package materialization

import (
	bytes "bytes"
	sha256 "crypto/sha256"
	fmt "fmt"
	sort "sort"
	strconv "strconv"
	strings "strings"
	testing "testing"
	time "time"

	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	executionplan "github.com/AlanD20/groundplane/internal/common/executionplan"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	proto "google.golang.org/protobuf/proto"
)

func newConfigurationRecoveryFixture(t *testing.T) configurationRecoveryFixture {
	t.Helper()
	assignment, _ := sealedRecreateProbeAssignment(t, 1, "singleton")
	plan := proto.CloneOf(assignment.Plan)
	plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
	plan.CandidateReleaseProcedure.Members = nil
	at := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
	prior, next := []byte("prior pinned configuration\n"), []byte("candidate configuration\n")
	forward := recoveryMaterializationStep(
		plan.Artifacts[0], ids.NewAt(ids.KindStep, at, 21), ids.NewAt(ids.KindConfig, at, 22), next,
		agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
	)
	probe := recoveryMaterializationStep(
		plan.Artifacts[0], ids.NewAt(ids.KindStep, at, 23), ids.NewAt(ids.KindConfig, at, 24), prior,
		agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
	)
	compensate := recoveryMaterializationStep(
		plan.Artifacts[0], ids.NewAt(ids.KindStep, at, 25), ids.NewAt(ids.KindConfig, at, 26), prior,
		agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
	)
	compensate.PrerequisiteStepId = forward.StepId
	plan.CandidateReleaseProcedure.ConfigurationRestoration = &agentpb.ConfigurationRestoration{
		PriorSnapshotId: ids.NewAt(ids.KindConfig, at, 27), PriorSnapshotSha256: bytes.Repeat([]byte{0x41}, 32),
		Files: []*agentpb.ConfigurationFileRestoration{{
			ForwardStepId: forward.StepId, ProbeStepId: probe.StepId, CompensateStepId: compensate.StepId,
		}},
	}
	plan.Steps = append([]*agentpb.ExecutionStep{forward}, plan.Steps...)
	plan.Steps = append(plan.Steps, probe, compensate)
	assignment.Plan = plan
	assignment.RestorationAuthority.PlanHash = bytes.Clone(plan.PlanHash)
	assignment.AssignmentID = ids.NewAt(ids.KindAssignment, at, 28)
	assignment.ExecutionEpoch = 7
	assignment.ForwardDeadline = time.Now().Add(time.Minute)
	assignment.RecoveryDeadline = assignment.ForwardDeadline.Add(time.Minute)
	assignment.Deadline = assignment.ForwardDeadline
	return configurationRecoveryFixture{
		assignment: assignment, forward: plan.Steps[0],
		probe: plan.Steps[len(plan.Steps)-2], compensate: plan.Steps[len(plan.Steps)-1],
		prior: prior, next: next,
	}
}

type configurationRecoveryFixture struct {
	assignment testtaskassignment.Assignment
	forward    *agentpb.ExecutionStep
	probe      *agentpb.ExecutionStep
	compensate *agentpb.ExecutionStep
	prior      []byte
	next       []byte
}

// The fixture seals an ordinary Release with explicit historical ownership.
// Its applied witness happens to equal the selected native predecessor; it is
// not a fallback selector or a relabeled current-plan artifact.
func sealedRecreateProbeAssignment(
	t *testing.T,
	replicas uint32,
	priorTarget string,
) (testtaskassignment.Assignment, *agentpb.ExecutionStep) {
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
	assignment := testtaskassignment.Assignment{
		TaskID: ids.NewAt(ids.KindTask, now, 13), OperationID: ids.NewAt(ids.KindOperation, now, 14),
		ExecutionMode: agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD, Plan: plan,
	}
	assignment.RestorationAuthority = &agentpb.ReleaseRestorationAuthority{
		TaskId: assignment.TaskID, OperationId: assignment.OperationID, PlanHash: plan.PlanHash, EnvironmentId: environmentID, CandidateArtifactId: candidate.ArtifactId,
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

	digest := sha256.Sum256([]byte("controller-owned recreate fixture authority"))
	assignment.RestorationAuthority.AuthoritySha256 = digest[:]
	if err := testtaskassignment.ValidateCandidateReleaseAuthority(assignment, plan); err != nil {
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
func recreateTestArtifact(artifactID, releaseID string, replicas uint32) *agentpb.ComposeArtifact {
	return &agentpb.ComposeArtifact{
		ArtifactId: artifactID, ProjectName: "gp-project",
		Services: []*agentpb.ComposeService{{
			ServiceId: "svc_api", ComposeName: "api", ExpectedReplicas: replicas, HasHealthcheck: true,
			Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
			ImageReference: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.release-id", Value: releaseID},
				{Key: "com.groundplane.runtime-role", Value: "singleton"},
			},
		}},
	}
}

func marshalNativeAssignmentArtifact(t *testing.T, artifact *agentpb.ComposeArtifact) []byte {
	t.Helper()
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
func sealAssignmentWitness(authority *agentpb.ReleaseRestorationAuthority, encoded []byte) {
	digest := sha256.Sum256(encoded)
	authority.AppliedPredecessor.ComposeArtifact = encoded
	authority.AppliedPredecessor.ComposeArtifactSha256 = digest[:]
}

func recoveryMaterializationStep(
	artifact *agentpb.ComposeArtifact,
	stepID string,
	materializationID string,
	content []byte,
	policy agentpb.ExecutionStepPolicy,
) *agentpb.ExecutionStep {
	digest := sha256.Sum256(content)
	return &agentpb.ExecutionStep{
		StepId: stepID, TimeoutSeconds: 30, Policy: policy,
		Payload: &agentpb.ExecutionStep_MaterializeFile{MaterializeFile: &agentpb.MaterializeFile{
			ArtifactId: artifact.ArtifactId, MaterializationId: materializationID,
			EnvironmentId: artifact.OwnerId, Destination: "config/application.yaml",
			OutputKind: agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE,
			Uid:        1000, Gid: 1000, Mode: 0o444, Length: uint64(len(content)), Sha256: digest[:],
		}},
	}
}

func recoveryAssignment(
	fixture configurationRecoveryFixture,
	phase agentpb.ReleaseRecoveryPhase,
) testtaskassignment.Assignment {
	assignment := fixture.assignment
	assignment.ExecutionMode = agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY
	assignment.ExecutionEpoch++
	assignment.Deadline = assignment.RecoveryDeadline
	stepIDs := executionplan.RecoveryStepIDs(assignment.Plan.CandidateReleaseProcedure)
	cursor := uint32(0)
	if phase == agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROVEN {
		cursor = uint32(len(stepIDs))
	}
	assignment.ReleaseRecoveryDirective = &agentpb.ReleaseRecoveryDirective{
		StepIds: stepIDs, Cursor: cursor, Phase: phase,
		ApplicableCompensationStepIds: []string{fixture.compensate.StepId},
	}
	return assignment
}
