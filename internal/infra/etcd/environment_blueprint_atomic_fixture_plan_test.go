package etcd

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func environmentBlueprintAtomicFixturePlan(
	t *testing.T,
	task TaskRecord,
	environmentID string,
	manifest ReleaseStagedManifest,
	procedure *agentpb.CandidateReleaseProcedure,
) *agentpb.ExecutionPlan {
	t.Helper()
	canonicalYAML := []byte("services: {}\n")
	yamlDigest := sha256.Sum256(canonicalYAML)
	artifact := &agentpb.ComposeArtifact{
		ArtifactId:    task.Params[TaskComposeArtifactParam],
		OwnerKind:     agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:       environmentID,
		ProjectName:   "gp-" + strings.ToLower(environmentID),
		CanonicalYaml: canonicalYAML,
		YamlSha256:    yamlDigest[:],
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/" + task.Owner.TenantID + "/" +
			task.Owner.ProjectID + "/" + environmentID,
		Services: make([]*agentpb.ComposeService, len(manifest.Members)),
	}
	steps := make([]*agentpb.ExecutionStep, 0, len(manifest.Members)*3)
	for index, member := range procedure.GetMembers() {
		serviceName := fmt.Sprintf("candidate-%02d", index)
		imageDigest := sha256.Sum256([]byte(member.GetServiceId()))
		artifact.Services[index] = &agentpb.ComposeService{
			ServiceId: member.GetServiceId(), ComposeName: serviceName,
			ExpectedReplicas: 1, HasHealthcheck: true,
			Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
			ImageReference: "sha256:" + hex.EncodeToString(imageDigest[:]),
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.environment-id", Value: environmentID},
				{Key: "com.groundplane.kind", Value: "service"},
				{Key: "com.groundplane.managed", Value: "true"},
				{Key: "com.groundplane.plan-id", Value: task.PlanID},
				{Key: "com.groundplane.release-id", Value: member.GetCandidateReleaseId()},
				{Key: "com.groundplane.render-generation", Value: fmt.Sprintf("%d", task.RenderGeneration)},
				{Key: "com.groundplane.service-id", Value: member.GetServiceId()},
			},
		}
		forwardStepID := member.GetForwardStepIds()[0]
		steps = append(steps,
			&agentpb.ExecutionStep{
				StepId: forwardStepID, TimeoutSeconds: uint32(task.TimeoutSeconds),
				Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
				Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
					ArtifactId: artifact.ArtifactId, ServiceIds: []string{member.GetServiceId()},
					ForceRecreate: true, NoDependencies: true,
				}},
			},
			&agentpb.ExecutionStep{
				StepId: member.GetServingPredecessor().GetProbeStepId(), TimeoutSeconds: uint32(task.TimeoutSeconds),
				Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
				Payload: &agentpb.ExecutionStep_CandidateRestorationProbe{
					CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{
						CandidateArtifactId: artifact.ArtifactId, ServiceId: member.GetServiceId(), CandidateReleaseId: member.GetCandidateReleaseId(),
					},
				},
			},
			&agentpb.ExecutionStep{
				StepId: member.GetServingPredecessor().
					GetCompensateStepId(),
				TimeoutSeconds:     uint32(task.TimeoutSeconds),
				Policy:             agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
				PrerequisiteStepId: forwardStepID,
				Payload: &agentpb.ExecutionStep_CandidateRestorationCompensate{
					CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
						CandidateArtifactId: artifact.ArtifactId, ServiceId: member.GetServiceId(), CandidateReleaseId: member.GetCandidateReleaseId(),
					},
				},
			},
		)
	}
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration),
		Operation:        agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
		TargetId:         environmentID, Artifacts: []*agentpb.ComposeArtifact{artifact},
		Steps: steps, CandidateReleaseProcedure: procedure,
	})
	if err != nil {
		t.Fatalf("seal Blueprint fixture plan: %v", err)
	}
	return plan
}
