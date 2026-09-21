package etcd

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func environmentBlueprintAtomicFixturePlan(
	t *testing.T,
	task TaskRecord,
	projection testenvironmentprojection.EnvironmentComposeProjection,
	manifest testreleases.ReleaseStagedManifest,
	procedure *agentpb.CandidateReleaseProcedure,
) *agentpb.ExecutionPlan {
	t.Helper()
	environmentID := projection.EnvironmentID
	canonicalYAML := []byte("services: {}\n")
	yamlDigest := sha256.Sum256(canonicalYAML)
	artifact := &agentpb.ComposeArtifact{
		ArtifactId:    task.Params[testtaskjournal.TaskComposeArtifactParam],
		OwnerKind:     agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:       environmentID,
		ProjectName:   "gp-" + strings.ToLower(environmentID),
		CanonicalYaml: canonicalYAML,
		YamlSha256:    yamlDigest[:],
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/" + task.Owner.TenantID + "/" +
			task.Owner.ProjectID + "/" + environmentID,
		Services: make([]*agentpb.ComposeService, len(manifest.Members)),
		Volumes:  make([]*agentpb.ComposeVolume, len(projection.Volumes)),
	}
	for index, volume := range projection.Volumes {
		artifact.Volumes[index] = &agentpb.ComposeVolume{
			VolumeId: volume.ID, ComposeName: volume.Key, DockerName: "gp_vol_" + strings.ToLower(volume.ID),
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.environment-id", Value: environmentID},
				{Key: "com.groundplane.kind", Value: "volume"},
				{Key: "com.groundplane.managed", Value: "true"},
			},
		}
	}
	slices.SortFunc(artifact.Volumes, func(left, right *agentpb.ComposeVolume) int {
		return cmp.Compare(left.VolumeId, right.VolumeId)
	})
	steps := make([]*agentpb.ExecutionStep, 0, len(manifest.Members)*3)
	for index, member := range procedure.GetMembers() {
		serviceName := fmt.Sprintf("candidate-%02d", index)
		for _, service := range projection.DesiredServices {
			if service.Desired.ID == member.GetServiceId() {
				serviceName = service.Desired.Name
			}
		}
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
				{Key: "com.groundplane.runtime-role", Value: "singleton"},
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
	var document strings.Builder
	document.WriteString("services:\n")
	for _, service := range artifact.Services {
		document.WriteString("  " + service.ComposeName + ":\n    image: " + service.ImageReference + "\n    labels:\n")
		for _, label := range service.ExpectedLabels {
			document.WriteString("      " + label.Key + ": " + fmt.Sprintf("%q", label.Value) + "\n")
		}
	}
	artifact.CanonicalYaml = []byte(document.String())
	yamlDigest = sha256.Sum256(artifact.CanonicalYaml)
	artifact.YamlSha256 = yamlDigest[:]
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
