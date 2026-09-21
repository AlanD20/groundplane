package etcd

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: ordinary publication must accept only the exact sealed plan from
// which its descriptor, durable Task, and candidate artifact were prepared.
func TestReleasePreparedArtifactRejectsUnsealedOrMismatchedAuthorityBeforePublication(t *testing.T) {
	t.Parallel()
	valid := releasePreparedArtifactFixture(t, []byte("services: {}\n"))
	for name, mutate := range map[string]func(*ReleasePublicationEvidence){
		"missing plan":  func(value *ReleasePublicationEvidence) { value.Plan = nil },
		"unsealed plan": func(value *ReleasePublicationEvidence) { value.Plan.PlanHash = nil },
		"descriptor": func(value *ReleasePublicationEvidence) {
			value.CandidateReleaseDescriptor.PlanHash[0] ^= 0xff
		},
		"Task plan id": func(value *ReleasePublicationEvidence) {
			value.Task.PlanID = "plan_01ARZ3NDEKTSV4RRFFQ69G5FB0"
		},
		"Task plan hash":         func(value *ReleasePublicationEvidence) { value.Task.PlanHash = strings.Repeat("f", 64) },
		"Task render generation": func(value *ReleasePublicationEvidence) { value.Task.RenderGeneration++ },
		"Task target": func(value *ReleasePublicationEvidence) {
			value.Task.Target = "svc_01ARZ3NDEKTSV4RRFFQ69G5FB1"
		},
		"Task operation": func(value *ReleasePublicationEvidence) { value.Task.Type = testtaskjournal.TaskRollback },
		"Task step count": func(value *ReleasePublicationEvidence) {
			value.Task.Steps = value.Task.Steps[:len(value.Task.Steps)-1]
		},
		"Task step identity": func(value *ReleasePublicationEvidence) {
			value.Task.Steps[0].ID = "step_01ARZ3NDEKTSV4RRFFQ69G5FB2"
		},
		"candidate artifact": func(value *ReleasePublicationEvidence) {
			value.Task.Params[testtaskjournal.TaskComposeArtifactParam] = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FB3"
		},
		"artifact owner": func(value *ReleasePublicationEvidence) {
			value.EnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FB4"
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			value := cloneReleasePreparedArtifactEvidence(valid)
			mutate(&value)
			if _, err := releasePreparedArtifact(value); !errors.Is(
				err,
				errs.New(errs.KindValidationFailed, ""),
			) {
				t.Fatalf("releasePreparedArtifact() error = %v, want validation.failed", err)
			}
		})
	}
}

// Rationale: retaining the prepared candidate in the existing publication
// marker must keep the established durable record ceiling fail-closed.
func TestReleasePreparedArtifactPreservesPublicationRecordSizeLimit(t *testing.T) {
	t.Parallel()
	yaml := append([]byte("services: {}\n#"), bytes.Repeat([]byte("x"), domain.MaximumRecordBytes)...)
	evidence := releasePreparedArtifactFixture(t, yaml)
	artifact, err := releasePreparedArtifact(evidence)
	if err != nil {
		t.Fatalf("releasePreparedArtifact() error = %v", err)
	}
	_, err = testreleases.EncodeReleaseRecord("release-publication", testreleases.ReleasePublicationMarker{
		CandidateReleaseDescriptor: evidence.CandidateReleaseDescriptor,
		ExecutedComposeArtifact:    artifact,
	})
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("oversized release publication marker error = %v, want validation.failed", err)
	}
}

func releasePreparedArtifactFixture(t *testing.T, yaml []byte) ReleasePublicationEvidence {
	t.Helper()
	const (
		planID        = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		serviceID     = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		releaseID     = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		artifactID    = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		applyID       = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		healthID      = "step_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		acknowledgeID = "step_01ARZ3NDEKTSV4RRFFQ69G5FAX"
		probeID       = "step_01ARZ3NDEKTSV4RRFFQ69G5FAY"
		compensateID  = "step_01ARZ3NDEKTSV4RRFFQ69G5FAZ"
	)
	digest := sha256.Sum256(yaml)
	projectName := "gp-" + strings.ToLower(environmentID)
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: artifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId: environmentID, ProjectName: projectName, CanonicalYaml: yaml, YamlSha256: digest[:],
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
			"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + environmentID,
		Services: []*agentpb.ComposeService{{
			ServiceId: serviceID, ComposeName: "api", ExpectedReplicas: 1, HasHealthcheck: true,
			Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
			ImageReference: "sha256:" + strings.Repeat("a", 64),
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.environment-id", Value: environmentID},
				{Key: "com.groundplane.kind", Value: "service"},
				{Key: "com.groundplane.managed", Value: "true"},
				{Key: "com.groundplane.plan-id", Value: planID},
				{Key: "com.groundplane.release-id", Value: releaseID},
				{Key: "com.groundplane.render-generation", Value: "1"},
				{Key: "com.groundplane.service-id", Value: serviceID},
			},
		}},
	}
	procedure, err := executionplan.BuildCandidateReleaseProcedure(executionplan.CandidateReleaseProcedureInput{
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY,
		Members: []executionplan.CandidateReleaseMemberInput{{
			ServiceID: serviceID, CandidateReleaseID: releaseID, CandidateArtifactID: artifactID,
			ForwardStepIDs: []string{applyID, healthID, acknowledgeID},
			CandidateAbsence: &executionplan.CandidateAbsenceInput{
				ComposeProjectName: projectName, ProbeStepID: probeID, CompensateStepID: compensateID,
				Services: []executionplan.CandidateServiceIdentity{{ServiceID: serviceID, ReleaseID: releaseID}},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: planID, RenderGeneration: 1,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, TargetId: serviceID,
		Artifacts: []*agentpb.ComposeArtifact{artifact},
		Steps: []*agentpb.ExecutionStep{
			{
				StepId:         applyID,
				TimeoutSeconds: 300,
				Policy:         agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
				Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
					ArtifactId: artifactID, ServiceIds: []string{serviceID}, ForceRecreate: true, NoDependencies: true,
				}},
			},
			{
				StepId:             healthID,
				TimeoutSeconds:     300,
				Policy:             agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
				PrerequisiteStepId: applyID,
				Payload: &agentpb.ExecutionStep_WaitHealthy{WaitHealthy: &agentpb.WaitHealthy{
					ArtifactId: artifactID, ServiceIds: []string{serviceID},
				}},
			},
			{
				StepId:             acknowledgeID,
				TimeoutSeconds:     300,
				Policy:             agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
				PrerequisiteStepId: healthID,
				Payload: &agentpb.ExecutionStep_ServiceRecreateAcknowledge{
					ServiceRecreateAcknowledge: &agentpb.ServiceRecreateAcknowledge{
						ArtifactId: artifactID, ServiceId: serviceID, ReleaseId: releaseID,
					},
				},
			},
			{
				StepId:         probeID,
				TimeoutSeconds: 300,
				Policy:         agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
				Payload: &agentpb.ExecutionStep_CandidateRestorationProbe{
					CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{
						CandidateArtifactId: artifactID, ServiceId: serviceID, CandidateReleaseId: releaseID,
					},
				},
			},
			{
				StepId:             compensateID,
				TimeoutSeconds:     300,
				Policy:             agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
				PrerequisiteStepId: applyID,
				Payload: &agentpb.ExecutionStep_CandidateRestorationCompensate{
					CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
						CandidateArtifactId: artifactID, ServiceId: serviceID, CandidateReleaseId: releaseID,
					},
				},
			},
		},
		CandidateReleaseProcedure: procedure,
	})
	if err != nil {
		t.Fatalf("seal ordinary Release fixture plan: %v", err)
	}
	descriptor, err := executionplan.DescribeCandidateRelease(plan)
	if err != nil {
		t.Fatal(err)
	}
	steps := make([]testtaskjournal.TaskStepRecord, len(plan.GetSteps()))
	for index, step := range plan.GetSteps() {
		steps[index] = testtaskjournal.TaskStepRecord{Kind: testtaskjournal.TaskStepOperation, ID: step.GetStepId()}
	}
	return ReleasePublicationEvidence{
		EnvironmentID: environmentID,
		Task: TaskRecord{
			PlanID: planID, PlanHash: hex.EncodeToString(plan.GetPlanHash()),
			RenderGeneration: 1, Type: testtaskjournal.TaskDeploy, Target: serviceID,
			Params: map[string]string{testtaskjournal.TaskComposeArtifactParam: artifactID}, Steps: steps,
		},
		CandidateReleaseDescriptor: descriptor,
		Plan:                       plan,
	}
}

func cloneReleasePreparedArtifactEvidence(value ReleasePublicationEvidence) ReleasePublicationEvidence {
	value.Plan = proto.CloneOf(value.Plan)
	value.CandidateReleaseDescriptor = executionplan.CloneCandidateReleaseDescriptor(value.CandidateReleaseDescriptor)
	value.Task.Params = map[string]string{
		testtaskjournal.TaskComposeArtifactParam: value.Task.Params[testtaskjournal.TaskComposeArtifactParam],
	}
	value.Task.Steps = testtaskjournal.CloneTaskSteps(value.Task.Steps)
	return value
}
