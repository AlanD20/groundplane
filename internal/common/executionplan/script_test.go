package executionplan

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	testScriptID          = "scr_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testScriptExecutionID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testScriptSnapshotID  = "01ARZ3NDEKTSV4RRFFQ69G5FAW"
)

func TestSealManualScriptPlanBindsEveryArtifact(t *testing.T) {
	plan := validManualScriptPlan(t)
	sealed, err := Seal(plan)
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	if len(sealed.PlanHash) != sha256.Size {
		t.Fatalf("PlanHash length = %d", len(sealed.PlanHash))
	}
	if !bytes.Equal(
		sealed.Steps[0].GetRunScript().BodySha256,
		sealed.ScriptBodyArtifacts[0].Sha256,
	) {
		t.Fatal("RunScript body digest is not bound to body metadata")
	}
}

func TestSealManualScriptPlanRejectsCrossExecutionArtifact(t *testing.T) {
	plan := validManualScriptPlan(t)
	plan.ScriptBodyArtifacts[0].ScriptExecutionId = "01ARZ3NDEKTSV4RRFFQ69G5FAX"
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(cross-execution body) error = nil")
	}
}

func TestSealManualScriptPlanRejectsSnapshotTampering(t *testing.T) {
	plan := validManualScriptPlan(t)
	plan.ScriptRunnerSnapshots[0].ServiceModRevision++
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(tampered snapshot) error = nil")
	}
}

func TestSealManualScriptPlanRejectsOwnershipLabelOutsideClosedSet(t *testing.T) {
	plan := validManualScriptPlan(t)
	plan.ScriptRunnerProjections[0].Labels = append(
		plan.ScriptRunnerProjections[0].Labels,
		&agentpb.ScriptStringPair{Key: "com.groundplane.task-id", Value: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
	)
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(extra ownership label) error = nil")
	}
}

// Rationale: one immutable Environment Blueprint candidate may run only its
// post-deploy Script after the candidate procedure and against the candidate
// Release already bound into the exact Compose artifact.
func TestSealAcceptsBlueprintCandidatePostDeployScript(t *testing.T) {
	sealed, err := Seal(validBlueprintScriptReconcilePlan(t))
	if err != nil {
		t.Fatalf("Seal(Blueprint candidate Script) error = %v", err)
	}
	if _, err := Validate(sealed); err != nil {
		t.Fatalf("Validate(Blueprint candidate Script) error = %v", err)
	}
}

// Rationale: permitting the typed Blueprint sequence must not grant Script
// execution to another reconcile target or to a candidate with no Release binding.
func TestSealRejectsNonBlueprintAndUnboundReconcileScripts(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*agentpb.ExecutionPlan)
	}{
		{name: "ordinary reconcile operation", mutate: func(plan *agentpb.ExecutionPlan) {
			plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
		}},
		{name: "unbound candidate Release", mutate: func(plan *agentpb.ExecutionPlan) {
			for _, label := range plan.Artifacts[0].Services[0].ExpectedLabels {
				if label.Key == labelReleaseID {
					label.Value = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"
				}
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := validBlueprintScriptReconcilePlan(t)
			test.mutate(plan)
			if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("Seal(invalid reconcile Script) error = %v, want validation.failed", err)
			}
		})
	}
}

// Rationale: an unrelated artifact must not authorize a Script when the exact
// predecessor ComposeApply selects a different artifact without the candidate.
func TestSealRejectsBlueprintScriptAuthorizedByDecoyArtifact(t *testing.T) {
	plan := validBlueprintScriptReconcilePlan(t)
	decoy := proto.Clone(plan.Artifacts[0]).(*agentpb.ComposeArtifact)
	decoy.ArtifactId = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	decoy.Services = nil
	plan.Artifacts = append(plan.Artifacts, decoy)
	plan.Steps[0].GetComposeApply().ArtifactId = decoy.ArtifactId

	if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Seal(decoy Blueprint artifact) error = %v, want validation.failed", err)
	}
}

// Rationale: a correctly rehashed forged ordinary reconcile plan must fail
// semantic validation, not merely fail because its digest is stale.
func TestValidateRejectsForgedOrdinaryReconcileScript(t *testing.T) {
	sealed, err := Seal(validBlueprintScriptReconcilePlan(t))
	if err != nil {
		t.Fatalf("Seal(Blueprint candidate Script) error = %v", err)
	}
	sealed.Operation = agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
	sealed.PlanHash = nil
	encoded, err := marshalHashInput(sealed)
	if err != nil {
		t.Fatalf("marshal forged reconcile hash input: %v", err)
	}
	digest := sha256.Sum256(encoded)
	sealed.PlanHash = digest[:]
	if _, err := Validate(sealed); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Validate(forged ordinary reconcile Script) error = %v, want validation.failed", err)
	}
}

func validBlueprintScriptReconcilePlan(t *testing.T) *agentpb.ExecutionPlan {
	t.Helper()
	plan := validManualScriptPlan(t)
	run := plan.Steps[0].GetRunScript()
	snapshot := plan.ScriptRunnerSnapshots[0]
	applyStepID := "step_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	yaml := []byte("services:\n  api:\n    image: registry.example/app@sha256:" + strings.Repeat("a", 64) + "\n")
	yamlDigest := sha256.Sum256(yaml)
	artifact := &agentpb.ComposeArtifact{
		ArtifactId:  "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		OwnerKind:   agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:     run.EnvironmentId,
		ProjectName: "gp-" + strings.ToLower(run.EnvironmentId),
		AuthorizedVolumeDir: "/var/lib/groundplane/volumes/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
			"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + run.EnvironmentId,
		CanonicalYaml: yaml,
		YamlSha256:    yamlDigest[:],
		Services: []*agentpb.ComposeService{{
			ServiceId: run.ServiceId, ComposeName: "api", ExpectedReplicas: 1,
			ImageReference: snapshot.ImageReference, ImageIndexDigest: append([]byte(nil), snapshot.ImageDigest...),
			ImageChildDigest: bytes.Repeat([]byte{0xbb}, sha256.Size),
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: labelEnvironmentID, Value: run.EnvironmentId},
				{Key: labelKind, Value: "service"},
				{Key: labelManaged, Value: "true"},
				{Key: labelPlanID, Value: plan.PlanId},
				{Key: labelReleaseID, Value: run.ReleaseId},
				{Key: labelRenderGen, Value: "7"},
				{Key: labelServiceID, Value: run.ServiceId},
			},
		}},
	}
	plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
	plan.TargetId = run.EnvironmentId
	plan.Artifacts = []*agentpb.ComposeArtifact{artifact}
	plan.Steps[0].Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_POST_HOOK
	plan.Steps[0].PrerequisiteStepId = applyStepID
	plan.Steps = append([]*agentpb.ExecutionStep{{
		StepId: applyStepID, TimeoutSeconds: 30,
		Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
			ArtifactId: artifact.ArtifactId, FullReconcile: true,
		}},
	}}, plan.Steps...)
	return plan
}

func validManualScriptPlan(t *testing.T) *agentpb.ExecutionPlan {
	t.Helper()
	imageDigest := bytes.Repeat([]byte{0xaa}, sha256.Size)
	bodyDigest := sha256.Sum256([]byte("php artisan migrate\n"))
	serviceDigest := sha256.Sum256([]byte("service definition"))
	projection := &agentpb.ScriptRunnerProjection{
		SnapshotId: testScriptSnapshotID,
		Name:       "gp-script-" + strings.ToLower(testScriptExecutionID),
		Image:      "registry.example/app@sha256:" + strings.Repeat("a", 64),
		Labels: []*agentpb.ScriptStringPair{
			{Key: "com.groundplane.environment-id", Value: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
			{Key: "com.groundplane.kind", Value: "script-runner"},
			{Key: "com.groundplane.managed", Value: "true"},
			{Key: "com.groundplane.operation-id", Value: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
			{Key: "com.groundplane.plan-id", Value: testPlanID},
			{Key: "com.groundplane.project-id", Value: "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
			{Key: "com.groundplane.release-id", Value: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
			{Key: "com.groundplane.render-generation", Value: "7"},
			{Key: "com.groundplane.script-execution-id", Value: testScriptExecutionID},
			{Key: "com.groundplane.script-generation", Value: "1"},
			{Key: "com.groundplane.script-id", Value: testScriptID},
			{Key: "com.groundplane.service-id", Value: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
			{Key: "com.groundplane.tenant-id", Value: "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		},
		Entrypoint:       []string{"/bin/sh"},
		Command:          []string{"/groundplane-script-body"},
		StopGraceSeconds: 10,
	}
	projectionDigest, err := scriptMessageDigest(projection)
	if err != nil {
		t.Fatalf("projection digest: %v", err)
	}
	snapshot := &agentpb.ResolvedRunnerSnapshot{
		SnapshotId: testScriptSnapshotID, ScriptExecutionId: testScriptExecutionID,
		TenantId: "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV", TenantModRevision: 1,
		ProjectId: "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV", ProjectModRevision: 2,
		EnvironmentId: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", EnvironmentModRevision: 3,
		ServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", ServiceModRevision: 4,
		ServiceDefinitionSha256: serviceDigest[:],
		ReleaseId:               "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV", ReleaseModRevision: 5,
		ImageReference:            "registry.example/app@sha256:" + strings.Repeat("a", 64),
		ImageDigest:               imageDigest,
		BlueprintBundleGeneration: "task_01ARZ3NDEKTSV4RRFFQ69G5FAX",
		RenderGeneration:          7, NetworkTopologyRevision: 6,
		AppliedEnvironmentRevisionId:       "task_01ARZ3NDEKTSV4RRFFQ69G5FAY",
		AppliedEnvironmentRenderGeneration: 8,
		AppliedEnvironmentModRevision:      9,
		RunnerProjectionSha256:             projectionDigest,
	}
	snapshotDigest, err := scriptMessageDigest(snapshot)
	if err != nil {
		t.Fatalf("snapshot digest: %v", err)
	}
	return &agentpb.ExecutionPlan{
		Schema: SchemaVersion, PlanId: testPlanID, RenderGeneration: 7,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_SCRIPT, TargetId: testScriptID,
		ScriptRunnerSnapshots:   []*agentpb.ResolvedRunnerSnapshot{snapshot},
		ScriptRunnerProjections: []*agentpb.ScriptRunnerProjection{projection},
		ScriptBodyArtifacts: []*agentpb.ScriptBodyArtifactMetadata{{
			ScriptExecutionId: testScriptExecutionID, ScriptId: testScriptID,
			Generation: 1, Size: uint32(len("php artisan migrate\n")), Sha256: bodyDigest[:],
		}},
		Steps: []*agentpb.ExecutionStep{{
			StepId: testStepID, TimeoutSeconds: ScriptExecutionTimeoutSeconds,
			Payload: &agentpb.ExecutionStep_RunScript{RunScript: &agentpb.RunScript{
				ScriptExecutionId: testScriptExecutionID, ScriptId: testScriptID, ScriptGeneration: 1,
				EnvironmentId: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				ServiceId:     "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				ReleaseId:     "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV", RenderGeneration: 7,
				ServiceDefinitionSha256: serviceDigest[:], BodySha256: bodyDigest[:],
				RunnerSnapshotId: testScriptSnapshotID, RunnerSnapshotSha256: snapshotDigest,
			}},
		}},
	}
}
