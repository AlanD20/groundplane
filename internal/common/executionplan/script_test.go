package executionplan

import (
	"bytes"
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
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
