package controller

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func TestBuildReleaseHookPlanKeepsClosedPhasesAndSlugOrder(t *testing.T) {
	// Rationale: release hooks must not become ordinary forward steps because
	// failure hooks execute only after compensation and never on success.
	releaseID := "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	hooks := []etcd.ReleaseHookRenderInput{
		releaseHookInput(t, releaseID, "z-post", core.ScriptPostDeploy, "01ARZ3NDEKTSV4RRFFQ69G5FAA", "01ARZ3NDEKTSV4RRFFQ69G5FAB"),
		releaseHookInput(t, releaseID, "a-pre", core.ScriptPreDeploy, "01ARZ3NDEKTSV4RRFFQ69G5FAC", "01ARZ3NDEKTSV4RRFFQ69G5FAD"),
		releaseHookInput(t, releaseID, "b-failure", core.ScriptOnFailure, "01ARZ3NDEKTSV4RRFFQ69G5FAE", "01ARZ3NDEKTSV4RRFFQ69G5FAF"),
	}
	plan, err := BuildReleaseHookPlan(ReleaseHookPlanInput{
		Operation: domain.OperationDeploy, ReleaseID: releaseID,
		ServingStepID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAG", CompensationStepID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAH",
		PreStepIDs: []string{"step_01ARZ3NDEKTSV4RRFFQ69G5FAJ"}, PostStepIDs: []string{"step_01ARZ3NDEKTSV4RRFFQ69G5FAK"},
		FailureStepIDs: []string{"step_01ARZ3NDEKTSV4RRFFQ69G5FAM"}, Hooks: hooks,
	})
	if err != nil {
		t.Fatalf("BuildReleaseHookPlan() error = %v", err)
	}
	if len(plan.PreSteps) != 1 || len(plan.PostSteps) != 1 || len(plan.FailureSteps) != 1 {
		t.Fatalf("hook phase counts = %d/%d/%d", len(plan.PreSteps), len(plan.PostSteps), len(plan.FailureSteps))
	}
	if plan.PreSteps[0].GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_PRE_HOOK ||
		plan.PostSteps[0].GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_POST_HOOK ||
		plan.FailureSteps[0].GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FAILURE_HOOK {
		t.Fatalf("hook policies = %s/%s/%s", plan.PreSteps[0].GetPolicy(), plan.PostSteps[0].GetPolicy(), plan.FailureSteps[0].GetPolicy())
	}
	if plan.PostSteps[0].GetPrerequisiteStepId() != "step_01ARZ3NDEKTSV4RRFFQ69G5FAG" ||
		plan.FailureSteps[0].GetPrerequisiteStepId() != "step_01ARZ3NDEKTSV4RRFFQ69G5FAH" {
		t.Fatalf("hook prerequisites = %q/%q", plan.PostSteps[0].GetPrerequisiteStepId(), plan.FailureSteps[0].GetPrerequisiteStepId())
	}
}

func releaseHookInput(
	t *testing.T,
	releaseID string,
	slug string,
	when core.ScriptHook,
	executionID string,
	snapshotID string,
) etcd.ReleaseHookRenderInput {
	t.Helper()
	snapshot, err := proto.Marshal(&agentpb.ResolvedRunnerSnapshot{
		SnapshotId: snapshotID, ScriptExecutionId: executionID,
		ReleaseId:     releaseID,
		EnvironmentId: "env_01ARZ3NDEKTSV4RRFFQ69G5FAP", ServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAQ",
		RenderGeneration: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := proto.Marshal(&agentpb.ScriptRunnerProjection{SnapshotId: snapshotID})
	if err != nil {
		t.Fatal(err)
	}
	return etcd.ReleaseHookRenderInput{
		ScriptID: "scr_01ARZ3NDEKTSV4RRFFQ69G5FAN", ScriptSlug: slug,
		ServiceID: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAQ", When: when, ScriptGeneration: 1,
		ScriptExecutionID: executionID, RunnerSnapshotID: snapshotID, BodySize: 4,
		BodySHA256:              "230d8358dc8e8890b4c4d7f0f0e3b57d8e3b69b3501e584b05a4534d36e8c20c",
		ServiceDefinitionSHA256: "230d8358dc8e8890b4c4d7f0f0e3b57d8e3b69b3501e584b05a4534d36e8c20c",
		RunnerSnapshot:          snapshot, RunnerProjection: projection,
	}
}
