package controller

import (
	"fmt"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Rationale: restart recovery must accept the maximum documented post-deploy
// Script selection and reproduce its exact apply, hook, then health ordering.
func TestBlueprintReleaseProcedureStepIDsAcceptMaximumHooks(t *testing.T) {
	task, members := blueprintReleaseStepIDFixture(taskcontract.MaximumBlueprintPostDeployHooks)
	apply, health, post, next, err := blueprintReleaseProcedureStepIDs(task, members, 0)
	if err != nil {
		t.Fatalf("blueprintReleaseProcedureStepIDs() error = %v", err)
	}
	if len(apply) != 1 || apply[0] != "apply" || len(health) != 1 || health[0] != "health" ||
		len(post) != 1 || len(post[0]) != taskcontract.MaximumBlueprintPostDeployHooks ||
		post[0][0] != "hook-00" || post[0][len(post[0])-1] != "hook-15" || next != len(task.Steps) {
		t.Fatalf("procedure ids = apply=%v health=%v post=%v next=%d", apply, health, post, next)
	}
}

// Rationale: oversized or tampered durable hook authority must be quarantined
// before an Agent receives a plan that cannot be reproduced after restart.
func TestBlueprintReleaseProcedureStepIDsRejectInvalidAuthority(t *testing.T) {
	t.Run("overflow", func(t *testing.T) {
		task, members := blueprintReleaseStepIDFixture(taskcontract.MaximumBlueprintPostDeployHooks + 1)
		if _, _, _, _, err := blueprintReleaseProcedureStepIDs(task, members, 0); err == nil {
			t.Fatal("blueprintReleaseProcedureStepIDs() accepted an oversized hook selection")
		}
	})
	t.Run("missing execution binding", func(t *testing.T) {
		task, members := blueprintReleaseStepIDFixture(1)
		delete(task.Params, etcd.ReleaseHookStepExecutionParam("hook-00"))
		if _, _, _, _, err := blueprintReleaseProcedureStepIDs(task, members, 0); err == nil {
			t.Fatal("blueprintReleaseProcedureStepIDs() accepted a missing execution binding")
		}
	})
	t.Run("wrong member binding", func(t *testing.T) {
		task, members := blueprintReleaseStepIDFixture(1)
		task.Params[etcd.ReleaseHookStepMemberParam("hook-00")] = "2"
		if _, _, _, _, err := blueprintReleaseProcedureStepIDs(task, members, 0); err == nil {
			t.Fatal("blueprintReleaseProcedureStepIDs() accepted a forged member binding")
		}
	})
}

func blueprintReleaseStepIDFixture(hookCount int) (etcd.TaskRecord, []etcd.ReleaseTaskRenderMember) {
	task := etcd.TaskRecord{Params: make(map[string]string)}
	task.Steps = append(task.Steps, etcd.TaskStepRecord{ID: "apply"})
	hooks := make([]etcd.ReleaseHookRenderInput, hookCount)
	for index := range hooks {
		stepID := fmt.Sprintf("hook-%02d", index)
		executionID := fmt.Sprintf("execution-%02d", index)
		task.Steps = append(task.Steps, etcd.TaskStepRecord{ID: stepID})
		task.Params[etcd.ReleaseHookStepMemberParam(stepID)] = "1"
		task.Params[etcd.ReleaseHookStepExecutionParam(stepID)] = executionID
		hooks[index] = etcd.ReleaseHookRenderInput{
			When: core.ScriptPostDeploy, ScriptExecutionID: executionID,
		}
	}
	task.Steps = append(task.Steps, etcd.TaskStepRecord{ID: "health"})
	return task, []etcd.ReleaseTaskRenderMember{{Render: etcd.ReleaseRenderInput{Hooks: hooks}}}
}
