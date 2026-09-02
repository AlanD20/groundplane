package controller

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: the final Blueprint plan must attach each sealed post-deploy
// Script identity to its exact RunScript step and no ordinary procedure step.
func TestBlueprintTaskStepRecordsCaptureSealedScriptIdentity(t *testing.T) {
	const (
		scriptID    = "scr_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		executionID = "01ARZ3NDEKTSV4RRFFQ69G5FAW"
	)
	steps := []*agentpb.ExecutionStep{
		{StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAX"},
		{
			StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAY",
			Payload: &agentpb.ExecutionStep_RunScript{RunScript: &agentpb.RunScript{
				ScriptExecutionId: executionID,
				ScriptId:          scriptID,
			}},
		},
		{StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAZ"},
	}
	members := []etcd.ReleaseTaskRenderMember{{Render: etcd.ReleaseRenderInput{
		Hooks: []etcd.ReleaseHookRenderInput{{
			ScriptID: scriptID, ScriptSlug: "migrate-schema",
			When: core.ScriptPostDeploy, ScriptExecutionID: executionID,
		}},
	}}}
	records, err := blueprintTaskStepRecords(steps, members)
	if err != nil {
		t.Fatalf("blueprintTaskStepRecords() error = %v", err)
	}
	if len(records) != 3 || records[0].ScriptID != "" || records[0].ScriptSlug != "" ||
		records[1].ID != steps[1].StepId || records[1].ScriptID != scriptID ||
		records[1].ScriptSlug != "migrate-schema" ||
		records[2].ScriptID != "" || records[2].ScriptSlug != "" {
		t.Fatalf("blueprintTaskStepRecords() = %#v", records)
	}
}

// Rationale: Script metadata must come from the sealed hook rather than an
// untrusted or mismatched RunScript payload in the rebuilt execution plan.
func TestBlueprintTaskStepRecordsRejectMismatchedSealedIdentity(t *testing.T) {
	steps := []*agentpb.ExecutionStep{{
		StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Payload: &agentpb.ExecutionStep_RunScript{RunScript: &agentpb.RunScript{
			ScriptExecutionId: "01ARZ3NDEKTSV4RRFFQ69G5FAW",
			ScriptId:          "scr_01ARZ3NDEKTSV4RRFFQ69G5FAX",
		}},
	}}
	members := []etcd.ReleaseTaskRenderMember{{Render: etcd.ReleaseRenderInput{
		Hooks: []etcd.ReleaseHookRenderInput{{
			ScriptID: "scr_01ARZ3NDEKTSV4RRFFQ69G5FAY", ScriptSlug: "migrate-schema",
			When: core.ScriptPostDeploy, ScriptExecutionID: "01ARZ3NDEKTSV4RRFFQ69G5FAW",
		}},
	}}}
	if _, err := blueprintTaskStepRecords(steps, members); err == nil {
		t.Fatal("blueprintTaskStepRecords() accepted a mismatched sealed Script identity")
	}
}
