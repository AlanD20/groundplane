package etcd

import (
	"bytes"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// TASK-01: completing, restarting and retrying work must preserve the sealed
// public description without retaining credential-bearing procedure inputs.
func TestTaskDescriptionsSurviveCompletionAndRetryWithoutCredentials(t *testing.T) {
	now := taskJournalTime()
	task := validTaskRecord(now)
	task.TargetName = "shared-access"
	procedure := &agentpb.AdapterProcedure{AdapterKey: "postgres16",
		Phase:    agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_GRANT,
		AttachId: ids.New(ids.KindAttach), Password: []byte("private-procedure-password")}
	step := &agentpb.ExecutionStep{StepId: task.Steps[0].ID, TimeoutSeconds: 45,
		Payload: &agentpb.ExecutionStep_AdapterProcedure{AdapterProcedure: procedure}}
	task.Steps = taskjournal.CaptureStepDescriptions(task.Steps[:1], []*agentpb.ExecutionStep{step})
	procedure.AdapterKey, procedure.AttachId = "mutated-adapter", ids.New(ids.KindAttach)
	step.TimeoutSeconds = 99
	task, err := TransitionTaskStatus(task, taskjournal.TaskStatusPending, taskjournal.TaskStatusRunning, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	task, err = TransitionTaskStatus(task, taskjournal.TaskStatusRunning, taskjournal.TaskStatusFailed, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeTaskRecord(task)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, procedure.Password) || bytes.Contains(encoded, []byte("mutated-adapter")) {
		t.Fatal("durable description retained private or later-mutated procedure data")
	}
	restored, err := DecodeTaskRecord(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if restored.TargetName != "shared-access" || restored.Steps[0].Action != "Run postgres16 adapter: grant" || restored.Steps[0].TimeoutSeconds != 45 {
		t.Fatalf("captured presentation changed: %#v", restored.Steps)
	}
	retry, err := CloneRetryTask(restored, ids.New(ids.KindTask), taskjournal.TaskActorOperator, now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if retry.TargetName != restored.TargetName || retry.Steps[0] != restored.Steps[0] {
		t.Fatal("retry lost captured presentation")
	}
}
