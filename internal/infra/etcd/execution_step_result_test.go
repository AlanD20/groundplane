package etcd

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func TestExecutionStepResultPersistenceReplaysExactAndRejectsConflict(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	task := validTaskRecord(taskJournalTime())
	seedTaskRepositoryRunningTask(t, store, task)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	planHash, err := hex.DecodeString(task.PlanHash)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executionplan.SealExecutionStepResult(&agentpb.ExecutionStepResult{
		OperationId: task.OperationID, PlanHash: planHash, StepId: task.Steps[0].ID,
		Result: &agentpb.ExecutionStepResult_ProcedureServiceImage{
			ProcedureServiceImage: &agentpb.ProcedureServiceImageResult{
				ServiceId:          "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				ReleaseId:          "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				RequestedReference: "registry.example/app:candidate",
				ImmutableReference: "registry.example/app@sha256:" + strings.Repeat("b", 64),
				ImageDigest:        bytes.Repeat([]byte{0xbb}, 32),
				LocalImageId:       "sha256:" + strings.Repeat("c", 64),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	input := ExecutionStepResultInput{
		TaskID: task.ID, AssignmentID: taskEventTestAssignmentID,
		AgentID: taskEventTestAgentID, AgentGeneration: 1,
		Result: result, At: taskJournalTime().Add(time.Second),
	}
	first, err := repository.AcknowledgeExecutionStepResult(ctx, input)
	if err != nil || first.Duplicate {
		t.Fatalf("first write = %#v, %v", first, err)
	}
	restarted, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := restarted.AcknowledgeExecutionStepResult(ctx, input)
	if err != nil || !replay.Duplicate || replay.Revision != first.Revision {
		t.Fatalf("replay = %#v, %v; first = %#v", replay, err, first)
	}
	retryTask := validTaskRecord(taskJournalTime().Add(time.Minute))
	retryTask.OperationID = task.OperationID
	retryTask.PlanHash = task.PlanHash
	retryTask.Steps = append([]TaskStepRecord(nil), task.Steps...)
	seedTaskRepositoryRunningTask(t, store, retryTask)
	retryInput := input
	retryInput.TaskID = retryTask.ID
	retry, err := restarted.AcknowledgeExecutionStepResult(ctx, retryInput)
	if err != nil || !retry.Duplicate || retry.Revision != first.Revision {
		t.Fatalf("current retry replay = %#v, %v; first = %#v", retry, err, first)
	}
	staleAssignment := retryInput
	staleAssignment.AssignmentID = "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	if _, err := restarted.AcknowledgeExecutionStepResult(ctx, staleAssignment); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("stale assignment replay error = %v, want state conflict", err)
	}
	staleAgent := retryInput
	staleAgent.AgentID = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	if _, err := restarted.AcknowledgeExecutionStepResult(ctx, staleAgent); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("stale Agent replay error = %v, want state conflict", err)
	}
	staleGeneration := retryInput
	staleGeneration.AgentGeneration++
	if _, err := restarted.AcknowledgeExecutionStepResult(ctx, staleGeneration); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("stale Agent generation replay error = %v, want state conflict", err)
	}

	conflict := *result
	conflict.Result = &agentpb.ExecutionStepResult_ProcedureServiceImage{
		ProcedureServiceImage: proto.Clone(result.GetProcedureServiceImage()).(*agentpb.ProcedureServiceImageResult),
	}
	conflict.GetProcedureServiceImage().LocalImageId = "sha256:" + strings.Repeat("d", 64)
	conflict.ControlPayloadSha256 = nil
	conflicting, err := executionplan.SealExecutionStepResult(&conflict)
	if err != nil {
		t.Fatal(err)
	}
	input.Result = conflicting
	if _, err := restarted.AcknowledgeExecutionStepResult(ctx, input); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("conflict error = %v, want state conflict", err)
	}
	listed, err := restarted.ListExecutionStepResults(ctx, task.OperationID, task.PlanHash)
	if err != nil || len(listed) != 1 {
		t.Fatalf("listed = %#v, %v", listed, err)
	}
}
