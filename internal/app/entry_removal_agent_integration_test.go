package app

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/agent"
	migratedmaterialization "github.com/AlanD20/groundplane/internal/agent/materialization"
	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	testtaskmaterialization "github.com/AlanD20/groundplane/internal/controller/taskmaterialization"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/docker/materializerrunner"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type entryRemovalHelper struct{ calls int }

func (helper *entryRemovalHelper) Run(ctx context.Context, request materializerrunner.Request) error {
	_, err := entrymaterialization.Decode(ctx, request.Stream, entrymaterialization.Limits{
		MaxContentBytes:     entrymaterialization.MaximumContentBytes,
		MaxDestinationBytes: entrymaterialization.MaximumDestinationBytes,
	}, func(_ context.Context, _ entrymaterialization.Header, source io.Reader) error {
		_, err := io.Copy(io.Discard, source)
		return err
	})
	if err == nil {
		helper.calls++
	}
	return err
}

func runEntryRemovalAgent(
	t *testing.T,
	fixture *ExecutedArtifactFixture,
	plans *testtaskplanning.TaskPlanResolver,
	materials *testtaskmaterialization.TaskMaterializationResolver,
	task etcd.TaskRecord,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	agentID := ids.New(ids.KindAgent)
	claimed, found, err := fixture.Tasks.ClaimNextTask(ctx, agentID, 1, time.Now().UTC())
	if err != nil || !found || claimed.Task.Record.ID != task.ID {
		t.Fatalf("claim Entry cleanup: %v / %v", found, err)
	}
	task, assigned := claimed.Task.Record, claimed.Assignment.Record
	plan, err := plans.ResolveExecutionPlan(ctx, task)
	if err != nil {
		t.Fatal("reconstruct Entry cleanup", err)
	}
	helper := &entryRemovalHelper{}
	runtime, err := migratedmaterialization.New(helper, nil)
	if err != nil {
		t.Fatal(err)
	}
	pool := agent.NewWorkerPoolWithRuntimes(1, "/var/lib/groundplane/vol", nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil, runtime)
	done := make(chan struct{})
	go func() { pool.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	if err := pool.Submit(ctx, testtaskassignment.Assignment{AssignmentID: assigned.AssignmentID, TaskID: task.ID,
		OperationID: task.OperationID, Plan: plan, ExecutionEpoch: assigned.ExecutionEpoch,
		ExecutionMode:   agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD,
		ForwardDeadline: assigned.Deadline, RecoveryDeadline: assigned.RecoveryDeadline, Deadline: assigned.Deadline}); err != nil {
		t.Fatal("submit Entry cleanup", err)
	}
	for _, step := range plan.Steps {
		if step.GetMaterializeFile() == nil {
			t.Fatal("Entry cleanup attempted workload execution")
		}
		sendEntryRemovalMaterialization(t, ctx, pool, materials, task, assigned.AssignmentID, plan, step)
	}
	for {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case output := <-pool.Outputs():
			if output.Progress != nil {
				progress := output.Progress
				state, wireState := testtaskjournal.TaskEventStateRunning, agentpb.TaskState_TASK_STATE_RUNNING
				if progress.State == agent.TaskProgressCompleted {
					state, wireState = testtaskjournal.TaskEventStateCompleted, agentpb.TaskState_TASK_STATE_COMPLETED
				}
				if progress.State != agent.TaskProgressRunning && progress.State != agent.TaskProgressCompleted {
					t.Fatalf("Entry cleanup step failed: %+v", progress)
				}
				_, err := fixture.Tasks.AppendTaskEvent(
					ctx,
					testtaskjournal.TaskEventInput{Identity: testtaskjournal.TaskEventIdentity{
						AssignmentID: assigned.AssignmentID, AgentID: agentID, AgentGeneration: 1, TaskID: task.ID,
						StepID: progress.StepID, Attempt: progress.ExecutionEpoch, Ordinal: progress.Ordinal},
						State: state, Payload: json.RawMessage(`{}`)},
					time.Now().UTC(),
				)
				if err != nil {
					t.Fatal(err)
				}
				if err := pool.AcceptTaskEventAck(ctx, &agentpb.TaskEventAck{TaskId: task.ID, AssignmentId: assigned.AssignmentID,
					PlanHash: progress.PlanHash[:], StepId: progress.StepID, ExecutionEpoch: progress.ExecutionEpoch,
					Ordinal: progress.Ordinal, State: wireState}); err != nil {
					t.Fatal(err)
				}
			} else if output.Result != nil {
				result := output.Result
				if result.Terminal != agent.TaskTerminalCompleted || result.Compose == nil || result.EnvironmentDirectory != nil ||
					helper.calls == 0 || helper.calls != len(plan.Steps) {
					t.Fatalf("Entry cleanup did not complete: %+v / helper calls %d", result, helper.calls)
				}
				evidence := testtaskjournal.TaskResultRecord{Kind: testtaskjournal.TaskResultCompose, ExecutionEpoch: result.ExecutionEpoch,
					Diagnostic: testtaskjournal.TaskResultDiagnosticNone}
				finished := time.Now().UTC()
				for range 2 {
					if _, err := fixture.Tasks.AcknowledgeTask(ctx, agentID, 1, task.ID, assigned.AssignmentID, testtaskjournal.TaskStatusCompleted, evidence, finished); err != nil {
						t.Fatal("Entry cleanup terminal acknowledgement/replay", err)
					}
				}
				return
			} else {
				t.Fatal("Entry cleanup requested unrelated execution")
			}
		}
	}
}

func sendEntryRemovalMaterialization(t *testing.T, ctx context.Context, pool *agent.WorkerPool,
	resolver *testtaskmaterialization.TaskMaterializationResolver, task etcd.TaskRecord, assignmentID string,
	plan *agentpb.ExecutionPlan, step *agentpb.ExecutionStep) {
	t.Helper()
	source, err := resolver.ResolveMaterialization(ctx, task, plan, step)
	if err != nil {
		t.Fatal("resolve pinned cleanup bytes", err)
	}
	defer source.Close()
	material := step.GetMaterializeFile()
	outer := func() *agentpb.MaterializationTransfer {
		return &agentpb.MaterializationTransfer{TaskId: task.ID, AssignmentId: assignmentID,
			PlanHash: append([]byte(nil), plan.PlanHash...), StepId: step.StepId}
	}
	header := outer()
	header.Record = &agentpb.MaterializationTransfer_Header{Header: &agentpb.MaterializationTransferHeader{
		ArtifactId: material.ArtifactId, MaterializationId: material.MaterializationId,
		EnvironmentId: material.EnvironmentId, RenderGeneration: plan.RenderGeneration,
		Destination: material.Destination, ServiceId: material.ServiceId, ServiceName: material.ServiceName,
		OutputKind: material.OutputKind, Uid: material.Uid, Gid: material.Gid, Mode: material.Mode,
		Length: material.Length, Sha256: append([]byte(nil), material.Sha256...),
	}}
	if err := pool.AcceptMaterializationTransfer(ctx, header); err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(source)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(content)
	var sequence uint32
	for offset := 0; offset < len(content); offset += 32768 {
		sequence++
		chunk := outer()
		chunk.Record = &agentpb.MaterializationTransfer_Chunk{Chunk: &agentpb.MaterializationTransferChunk{
			Sequence: sequence, Content: append([]byte(nil), content[offset:min(offset+32768, len(content))]...),
		}}
		if err := pool.AcceptMaterializationTransfer(ctx, chunk); err != nil {
			t.Fatal(err)
		}
	}
	end := outer()
	end.Record = &agentpb.MaterializationTransfer_End{End: &agentpb.MaterializationTransferEnd{ChunkCount: sequence}}
	if err := pool.AcceptMaterializationTransfer(ctx, end); err != nil {
		t.Fatal(err)
	}
}
