package etcd_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/agent"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/internal/infra/docker/environmentdirectoryhelper"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumeremoval"
	removal "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type volumeAgentComposeHelper struct{ runner *runner.FakeRunner }

func (helper volumeAgentComposeHelper) Execute(
	ctx context.Context,
	request *agentpb.ComposeHelperRequest,
) (*agentpb.ComposeHelperResponse, error) {
	return composehelper.Execute(ctx, helper.runner, request)
}

type volumeAgentObserver struct{}

func (volumeAgentObserver) Observe(context.Context, *agentpb.ExecutionPlan, string) (*agentpb.ObservedProject, error) {
	return nil, errs.New(errs.KindInternal, "unexpected Compose observation")
}

func (volumeAgentObserver) ObserveReleaseRestoration(
	context.Context,
	*agentpb.ExecutionPlan,
	string,
	string,
) (*agentpb.ObservedProject, error) {
	return nil, errs.New(errs.KindInternal, "unexpected release-restoration observation")
}

func (volumeAgentObserver) ObserveRestoration(
	context.Context,
	*executionplan.RestorationObservation,
) (*agentpb.ObservedProject, error) {
	return nil, errs.New(errs.KindInternal, "unexpected restoration observation")
}

type volumeAgentPathHelper struct {
	store etcd.Store
	calls int
}

func (helper *volumeAgentPathHelper) Execute(
	ctx context.Context,
	request *agentpb.EnvironmentDirectoryHelperRequest,
) (*agentpb.EnvironmentDirectoryHelperResponse, error) {
	pending, err := helper.store.Get(ctx, removal.PendingPathKey(request.OperationId))
	if err != nil || pending.Entry == nil || !bytes.Equal(pending.Entry.Value, request.VolumeRemovalPendingPath) {
		return nil, errs.New(errs.KindStateConflict, "helper call was not durably authorized")
	}
	return environmentdirectoryhelper.Execute(ctx, "/var/lib/groundplane/vol", helper, request)
}

func (*volumeAgentPathHelper) Create(context.Context, string, string) error {
	return errs.New(errs.KindInternal, "unexpected directory creation")
}

func (helper *volumeAgentPathHelper) RemoveManagedVolume(
	_ context.Context,
	request environmentdirectoryhelper.ManagedVolumeDirectoryRemoveRequest,
) (environmentdirectoryhelper.ManagedVolumeDirectoryRemoveResult, error) {
	helper.calls++
	if helper.calls == 1 && len(request.Cursor) == 0 {
		return environmentdirectoryhelper.ManagedVolumeDirectoryRemoveResult{
			MutationCount: 128,
			NextCursor:    []byte("next"),
		}, nil
	}
	if helper.calls == 2 && string(request.Cursor) == "next" {
		return environmentdirectoryhelper.ManagedVolumeDirectoryRemoveResult{MutationCount: 1, Complete: true}, nil
	}
	return environmentdirectoryhelper.ManagedVolumeDirectoryRemoveResult{}, errs.New(
		errs.KindInternal,
		"Volume traversal skipped a saved cursor",
	)
}

// Rationale: join the real DELETE, plan resolver, Agent worker and checkpoint
// service. Only Docker and filesystem effects are faked here; descriptor-relative
// filesystem behavior has its separate real-file helper regression.
func TestVolumeRemovalProductionAgentReachesTerminal(t *testing.T) {
	fixture, mutations, reads, created := newVolumeRemovalProductionJourney(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	impact, err := reads.GetVolumeDeletionImpact(ctx, created.Volume.ID, "", 40)
	if err != nil {
		t.Fatal(err)
	}
	response, err := mutations.RemoveVolume(
		ctx,
		created.Volume.ID,
		impact.ImpactToken,
		created.Volume.Key,
		"018f3111-0000-7000-8000-000000000007",
	)
	if err != nil {
		t.Fatal(err)
	}
	var accepted api.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil {
		t.Fatal(err)
	}
	agentID := ids.New(ids.KindAgent)
	claimed, found, err := fixture.Tasks.ClaimNextTask(ctx, agentID, 1, time.Now().UTC())
	if err != nil || !found || claimed.Task.Record.ID != accepted.TaskID {
		t.Fatalf("claim DELETE: %v", err)
	}
	task, assigned := claimed.Task.Record, claimed.Assignment.Record
	resolver, err := controller.NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", fixture.Blueprint, nil)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := volumeremoval.NewEvidenceRepository(fixture.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	if err := resolver.EnableVolumeRemovalPlans(evidence); err != nil {
		t.Fatal(err)
	}
	plan, err := resolver.ResolveExecutionPlan(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := volumeremoval.NewEnvironmentVolumeRemovalRuntimeRepository(fixture.Store)
	if err != nil {
		t.Fatal(err)
	}
	checkpoints, err := controller.NewVolumeRemovalCheckpointService(runtime)
	if err != nil {
		t.Fatal(err)
	}
	docker := runner.NewFake()
	docker.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
		command := strings.Join(opts.Args, " ")
		if strings.HasPrefix(command, "container ls ") || strings.HasPrefix(command, "volume ls ") {
			return runner.Result{}, nil
		}
		return runner.Result{}, errs.New(errs.KindInternal, "unexpected Docker effect")
	}
	compose, err := agent.NewComposeRuntime(volumeAgentComposeHelper{docker}, volumeAgentObserver{})
	if err != nil {
		t.Fatal(err)
	}
	path := &volumeAgentPathHelper{store: fixture.Store}
	directories, err := agent.NewEnvironmentDirectoryRuntime(path)
	if err != nil {
		t.Fatal(err)
	}
	pool := agent.NewWorkerPoolWithRuntimes(
		1,
		"/var/lib/groundplane/vol",
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		compose,
		directories,
		nil,
	)
	done := make(chan struct{})
	go func() { pool.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	if err := pool.Submit(ctx, agent.Assignment{AssignmentID: assigned.AssignmentID, TaskID: task.ID, OperationID: task.OperationID,
		Plan: plan, ExecutionEpoch: assigned.ExecutionEpoch, ExecutionMode: agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD,
		ForwardDeadline: assigned.Deadline, RecoveryDeadline: assigned.RecoveryDeadline, Deadline: assigned.Deadline}); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case output := <-pool.Outputs():
			if output.VolumeCheckpoint != nil {
				ack, err := checkpoints.CheckpointVolumeRemoval(ctx, agentID, 1, output.VolumeCheckpoint)
				if err != nil {
					t.Fatal(err)
				}
				if err := pool.AcceptVolumeRemovalCheckpointAck(ack); err != nil {
					t.Fatal(err)
				}
			} else if output.Progress != nil {
				progress := output.Progress
				state, wireState := etcd.TaskEventStateRunning, agentpb.TaskState_TASK_STATE_RUNNING
				if progress.State == agent.TaskProgressCompleted {
					state, wireState = etcd.TaskEventStateCompleted, agentpb.TaskState_TASK_STATE_COMPLETED
				}
				if progress.State != agent.TaskProgressRunning && progress.State != agent.TaskProgressCompleted {
					t.Fatalf("Agent step failed: %+v", progress)
				}
				_, err := fixture.Tasks.AppendTaskEvent(ctx, etcd.TaskEventInput{Identity: etcd.TaskEventIdentity{
					AssignmentID: assigned.AssignmentID, AgentID: agentID, AgentGeneration: 1, TaskID: task.ID, StepID: progress.StepID,
					Attempt: progress.ExecutionEpoch, Ordinal: progress.Ordinal}, State: state, Payload: json.RawMessage(`{}`)}, time.Now().UTC())
				if err != nil {
					t.Fatal(err)
				}
				if err := pool.AcceptTaskEventAck(ctx, &agentpb.TaskEventAck{TaskId: task.ID, AssignmentId: assigned.AssignmentID,
					PlanHash: progress.PlanHash[:], StepId: progress.StepID, ExecutionEpoch: progress.ExecutionEpoch, Ordinal: progress.Ordinal, State: wireState}); err != nil {
					t.Fatal(err)
				}
			} else if output.Result != nil {
				result := output.Result
				if result.Terminal != agent.TaskTerminalCompleted || result.EnvironmentDirectory == nil || !result.EnvironmentDirectory.Complete || result.Compose != nil || path.calls != 2 {
					t.Fatalf("Agent did not complete checkpointed removal: %+v calls=%d", result, path.calls)
				}
				_, err := fixture.Tasks.AcknowledgeTask(ctx, agentID, 1, task.ID, assigned.AssignmentID, etcd.TaskStatusCompleted,
					etcd.TaskResultRecord{Kind: etcd.TaskResultEnvironmentDirectory, Diagnostic: etcd.TaskResultDiagnosticNone}, time.Now().UTC())
				if err != nil {
					t.Fatal(err)
				}
				owner, err := fixture.Store.Get(ctx, removal.OwnerKey(task.Target))
				if err != nil || owner.Entry != nil {
					t.Fatalf("terminal removal retained ownership: %v", err)
				}
				return
			}
		}
	}
}
