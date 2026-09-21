package app

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
	migratedcomposeruntime "github.com/AlanD20/groundplane/internal/agent/composeruntime"
	migratedenvironmentdirectory "github.com/AlanD20/groundplane/internal/agent/environmentdirectory"
	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/docker/environmentdirectoryhelper"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type volumeCreatePathHelper struct{ calls int }

func (helper *volumeCreatePathHelper) Execute(
	ctx context.Context,
	request *agentpb.EnvironmentDirectoryHelperRequest,
) (*agentpb.EnvironmentDirectoryHelperResponse, error) {
	return environmentdirectoryhelper.Execute(ctx, "/var/lib/groundplane/vol", helper, request)
}

func (*volumeCreatePathHelper) Create(context.Context, string, string) error {
	return errs.New(errs.KindInternal, "unexpected Environment creation")
}

func (helper *volumeCreatePathHelper) EnsureManagedVolumes(
	_ context.Context,
	request environmentdirectoryhelper.ManagedVolumeEnsureRequest,
) error {
	if len(request.Volumes) != 1 || request.Volumes[0].Key != "data" {
		return errs.New(errs.KindInternal, "unexpected Volume directory")
	}
	helper.calls++
	return nil
}

// Rationale: join actual create/edit publication, assignment, plan reconstruction,
// Agent execution and terminal acknowledgement. Only Docker/filesystem effects
// are faked, and any Compose/container command fails this resource-only journey.
func TestVolumeResourceProductionAgentCreatesThenVerifiesSlugEdit(t *testing.T) {
	fixture, mutations, reads, created := newPendingVolumeProductionJourney(t)
	ctx := t.Context()
	docker := runner.NewFake()
	createdDocker, inspections := 0, 0
	var inspection struct {
		Name    string
		Driver  string
		Labels  map[string]string
		Options map[string]string
	}
	docker.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
		if len(opts.Args) < 2 || opts.Args[0] != "volume" {
			return runner.Result{}, errs.New(errs.KindInternal, "unexpected workload command")
		}
		switch opts.Args[1] {
		case "inspect":
			inspections++
			if createdDocker == 0 {
				return runner.Result{ExitCode: 1}, errs.New(errs.KindStateConflict, "absent Volume")
			}
			value, err := json.Marshal(inspection)
			return runner.Result{Stdout: value}, err
		case "create":
			createdDocker++
			inspection.Name, inspection.Driver = opts.Args[len(opts.Args)-1], "local"
			inspection.Labels, inspection.Options = map[string]string{}, map[string]string{}
			for index := 2; index+1 < len(opts.Args); index++ {
				if opts.Args[index] != "--label" && opts.Args[index] != "--opt" {
					continue
				}
				key, value, ok := strings.Cut(opts.Args[index+1], "=")
				if !ok {
					return runner.Result{}, errs.New(errs.KindInternal, "invalid Volume option")
				}
				if opts.Args[index] == "--label" {
					inspection.Labels[key] = value
				} else {
					inspection.Options[key] = value
				}
				index++
			}
			return runner.Result{}, nil
		default:
			return runner.Result{}, errs.New(errs.KindInternal, "unexpected Docker mutation")
		}
	}
	path := &volumeCreatePathHelper{}
	runVolumeResourceAgentTask(t, fixture, created.TaskID, docker, path)
	active, err := reads.GetVolume(ctx, created.Volume.ID)
	if err != nil || active.State != "active" || createdDocker != 1 || path.calls != 1 {
		t.Fatalf(
			"create did not reach active: state=%s error=%v Docker=%d directory=%d",
			active.State,
			err,
			createdDocker,
			path.calls,
		)
	}
	if _, found, err := fixture.Blueprint.GetEnvironmentAppliedComposeProjection(ctx, fixture.EnvironmentID); err != nil ||
		found {
		t.Fatalf("resource-only creation fabricated applied workload authority: found=%t error=%v", found, err)
	}
	// Seed a prior acknowledged snapshot to exercise retention on the edit path.
	prior, found, err := fixture.Blueprint.GetEnvironmentComposeProjection(ctx, fixture.EnvironmentID)
	if err != nil || !found {
		t.Fatalf("read prior desired fixture: %v", err)
	}
	priorValue, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(prior.Record)
	if err != nil {
		t.Fatal(err)
	}
	appliedKey := testenvironmentprojection.EnvironmentComposeProjectionStorageKey(fixture.EnvironmentID)
	priorRevision, err := fixture.Store.Put(ctx, appliedKey, priorValue)
	if err != nil {
		t.Fatal(err)
	}
	response, err := mutations.EditVolume(
		ctx,
		created.Volume.ID,
		api.VolumeEdit{Slug: "renamed"},
		"018f3111-0000-7000-8000-000000000008",
	)
	if err != nil {
		t.Fatal(err)
	}
	var edited api.VolumeMutationResponse
	if err := json.Unmarshal(response.Body, &edited); err != nil {
		t.Fatal(err)
	}
	beforeInspect := inspections
	runVolumeResourceAgentTask(t, fixture, edited.TaskID, docker, path)
	after, err := reads.GetVolume(ctx, created.Volume.ID)
	if err != nil || after.State != "active" || after.Slug != "renamed" || after.Key != active.Key ||
		after.Path != active.Path || createdDocker != 1 || path.calls != 1 || inspections != beforeInspect+1 {
		t.Fatalf(
			"slug edit changed host data or failed: error=%v Docker=%d directory=%d inspections=%d",
			err,
			createdDocker,
			path.calls,
			inspections,
		)
	}
	applied, err := fixture.Store.Get(ctx, appliedKey)
	if err != nil || applied.Entry == nil || applied.Entry.ModRevision != priorRevision ||
		!bytes.Equal(applied.Entry.Value, priorValue) {
		t.Fatalf("slug edit changed prior applied authority: %v", err)
	}
	replay, err := mutations.EditVolume(
		ctx,
		created.Volume.ID,
		api.VolumeEdit{Slug: "renamed"},
		"018f3111-0000-7000-8000-000000000008",
	)
	if err != nil || !bytes.Equal(replay.Body, response.Body) {
		t.Fatalf("slug edit replay changed: %v", err)
	}
}

func runVolumeResourceAgentTask(
	t *testing.T,
	fixture *volumeRemovalProductionFixture,
	taskID string,
	docker *runner.FakeRunner,
	path *volumeCreatePathHelper,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	agentID := ids.New(ids.KindAgent)
	claimed, found, err := fixture.Tasks.ClaimNextTask(ctx, agentID, 1, time.Now().UTC())
	if err != nil || !found || claimed.Task.Record.ID != taskID {
		t.Fatalf("claim resource Task: %v found=%t", err, found)
	}
	task, assigned := claimed.Task.Record, claimed.Assignment.Record
	resolver, err := testtaskplanning.NewTaskPlanResolverWithBlueprints(
		"/var/lib/groundplane/vol",
		fixture.Blueprint,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := resolver.ResolveExecutionPlan(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	compose, err := migratedcomposeruntime.New(volumeAgentComposeHelper{docker}, volumeAgentObserver{})
	if err != nil {
		t.Fatal(err)
	}
	directories, err := migratedenvironmentdirectory.New(path)
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
	if err := pool.Submit(ctx, testtaskassignment.Assignment{AssignmentID: assigned.AssignmentID, TaskID: task.ID, OperationID: task.OperationID,
		Plan: plan, ExecutionEpoch: assigned.ExecutionEpoch, ExecutionMode: agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD,
		ForwardDeadline: assigned.Deadline, RecoveryDeadline: assigned.RecoveryDeadline, Deadline: assigned.Deadline}); err != nil {
		t.Fatal(err)
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
					t.Fatalf("resource step failed: %+v", progress)
				}
				_, err := fixture.Tasks.AppendTaskEvent(
					ctx,
					testtaskjournal.TaskEventInput{Identity: testtaskjournal.TaskEventIdentity{
						AssignmentID: assigned.AssignmentID, AgentID: agentID, AgentGeneration: 1, TaskID: task.ID, StepID: progress.StepID,
						Attempt: progress.ExecutionEpoch, Ordinal: progress.Ordinal}, State: state, Payload: json.RawMessage(`{}`)},
					time.Now().UTC(),
				)
				if err != nil {
					t.Fatal(err)
				}
				if err := pool.AcceptTaskEventAck(ctx, &agentpb.TaskEventAck{TaskId: task.ID, AssignmentId: assigned.AssignmentID,
					PlanHash: progress.PlanHash[:], StepId: progress.StepID, ExecutionEpoch: progress.ExecutionEpoch, Ordinal: progress.Ordinal, State: wireState}); err != nil {
					t.Fatal(err)
				}
			} else if output.Result != nil {
				result := output.Result
				if result.Terminal != agent.TaskTerminalCompleted || result.Compose == nil || result.EnvironmentDirectory != nil {
					t.Fatalf("resource Task did not complete: %+v", result)
				}
				_, err := fixture.Tasks.AcknowledgeTask(ctx, agentID, 1, task.ID, assigned.AssignmentID, testtaskjournal.TaskStatusCompleted, testtaskjournal.TaskResultRecord{Kind: testtaskjournal.TaskResultCompose, Diagnostic: testtaskjournal.TaskResultDiagnosticNone}, time.Now().UTC())
				if err != nil {
					t.Fatal(err)
				}
				return
			} else {
				t.Fatal("resource Task requested unrelated execution")
			}
		}
	}
}
