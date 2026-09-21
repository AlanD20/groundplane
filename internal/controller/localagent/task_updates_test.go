package localagent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a cold process must close admission for the original, candidate
// and rollback generations before opening the Agent channel, even after expiry.
func TestTaskUpdatesStartupHoldCoversReconnectAndFutureGenerations(t *testing.T) {
	trace := &traceLog{}
	harness := newTestManager(t, seededRepository(trace, PhaseReady), trace)
	registry := agentchannel.NewRegistry()
	updates := testTaskUpdates(t, harness.manager, registry)
	claim := updateClaim(testNow.Add(-time.Hour))
	if err := updates.Restore(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	for generation := initialGeneration; generation <= initialGeneration+2; generation++ {
		session, err := registry.Open(context.Background(), testAgentID, generation)
		if err != nil {
			t.Fatal(err)
		}
		if session.AssignmentsAllowed() {
			t.Fatal("cold Task admission reopened before recovery")
		}
		if err := session.RecordReady(testNow, 1, "test"); err != nil {
			t.Fatal(err)
		}
		ready, err := registry.Ready(context.Background(), testAgentID, generation)
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-ready:
		default:
			t.Fatal("admission hold prevented authenticated readiness")
		}
		session.Close()
	}
}

// Rationale: an expired untouched Task must fence delayed publication, retain
// the old runtime, acknowledge timeout only after readiness, and resume dispatch.
func TestTaskUpdatesExpiredUnchangedTaskRecoversBeforeReleasing(t *testing.T) {
	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	harness := newTestManager(t, repository, trace)
	registry := agentchannel.NewRegistry()
	updates := testTaskUpdates(t, harness.manager, registry)
	claim := updateClaim(testNow.Add(-time.Hour))
	if err := updates.Restore(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	session, err := registry.Open(context.Background(), testAgentID, initialGeneration)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	status, err := updates.Execute(context.Background(), claim.Task.Record, claim.Assignment.Record.Deadline)
	if err != nil || status != testtaskjournal.TaskStatusTimedOut || !session.AssignmentsAllowed() ||
		harness.runtime.generateCalls != 0 {
		t.Fatalf("expired recovery = %s, %v, dispatch %t", status, err, session.AssignmentsAllowed())
	}
}

// Rationale: an expired partially installed candidate must restore the pinned
// predecessor with fresh authority before the runner may discard its claim.
func TestTaskUpdatesExpiredPartialCandidateRestoresBeforeTimeout(t *testing.T) {
	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	repository.record.Record.Phase = PhaseUpdating
	repository.record.Record.Generation++
	repository.record.Record.Image = replacementTestImage
	harness := newTestManager(t, repository, trace)
	updates := testTaskUpdates(t, harness.manager, agentchannel.NewRegistry())
	claim := updateClaim(testNow.Add(-time.Hour))
	if err := updates.Restore(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	status, err := updates.Execute(context.Background(), claim.Task.Record, claim.Assignment.Record.Deadline)
	if err != nil || status != testtaskjournal.TaskStatusTimedOut || repository.record.Record.Phase != PhaseReady ||
		repository.record.Record.Image != testImage || repository.record.Record.Generation != initialGeneration+2 ||
		harness.runtime.generateCalls != 1 {
		t.Fatalf("partial recovery = %s, %v, generation %d", status, err, repository.record.Record.Generation)
	}
}

// Rationale: storage outages leave the claim's safety hold intact; restored
// storage can settle that same operation without a new id or an extra rotation.
func TestTaskUpdatesUnresolvedRecoveryRetainsHoldUntilStorageReturns(t *testing.T) {
	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	harness := newTestManager(t, repository, trace)
	uncertain := &uncertainReplacementRepository{fakeRepository: repository, resolutionUnavailable: true}
	harness.manager.repository = uncertain
	registry := agentchannel.NewRegistry()
	updates := testTaskUpdates(t, harness.manager, registry)
	claim := updateClaim(testNow.Add(-time.Hour))
	if err := updates.Restore(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	session, err := registry.Open(context.Background(), testAgentID, initialGeneration)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	status, err := updates.Execute(context.Background(), claim.Task.Record, claim.Assignment.Record.Deadline)
	if !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) || status != "" || session.AssignmentsAllowed() {
		t.Fatalf("unresolved recovery = %s, %v, dispatch %t", status, err, session.AssignmentsAllowed())
	}
	uncertain.resolutionUnavailable = false
	status, err = updates.Execute(context.Background(), claim.Task.Record, claim.Assignment.Record.Deadline)
	if err != nil || status != testtaskjournal.TaskStatusTimedOut || !session.AssignmentsAllowed() {
		t.Fatalf("resolved recovery = %s, %v, dispatch %t", status, err, session.AssignmentsAllowed())
	}
}

// Rationale: a candidate which durably reached Ready before a lost Task ACK
// must not be replaced again by a restarted runner with an expired claim.
func TestTaskUpdatesColdReadyReplayDoesNotRollBackOnExpiredAcknowledgement(t *testing.T) {
	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	repository.record.Record.Image = replacementTestImage
	repository.record.Record.Generation++
	harness := newTestManager(t, repository, trace)
	updates := testTaskUpdates(t, harness.manager, agentchannel.NewRegistry())
	claim := updateClaim(testNow.Add(-time.Hour))
	if err := updates.Restore(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		status, err := updates.Execute(context.Background(), claim.Task.Record, claim.Assignment.Record.Deadline)
		if err != nil || status != testtaskjournal.TaskStatusCompleted || harness.runtime.generateCalls != 0 {
			t.Fatalf("ready replay = %s, %v, rotations %d", status, err, harness.runtime.generateCalls)
		}
	}
}

// Rationale: an Abort that wins the qualification race cannot be reported as
// completed; recovery retains the hold and restores the predecessor first.
func TestTaskUpdatesAbortWinsReadyQualificationRace(t *testing.T) {
	registry := agentchannel.NewRegistry()
	lifecycle := &abortAtQualificationLifecycle{}
	updates := testTaskUpdates(t, lifecycle, registry)
	lifecycle.abort = updates.Abort
	claim := updateClaim(testNow)
	if err := updates.Restore(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	lifecycle.taskID = claim.Task.Record.ID
	status, err := updates.Execute(context.Background(), claim.Task.Record, claim.Assignment.Record.Deadline)
	if err != nil || status != testtaskjournal.TaskStatusAborted || !lifecycle.restored {
		t.Fatalf("abort race = %s, %v, restored %t", status, err, lifecycle.restored)
	}
}

// Rationale: durable task parameters cannot wrap the rollback generation or
// switch target, execution owner, or image while restoring startup authority.
func TestDecodeUpdateTaskRejectsMalformedRecoveryAuthority(t *testing.T) {
	for _, scenario := range []string{"overflow", "target", "executor", "extra", "same-image"} {
		t.Run(scenario, func(t *testing.T) {
			task := updateClaim(testNow).Task.Record
			switch scenario {
			case "overflow":
				task.Params["starting_generation"] = "18446744073709551614"
			case "target":
				task.Target = ids.New(ids.KindService)
			case "executor":
				task.Executor = testtaskjournal.TaskExecutorAgent
			case "extra":
				task.Params["extra"] = "ignored"
			case "same-image":
				task.Params["image"] = task.Params["previous_image"]
			}
			if _, err := DecodeUpdateTask(task); err == nil {
				t.Fatal("malformed recovery authority was accepted")
			}
		})
	}
}

func testTaskUpdates(t *testing.T, agents UpdateLifecycle, sessions UpdateAdmission) *TaskUpdates {
	t.Helper()
	updates, err := NewTaskUpdates(agents, sessions, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	updates.now = func() time.Time { return testNow }
	return updates
}

func updateClaim(started time.Time) etcd.TaskAssignment {
	taskID := ids.New(ids.KindTask)
	return etcd.TaskAssignment{
		Task: testkeyvalue.Versioned[etcd.TaskRecord]{Record: etcd.TaskRecord{
			ID: taskID, Executor: testtaskjournal.TaskExecutorController, Type: testtaskjournal.TaskUpdate,
			Target: testAgentID, Params: map[string]string{testtaskjournal.TaskResourceKindParam: testtaskjournal.TaskResourceAgent, "image": replacementTestImage,
				"previous_image": testImage, "starting_generation": "1",
			},
		}},
		Assignment: testkeyvalue.Versioned[testtaskassignments.TaskAssignmentRecord]{
			Record: testtaskassignments.TaskAssignmentRecord{
				TaskID: taskID, Executor: testtaskjournal.TaskExecutorController,
				AssignedAt: started, Deadline: started.Add(300 * time.Second),
			},
		},
	}
}

type abortAtQualificationLifecycle struct {
	abort    func(context.Context, string) error
	taskID   string
	restored bool
}

func (lifecycle *abortAtQualificationLifecycle) RecoverUpdate(
	ctx context.Context, _ UpdateRequest, goal UpdateGoal,
) (UpdateResult, error) {
	if goal == UpdateRestore {
		lifecycle.restored = true
		return UpdateReadyPrevious, nil
	}
	if err := lifecycle.abort(ctx, lifecycle.taskID); err != nil {
		return "", err
	}
	return UpdateReadyDesired, nil
}

func (*abortAtQualificationLifecycle) Health(context.Context, string) (Health, error) {
	return Health{}, errs.New(errs.KindInternal, "unexpected health lookup")
}

// Rationale: settled Task cleanup must not release another owner's pause.
func TestTaskUpdatesSettlementPreservesIndependentPause(t *testing.T) {
	trace := &traceLog{}
	harness := newTestManager(t, seededRepository(trace, PhaseReady), trace)
	registry := agentchannel.NewRegistry()
	other, err := registry.PauseAssignments(context.Background(), testAgentID, math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	defer other()
	updates := testTaskUpdates(t, harness.manager, registry)
	claim := updateClaim(testNow.Add(-time.Hour))
	if err := updates.Restore(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	session, err := registry.Open(context.Background(), testAgentID, initialGeneration)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if _, err := updates.Execute(context.Background(), claim.Task.Record, claim.Assignment.Record.Deadline); err != nil {
		t.Fatal(err)
	}
	if session.AssignmentsAllowed() {
		t.Fatal("Task completion reopened another owner's admission pause")
	}
}
