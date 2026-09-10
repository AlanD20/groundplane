package controllerupgrade

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the predecessor may only commit the handoff; the candidate must
// resume the same Task, authenticate its Agent and qualify before completion.
func TestCoordinatorHandsOffThenQualifiesSameTask(t *testing.T) {
	h := newCoordinatorHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	h.unit.launch = cancel
	old := h.coordinator(t, h.input.PreviousController)
	if err := old.Restore(ctx, h.claim); err != nil {
		t.Fatal(err)
	}
	status, err := old.Execute(ctx, h.claim.Task.Record, h.claim.Assignment.Record.Deadline)
	if !errors.Is(err, context.Canceled) || status != "" || h.store.current().Phase != upgrade.PhaseActivating ||
		len(h.agents.goals) != 0 {
		t.Fatalf("handoff = %s, %v, journal %s", status, err, h.store.current().Phase)
	}
	h.store.startCandidate()
	candidate := h.coordinator(t, h.input.Manifest.ControllerSHA256)
	if err := candidate.Restore(context.Background(), h.claim); err != nil {
		t.Fatal(err)
	}
	status, err = candidate.Execute(context.Background(), h.claim.Task.Record, h.claim.Assignment.Record.Deadline)
	if err != nil || status != etcd.TaskStatusCompleted || h.store.current().Phase != upgrade.PhaseHealthy ||
		len(h.agents.goals) != 1 || h.agents.goals[0] != localagent.UpdateFinish {
		t.Fatalf("qualification = %s, %v, journal %s", status, err, h.store.current().Phase)
	}
}

// Rationale: active application work cannot be aborted or replaced to make an
// upgrade proceed. A bounded failed drain releases only its admission hold.
func TestCoordinatorBusyDrainLeavesRuntimeAndDispatchUntouched(t *testing.T) {
	h := newCoordinatorHarness(t)
	coordinator := h.coordinator(t, h.input.PreviousController)
	h.work.busy = true
	h.work.onCheck = func() { h.now = h.now.Add(121 * time.Second) }
	session, err := h.sessions.Open(
		context.Background(), h.input.Agent.ID, h.input.Agent.Generation,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := coordinator.Restore(context.Background(), h.claim); err != nil {
		t.Fatal(err)
	}
	status, err := coordinator.Execute(context.Background(), h.claim.Task.Record, h.claim.Assignment.Record.Deadline)
	if err != nil || status != etcd.TaskStatusFailed || h.store.found || h.unit.launches != 0 ||
		len(h.agents.goals) != 0 || !session.AssignmentsAllowed() {
		t.Fatalf("busy preparation = %s, %v, dispatch %t", status, err, session.AssignmentsAllowed())
	}
}

// Rationale: candidate failure cannot claim success or rely on candidate code
// to restore the old executable. The predecessor resumes Agent recovery later.
func TestCoordinatorBadCandidateRequestsNativeRollbackAndPredecessorRecovers(t *testing.T) {
	h := newCoordinatorHarness(t)
	h.store.prepareExpected(h.expected)
	h.store.startCandidate()
	h.agents.err = errs.New(errs.KindTaskTimedOut, "candidate not ready")
	ctx, cancel := context.WithCancel(context.Background())
	h.unit.launch = cancel
	candidate := h.coordinator(t, h.input.Manifest.ControllerSHA256)
	if err := candidate.Restore(ctx, h.claim); err != nil {
		t.Fatal(err)
	}
	status, err := candidate.Execute(ctx, h.claim.Task.Record, h.claim.Assignment.Record.Deadline)
	if !errors.Is(err, context.Canceled) || status != "" || h.store.current().Phase != upgrade.PhaseRollingBack {
		t.Fatalf("candidate failure = %s, %v, journal %s", status, err, h.store.current().Phase)
	}
	h.store.restorePredecessor()
	h.agents.err = nil
	predecessor := h.coordinator(t, h.input.PreviousController)
	if err := predecessor.Restore(context.Background(), h.claim); err != nil {
		t.Fatal(err)
	}
	status, err = predecessor.Execute(context.Background(), h.claim.Task.Record, h.claim.Assignment.Record.Deadline)
	if err != nil || status != etcd.TaskStatusFailed || h.store.current().Phase != upgrade.PhaseRecovered ||
		h.agents.goals[len(h.agents.goals)-1] != localagent.UpdateRestore {
		t.Fatalf("predecessor recovery = %s, %v, journal %s", status, err, h.store.current().Phase)
	}
}

// Rationale: Abort and activation must have one winner. A cancelled prepared
// operation never launches, while committed activation cannot cancel recovery.
func TestCoordinatorAbortHasOneWinnerWithActivation(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "prepared", true: "activated"}[committed], func(t *testing.T) {
			h := newCoordinatorHarness(t)
			h.store.prepareExpected(h.expected)
			if committed {
				h.store.journal.Phase = upgrade.PhaseActivating
			}
			coordinator := h.coordinator(t, h.input.PreviousController)
			if err := coordinator.Restore(context.Background(), h.claim); err != nil {
				t.Fatal(err)
			}
			err := coordinator.Abort(context.Background(), h.claim.Task.Record.ID)
			if committed {
				if !errors.Is(err, errs.New(errs.KindResourceInUse, "")) ||
					h.store.current().Phase != upgrade.PhaseActivating {
					t.Fatalf("committed abort = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			status, err := coordinator.Execute(
				context.Background(),
				h.claim.Task.Record,
				h.claim.Assignment.Record.Deadline,
			)
			if err != nil || status != etcd.TaskStatusAborted || h.store.current().Phase != upgrade.PhaseCancelled ||
				h.unit.launches != 0 {
				t.Fatalf("prepared abort = %s, %v", status, err)
			}
		})
	}
}

// Rationale: an expired native Task must retain its claim until both executable
// and Agent recovery are proved; a storage outage cannot erase that authority.
func TestCoordinatorExpiredRecoveryKeepsUnresolvedClaim(t *testing.T) {
	h := newCoordinatorHarness(t)
	h.store.prepareExpected(h.expected)
	h.store.restorePredecessor()
	h.now = h.expected.Deadline.Add(time.Hour)
	h.agents.err = errs.New(errs.KindStorageUnavailable, "recovery unavailable")
	coordinator := h.coordinator(t, h.input.PreviousController)
	if err := coordinator.Restore(context.Background(), h.claim); err != nil {
		t.Fatal(err)
	}
	status, err := coordinator.Execute(context.Background(), h.claim.Task.Record, h.claim.Assignment.Record.Deadline)
	if !errors.Is(err, h.agents.err) || status != "" || h.store.current().Phase != upgrade.PhaseRolledBack {
		t.Fatalf("unresolved late recovery = %s, %v", status, err)
	}
	h.agents.err = nil
	status, err = coordinator.Execute(context.Background(), h.claim.Task.Record, h.claim.Assignment.Record.Deadline)
	if err != nil || status != etcd.TaskStatusFailed || h.store.current().Phase != upgrade.PhaseRecovered {
		t.Fatalf("resolved late recovery = %s, %v", status, err)
	}
}

type coordinatorHarness struct {
	input    Input
	expected upgrade.Journal
	claim    etcd.TaskAssignment
	store    *coordinatorStore
	unit     *coordinatorUnit
	agents   *coordinatorAgents
	work     *coordinatorWork
	now      time.Time
	sessions *agentchannel.Registry
}

func newCoordinatorHarness(t *testing.T) *coordinatorHarness {
	t.Helper()
	now := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	input := nativeTestInput(t)
	task, err := NewTask(now, "native-update-key", input)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := DecodeTask(task, now, now.Add(600*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return &coordinatorHarness{input: input, expected: expected, now: now,
		claim: etcd.TaskAssignment{Task: etcd.Versioned[etcd.TaskRecord]{Record: task},
			Assignment: etcd.Versioned[etcd.TaskAssignmentRecord]{Record: etcd.TaskAssignmentRecord{
				TaskID: task.ID, Executor: etcd.TaskExecutorController, AssignedAt: now, Deadline: expected.Deadline,
			}}},
		store: &coordinatorStore{manifest: input.Manifest, installed: input.PreviousController},
		unit:  &coordinatorUnit{}, work: &coordinatorWork{},
		agents: &coordinatorAgents{agent: localagent.Agent{ID: input.Agent.ID, Image: input.Agent.Image,
			Generation: input.Agent.Generation, Phase: localagent.PhaseReady,
			Config: localagent.Config{MaxConcurrentTasks: 1, PullIntervalSeconds: 5}}},
	}
}

func (h *coordinatorHarness) coordinator(t *testing.T, process upgrade.Digest) *Coordinator {
	t.Helper()
	ready, err := NewReadiness(&readinessStorage{})
	if err != nil {
		t.Fatal(err)
	}
	ready.MarkHTTPReady()
	ready.MarkChannelReady()
	h.sessions = agentchannel.NewRegistry()
	coordinator, err := NewCoordinator(Dependencies{Journal: h.store, Unit: h.unit, Agents: h.agents,
		Work: h.work, Sessions: h.sessions, Readiness: ready, ProcessDigest: process,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.now = func() time.Time { return h.now }
	return coordinator
}

type coordinatorStore struct {
	mu        sync.Mutex
	journal   upgrade.Journal
	found     bool
	manifest  upgrade.Manifest
	installed upgrade.Digest
}

func (store *coordinatorStore) Current(context.Context) (upgrade.Journal, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.journal, store.found, nil
}
func (store *coordinatorStore) current() upgrade.Journal {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.journal
}
func (store *coordinatorStore) prepareExpected(journal upgrade.Journal) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.journal, store.found = journal, true
}
func (store *coordinatorStore) Prepare(_ context.Context, journal upgrade.Journal) error {
	store.prepareExpected(journal)
	return nil
}
func (store *coordinatorStore) Inspect(context.Context, upgrade.Digest) (upgrade.Manifest, error) {
	return store.manifest, nil
}
func (store *coordinatorStore) Installed(context.Context) (upgrade.Digest, error) {
	return store.installed, nil
}
func (store *coordinatorStore) Advance(_ context.Context, id string, from, to upgrade.Phase) (upgrade.Journal, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.journal.TaskID != id || store.journal.Phase != from || !from.CanAdvance(to) {
		return upgrade.Journal{}, errs.New(errs.KindStateConflict, "journal changed")
	}
	store.journal.Phase = to
	return store.journal, nil
}
func (store *coordinatorStore) startCandidate() {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.journal.Phase = upgrade.PhaseStarting
	store.journal.TrialBootID = "12345678-1234-1234-1234-123456789abc"
	store.installed = store.manifest.ControllerSHA256
}
func (store *coordinatorStore) restorePredecessor() {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.journal.Phase = upgrade.PhaseRolledBack
	store.installed = store.journal.PreviousController
}

type coordinatorUnit struct {
	launches int
	launch   func()
}

func (*coordinatorUnit) VerifyBootstrap(context.Context) error { return nil }
func (unit *coordinatorUnit) Launch(context.Context, string) error {
	unit.launches++
	if unit.launch != nil {
		unit.launch()
	}
	return nil
}

type coordinatorAgents struct {
	agent localagent.Agent
	goals []localagent.UpdateGoal
	err   error
}

func (agents *coordinatorAgents) ListHealth(context.Context) ([]localagent.Health, error) {
	return []localagent.Health{{Agent: agents.agent, Healthy: true}}, nil
}
func (agents *coordinatorAgents) Health(context.Context, string) (localagent.Health, error) {
	return localagent.Health{Agent: agents.agent, Healthy: true}, nil
}
func (agents *coordinatorAgents) RecoverUpdate(
	_ context.Context, request localagent.UpdateRequest, goal localagent.UpdateGoal,
) (localagent.UpdateResult, error) {
	agents.goals = append(agents.goals, goal)
	if agents.err != nil {
		return "", agents.err
	}
	if goal == localagent.UpdateRestore {
		agents.agent.Image, agents.agent.Generation = request.PreviousImage, request.StartingGeneration+2
		return localagent.UpdateReadyPrevious, nil
	}
	agents.agent.Image, agents.agent.Generation = request.DesiredImage, request.StartingGeneration+1
	return localagent.UpdateReadyDesired, nil
}

type coordinatorWork struct {
	busy    bool
	onCheck func()
}

func (work *coordinatorWork) RequireIdle(context.Context, string, uint64, int32) error {
	if work.onCheck != nil {
		work.onCheck()
	}
	if work.busy {
		return errs.New(errs.KindResourceInUse, "active work")
	}
	return nil
}
