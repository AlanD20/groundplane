//go:build agenthostacceptance

package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/scriptruntime"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// TASK-86: run only in an isolated Agent state mount. The actual worker,
// process-ownership lock, disk journal, Script executor and native exchange run;
// no Controller Start is granted and any attempt at a host effect fails the run.
// This does not qualify executed Script commands or durable Controller settlement.
func TestNativeWorkersBoundCapacityUntilResultConsumption(t *testing.T) {
	f := newNativeWorkersHostFixture(t, 2)
	first, second := f.assignment(t), f.assignment(t)
	if err := f.workers.submitScript(proto.CloneOf(first), 2); err != nil {
		t.Fatal(err)
	}
	f.start(t, first)
	if err := f.workers.submitScript(proto.CloneOf(second), 1); err != nil {
		t.Fatal(err)
	}
	f.start(t, second)
	if f.workers.capacity() != 0 {
		t.Fatal("two native operations did not occupy the shared bound")
	}
	duplicate := proto.CloneOf(first)
	alias := duplicate.Body.Body
	if err := f.workers.submitScript(duplicate, 0); err != nil {
		t.Fatalf("exact redispatch consumed capacity or rejected: %v", err)
	}
	if !bytes.Equal(alias, make([]byte, len(alias))) || duplicate.Body.Body != nil {
		t.Fatal("redispatch retained received Script plaintext")
	}
	select {
	case <-f.exchange.outgoing:
		t.Fatal("redispatch started another execution")
	default:
	}
	third := f.assignment(t)
	rejected := proto.CloneOf(third)
	alias = rejected.Body.Body
	if err := f.workers.submitScript(rejected, 1); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("shared bound did not refuse more work: %v", err)
	}
	if !bytes.Equal(alias, make([]byte, len(alias))) || rejected.Body.Body != nil {
		t.Fatal("capacity rejection retained received Script plaintext")
	}
	stale := proto.CloneOf(first.Attempt)
	stale.AttemptId = ids.New(ids.KindAttempt)
	if err := f.workers.cancelAttempt(&agentpb.WorkCancel{Attempt: stale}); err != nil {
		t.Fatal(err)
	}
	firstAttempt, err := workexecution.DecodeAgentAttempt(first.Attempt, f.workers.identity)
	if err != nil {
		t.Fatal(err)
	}
	secondAttempt, err := workexecution.DecodeAgentAttempt(second.Attempt, f.workers.identity)
	if err != nil {
		t.Fatal(err)
	}
	if f.workers.executionContext(firstAttempt).Err() != nil || f.workers.executionContext(secondAttempt).Err() != nil {
		t.Fatal("stale cancellation stopped current native work")
	}
	if err := f.workers.cancelAttempt(&agentpb.WorkCancel{Attempt: first.Attempt}); err != nil {
		t.Fatal(err)
	}
	output := f.output(t)
	if !workexecution.SameAssignment(output.attempt, firstAttempt) || !errors.Is(output.err, context.Canceled) {
		t.Fatalf("canceled native output lost identity or failure: %v", output.err)
	}
	if f.workers.capacity() != 0 || f.workers.executionContext(secondAttempt).Err() != nil {
		t.Fatal("unconsumed result released capacity or canceled an unrelated worker")
	}
	wrong := output
	wrong.attempt.ID = ids.New(ids.KindAttempt)
	if err := f.workers.consume(wrong); err == nil || f.workers.capacity() != 0 {
		t.Fatal("a stale result released current capacity")
	}
	if err := f.workers.consume(output); err != nil || f.workers.capacity() != 1 {
		t.Fatalf("joined failed worker did not release exactly one local slot: %v", err)
	}
	if err := f.workers.consume(output); err == nil || f.workers.capacity() != 1 {
		t.Fatal("duplicate result consumption released another worker")
	}
	if err := f.workers.submitScript(proto.CloneOf(third), 1); err != nil {
		t.Fatalf("unrelated work could not reuse released capacity: %v", err)
	}
	f.start(t, third)
}

// TASK-86: stream replacement must join active native workers before process
// ownership can close. Wrong-host/stale input cannot replace a live assignment,
// and a closed worker session cannot start another operation.
func TestNativeWorkersShutdownJoinsAndReleasesProcessOwnership(t *testing.T) {
	f := newNativeWorkersHostFixture(t, 1)
	delivery := f.assignment(t)
	foreign := proto.CloneOf(delivery)
	foreign.Attempt.HostId = ids.New(ids.KindHost)
	if err := f.workers.submitScript(foreign, 1); err == nil || f.workers.capacity() != 1 {
		t.Fatal("wrong host acquired a native execution slot")
	}
	if err := f.workers.submitScript(proto.CloneOf(delivery), 1); err != nil {
		t.Fatal(err)
	}
	f.start(t, delivery)
	changed := proto.CloneOf(delivery)
	changed.Attempt.AttemptId = ids.New(ids.KindAttempt)
	if err := f.workers.submitScript(changed, 1); err == nil || f.workers.capacity() != 0 {
		t.Fatal("a different assignment replaced active work")
	}
	if err := f.ownership.Close(); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("process ownership closed while native work was executing: %v", err)
	}
	joined := make(chan struct{})
	go func() {
		f.workers.close()
		close(joined)
	}()
	select {
	case <-joined:
	case <-time.After(3 * time.Second):
		t.Fatal("native session shutdown did not cancel and join its worker")
	}
	if f.workers.capacity() != 0 {
		t.Fatal("closed session still advertises capacity")
	}
	if err := f.workers.submitScript(f.assignment(t), 1); err == nil {
		t.Fatal("closed session admitted work")
	}
	if err := f.ownership.Close(); err != nil {
		t.Fatalf("joined native worker retained process execution ownership: %v", err)
	}
}

type nativeWorkersHostFixture struct {
	workers   *executionWorkers
	exchange  *nativeExchange
	ownership *agentownership.Lease
}

// Unused effect methods deliberately have no implementation: granting Start in
// this submission/shutdown journey would panic instead of simulating success.
type nativeWorkersNoEffects struct{ scriptexecution.Engine }

func newNativeWorkersHostFixture(t *testing.T, limit int) *nativeWorkersHostFixture {
	t.Helper()
	if os.Getenv("GP_AGENT_WORKER_ACCEPTANCE") != "isolated-state" {
		t.Skip("requires a disposable isolated Agent state mount, not the host Agent directory")
	}
	ctx, cancel := context.WithCancel(t.Context())
	ownership, err := agentownership.Acquire(ctx)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		if err := ownership.Close(); err != nil {
			t.Error(err)
		}
	})
	journal, err := agentworkjournal.Open(filepath.Join(t.TempDir(), "work"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := journal.Close(); err != nil {
			t.Error(err)
		}
	})
	runtime, err := scriptruntime.New(nativeWorkersNoEffects{})
	if err != nil {
		t.Fatal(err)
	}
	scripts, err := scriptruntime.NewExecutor(runtime, journal)
	if err != nil {
		t.Fatal(err)
	}
	identity := workexecution.Identity{Executor: workexecution.Agent,
		HostID: ids.New(ids.KindHost), AgentID: ids.New(ids.KindAgent), Generation: 1}
	exchange := newNativeExchange(ctx, journal)
	exchange.identity = identity
	workers := &executionWorkers{ctx: ctx, cancel: cancel, identity: identity, limit: limit,
		scripts: scripts, exchange: exchange, ownership: ownership,
		active: make(map[string]activeExecution), outputs: make(chan executionOutput, limit)}
	t.Cleanup(func() { workers.close(); exchange.close() })
	return &nativeWorkersHostFixture{workers: workers, exchange: exchange, ownership: ownership}
}

func (f *nativeWorkersHostFixture) assignment(t *testing.T) *agentpb.ScriptWorkAssignment {
	t.Helper()
	at := time.Now().UTC()
	attempt := workexecution.Attempt{ID: ids.New(ids.KindAttempt), TaskID: ids.New(ids.KindTask),
		WorkItemID: ids.New(ids.KindWorkItem), Number: 1, Executor: f.workers.identity,
		Input:     workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)},
		Direction: workexecution.Forward, AssignedAt: at, Deadline: at.Add(time.Minute)}
	wire, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		t.Fatal(err)
	}
	executionID, snapshotID, scriptID := ids.NewULID(), ids.NewULID(), ids.New(ids.KindScript)
	body := []byte("echo captured-script\n")
	digest := sha256.Sum256(body)
	return &agentpb.ScriptWorkAssignment{Attempt: wire, OperationId: ids.New(ids.KindOperation),
		Run: &agentpb.RunScript{
			ScriptExecutionId: executionID,
			ScriptId:          scriptID,
			ScriptGeneration:  1,
			RunnerSnapshotId:  snapshotID,
		},
		Snapshot: &agentpb.ResolvedRunnerSnapshot{SnapshotId: snapshotID, ScriptExecutionId: executionID,
			LocalImageId: "sha256:" + strings.Repeat("b", 64)},
		Projection: &agentpb.ScriptRunnerProjection{SnapshotId: snapshotID, Image: "sha256:" + strings.Repeat("b", 64)},
		Body: &agentpb.ScriptBodyArtifact{Metadata: &agentpb.ScriptBodyArtifactMetadata{
			ScriptExecutionId: executionID, ScriptId: scriptID, Generation: 1, Size: uint32(len(body)), Sha256: digest[:]}, Body: body},
		Checkpoint: &agentpb.ScriptExecutionCheckpoint{ScriptExecutionId: executionID,
			State: agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED}}
}

func (f *nativeWorkersHostFixture) start(t *testing.T, assignment *agentpb.ScriptWorkAssignment) {
	t.Helper()
	select {
	case request := <-f.exchange.outgoing:
		if start := request.GetWorkStartRequest(); start == nil || !proto.Equal(start.Attempt, assignment.Attempt) {
			t.Fatal("worker did not request Start for the exact captured assignment")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not enter its Start acknowledgement boundary")
	}
}

func (f *nativeWorkersHostFixture) output(t *testing.T) executionOutput {
	t.Helper()
	select {
	case output := <-f.workers.outputs:
		return output
	case <-time.After(3 * time.Second):
		t.Fatal("native worker did not return its local execution result")
		return executionOutput{}
	}
}
