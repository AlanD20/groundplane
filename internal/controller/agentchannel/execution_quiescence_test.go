package agentchannel

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// A successful reply is bound to a fresh nonce and pins the exact process
// until the assignment hold is released. A delayed result cannot qualify a
// later request or permit a reconnect between qualification and activation.
func TestExecutionQuiescenceFreshResponsePinsProcess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	registry, stream, agentID := nativeConnectedQueryChannel(t, ctx)
	resume, err := registry.PauseAssignments(ctx, agentID, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer resume()

	first := beginExecutionQuiescence(ctx, registry, agentID, 1)
	firstRequest := nextServiceWire(t, ctx, stream).GetCheckExecutionQuiescence()
	if firstRequest.GetRequestId() == "" {
		t.Fatal("execution qualification request has no nonce")
	}
	stream.received <- executionQuiescenceResult("stale-"+firstRequest.RequestId, false)
	stream.received <- executionQuiescenceResult(firstRequest.RequestId, true)
	if err := awaitExecutionQuiescence(t, ctx, first); err != nil {
		t.Fatal(err)
	}

	second := beginExecutionQuiescence(ctx, registry, agentID, 1)
	secondRequest := nextServiceWire(t, ctx, stream).GetCheckExecutionQuiescence()
	if secondRequest.GetRequestId() == "" || secondRequest.RequestId == firstRequest.RequestId {
		t.Fatal("execution qualification reused its nonce")
	}
	stream.received <- executionQuiescenceResult(secondRequest.RequestId, true)
	if err := awaitExecutionQuiescence(t, ctx, second); err != nil {
		t.Fatal(err)
	}

	if replacement, err := registry.Open(ctx, agentID, 1); replacement != nil || !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("qualified process replacement = session %v, error %v", replacement, err)
	}
	resume()
	replacement, err := registry.Open(ctx, agentID, 1)
	if err != nil {
		t.Fatal(err)
	}
	replacement.Close()
}

// Replacing the authenticated process before its response is accepted retires
// the observation. A response from the old stream cannot qualify the new one.
func TestExecutionQuiescenceRejectsReplacedSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	registry, stream, agentID := nativeConnectedQueryChannel(t, ctx)
	resume, err := registry.PauseAssignments(ctx, agentID, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer resume()

	result := beginExecutionQuiescence(ctx, registry, agentID, 1)
	request := nextServiceWire(t, ctx, stream).GetCheckExecutionQuiescence()
	if request == nil {
		t.Fatal("execution qualification was not sent")
	}
	replacement, err := registry.Open(ctx, agentID, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if err := awaitExecutionQuiescence(t, ctx, result); !errors.Is(
		err,
		errs.New(errs.KindRequestUnavailable, ""),
	) {
		t.Fatalf("replaced process qualification = %v", err)
	}
}

func TestExecutionQuiescenceRejectsDifferentDurableProcessNonce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	registry, _, agentID := nativeConnectedQueryChannel(t, ctx)
	resume, err := registry.PauseAssignments(ctx, agentID, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer resume()
	registry.mu.Lock()
	expected := registry.agents[agentID].processAuthentication.ProcessGeneration
	registry.mu.Unlock()
	expected[0]++
	if err := registry.CheckExecutionQuiescenceForProcess(ctx, agentID, 1, expected); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("different durable process nonce qualified current session: %v", err)
	}
}

// The proof is available only inside an assignment hold. Existing auxiliary
// work blocks it, and the same hold prevents a new auxiliary command or shell
// from racing the Agent's execution observation.
func TestExecutionQuiescenceRequiresHoldWithoutAuxiliaryWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	registry, stream, agentID := nativeConnectedQueryChannel(t, ctx)
	if err := registry.CheckExecutionQuiescence(ctx, agentID, 1); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("qualification without assignment hold = %v", err)
	}

	readCtx, cancelRead := context.WithCancel(ctx)
	defer cancelRead()
	read := beginServiceRead(readCtx, registry, agentID)
	if nextServiceWire(t, ctx, stream).GetObserveServices() == nil {
		t.Fatal("active service observation was not sent")
	}
	resume, err := registry.PauseAssignments(ctx, agentID, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer resume()
	if err := registry.CheckExecutionQuiescence(ctx, agentID, 1); !errors.Is(
		err,
		errs.New(errs.KindResourceInUse, ""),
	) {
		t.Fatalf("qualification with active auxiliary work = %v", err)
	}
	if _, err := registry.ResolveWorkloadImages(ctx, agentID, imageSelectors()); !errors.Is(
		err,
		errs.New(errs.KindResourceInUse, ""),
	) {
		t.Fatalf("new auxiliary image query during hold = %v", err)
	}
	if _, err := registry.ObserveServices(ctx, agentID, servingTargets()); !errors.Is(
		err,
		errs.New(errs.KindResourceInUse, ""),
	) {
		t.Fatalf("new service observation during hold = %v", err)
	}
	cancelRead()
	if reply := awaitServiceRead(t, ctx, read); !errors.Is(reply.err, context.Canceled) {
		t.Fatalf("active service observation cancellation = %v", reply.err)
	}

	if err := markAgentReady(registry, agentID); err != nil {
		t.Fatal(err)
	}
	if process, err := registry.OpenContainerShell(ctx, validTerminalOpen()); process != nil || !errors.Is(
		err,
		errs.New(errs.KindResourceInUse, ""),
	) {
		t.Fatalf("new terminal during hold = process %v, error %v", process, err)
	}
}

// An old Agent that cannot handle the new request closes its stream. The
// Controller reports unavailable and never converts silence into quiescence.
func TestExecutionQuiescenceUnsupportedAgentFailsClosed(t *testing.T) {
	streamCtx, stopStream := context.WithCancel(context.Background())
	registry, stream, agentID := nativeConnectedQueryChannel(t, streamCtx)
	queryCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resume, err := registry.PauseAssignments(queryCtx, agentID, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer resume()

	result := beginExecutionQuiescence(queryCtx, registry, agentID, 1)
	if nextServiceWire(t, queryCtx, stream).GetCheckExecutionQuiescence() == nil {
		t.Fatal("execution qualification was not sent")
	}
	stopStream()
	if err := awaitExecutionQuiescence(t, queryCtx, result); !errors.Is(
		err,
		errs.New(errs.KindRequestUnavailable, ""),
	) {
		t.Fatalf("unsupported Agent qualification = %v", err)
	}
}

func beginExecutionQuiescence(
	ctx context.Context,
	registry *Registry,
	agentID string,
	generation uint64,
) <-chan error {
	result := make(chan error, 1)
	go func() { result <- registry.CheckExecutionQuiescence(ctx, agentID, generation) }()
	return result
}

func awaitExecutionQuiescence(t *testing.T, ctx context.Context, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return nil
	}
}

func executionQuiescenceResult(requestID string, quiescent bool) *agentpb.AgentMessage {
	return &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ExecutionQuiescenceResult{
		ExecutionQuiescenceResult: &agentpb.ExecutionQuiescenceResult{
			RequestId: requestID,
			Quiescent: quiescent,
		},
	}}
}

func markAgentReady(registry *Registry, agentID string) error {
	registry.mu.Lock()
	state := registry.agents[agentID]
	registry.mu.Unlock()
	if state == nil {
		return errs.New(errs.KindInternal, "test Agent session is missing")
	}
	session := &Session{registry: registry, agentID: agentID, state: state, ctx: context.Background()}
	return session.RecordReady(time.Now().UTC(), 0, "qa", buildversion.BuildProvenance{})
}

func validTerminalOpen() *agentpb.ContainerTerminalOpen {
	return &agentpb.ContainerTerminalOpen{
		SessionId:              ids.New(ids.KindTerminal),
		Sources:                servingTargets(),
		ContainerId:            strings.Repeat("a", 64),
		User:                   "root",
		Command:                []string{"/bin/sh"},
		Columns:                80,
		Rows:                   24,
		InputInactivitySeconds: 60,
		InitialCredit:          1,
	}
}
