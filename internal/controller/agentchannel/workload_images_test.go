package agentchannel

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func imageSelectors() []*agentpb.WorkloadImageSelector {
	return []*agentpb.WorkloadImageSelector{
		{Selector: &agentpb.WorkloadImageSelector_RequestedReference{RequestedReference: "app:dev"}},
	}
}

// Rationale: exhausted or unavailable correlation must not take Task traffic
// offline or prevent the Agent from reporting readiness.
func TestImageResolutionUnavailablePreservesSession(t *testing.T) {
	r := NewRegistry()
	s, err := r.Open(context.Background(), "agent", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r.mu.Lock()
	s.state.imageCounter = ids.ImageCorrelationCounter{}
	r.mu.Unlock()
	if _, err := r.ResolveWorkloadImages(context.Background(), "agent", imageSelectors()); !errors.Is(
		err,
		errs.New(errs.KindWorkloadImageResolutionUnavailable, ""),
	) {
		t.Fatalf("unavailable resolution=%v", err)
	}
	if err := s.RecordReady(time.Now(), 1, "1.0.0"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.Done():
		t.Fatal("unavailable image lookup closed session")
	default:
	}
}

func imageResult(request *agentpb.ResolveWorkloadImages) *agentpb.AgentMessage {
	return &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_WorkloadImageResolutionResult{
		WorkloadImageResolutionResult: &agentpb.WorkloadImageResolutionResult{RequestId: request.RequestId,
			Outcome: &agentpb.WorkloadImageResolutionResult_Success{Success: &agentpb.WorkloadImageResolutions{
				Resolutions: []*agentpb.WorkloadImageResolution{
					{Selector: request.Selectors[0], LocalImageId: "sha256:" + strings.Repeat("a", 64)},
				},
			}},
		},
	}}
}

// Rationale: only the currently active request on the exact authenticated
// connection can release publication preflight; stale ids and peers cannot.
func TestImageResolutionCorrelationAndBusy(t *testing.T) {
	registry := NewRegistry()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	session, err := registry.Open(ctx, "agent", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	done := make(chan error, 1)
	go func() { _, err := registry.ResolveWorkloadImages(ctx, "agent", imageSelectors()); done <- err }()
	var command *imageCommand
	select {
	case command = <-session.state.imageCommands:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := registry.ResolveWorkloadImages(ctx, "agent", imageSelectors()); !errors.Is(
		err,
		errs.New(errs.KindWorkloadImageResolutionBusy, ""),
	) {
		t.Fatalf("concurrent request error=%v", err)
	}
	stale := imageResult(command.request)
	stale.GetWorkloadImageResolutionResult().RequestId = strings.Repeat("0", 32)
	session.acceptImageResult(stale)
	select {
	case err := <-done:
		t.Fatalf("stale result satisfied request: %v", err)
	default:
	}
	session.acceptImageResult(imageResult(command.request))
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

// Rationale: replacing a connection must fail its pending lookup even when an
// old Agent subsequently supplies an otherwise valid response.
func TestImageResolutionReplacementRejectsOldResponse(t *testing.T) {
	registry := NewRegistry()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	old, err := registry.Open(ctx, "agent", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	done := make(chan error, 1)
	go func() { _, err := registry.ResolveWorkloadImages(ctx, "agent", imageSelectors()); done <- err }()
	var command *imageCommand
	select {
	case command = <-old.state.imageCommands:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	next, err := registry.Open(ctx, "agent", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	old.acceptImageResult(imageResult(command.request))
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("old connection result accepted")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

// Rationale: the real Controller stream loop must transport the exchange while
// no Task store exists; resolution is authenticated coordination, not execution.
func TestImageResolutionThroughControllerStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	registry := NewRegistry()
	stream := newLiveStream(ctx)
	stream.received <- authenticateMessage(testAgentID, testToken(4))
	serverDone := make(chan error, 1)
	go func() { serverDone <- New(authorizedAuthenticator(), registry, nil, nil).Connect(stream) }()
	select {
	case config := <-stream.sent:
		if config.GetConfigUpdate() == nil {
			t.Fatal("missing authenticated configuration")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	done := make(chan error, 1)
	go func() { _, err := registry.ResolveWorkloadImages(ctx, testAgentID, imageSelectors()); done <- err }()
	select {
	case message := <-stream.sent:
		request := message.GetResolveWorkloadImages()
		if request == nil {
			t.Fatal("missing image request")
		}
		stream.received <- imageResult(request)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cancel()
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("server did not stop")
	}
}

// Rationale: malformed active results fail the batch, and cancelling preflight
// releases its slot without allowing the retired request to satisfy a retry.
func TestImageResolutionMalformedAndCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r := NewRegistry()
	s, err := r.Open(ctx, "agent", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, malformed := range []bool{true, false} {
		requestCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { _, err := r.ResolveWorkloadImages(requestCtx, "agent", imageSelectors()); done <- err }()
		var command *imageCommand
		select {
		case command = <-s.state.imageCommands:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if malformed {
			message := imageResult(command.request)
			message.GetWorkloadImageResolutionResult().GetSuccess().Resolutions = nil
			s.acceptImageResult(message)
		} else {
			stop()
		}
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("failed lookup returned success")
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		stop()
		s.acceptImageResult(imageResult(command.request))
	}
}
