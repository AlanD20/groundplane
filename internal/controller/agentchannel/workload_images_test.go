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

// QA: BP-08, SVC-05/10, HOST-05; local lookup refusal and Ready, not publication or Docker.
// Rationale: unavailable correlation must preserve the authenticated session
// and allow it to report readiness without leaving a deferred image request.
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
	if result, err := r.ResolveWorkloadImages(context.Background(), "agent", imageSelectors()); result != nil ||
		!errors.Is(
			err,
			errs.New(errs.KindWorkloadImageResolutionUnavailable, ""),
		) {
		t.Fatalf("unavailable resolution=%v", err)
	}
	select {
	case <-s.state.imageCommands:
		t.Fatal("unavailable lookup queued transport work")
	default:
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

// QA: BP-08, SVC-05/10; local busy/correlation and result identity, not image inspection or staging.
// Rationale: only the active request id may complete lookup; a stale response
// cannot satisfy it, and success must return the selected image identity.
func TestImageResolutionCorrelationAndBusy(t *testing.T) {
	registry := NewRegistry()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	session, err := registry.Open(ctx, "agent", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	done := beginImageRead(ctx, registry, "agent")
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
	case reply := <-done:
		t.Fatalf("stale result satisfied request: %v", reply)
	default:
	}
	session.acceptImageResult(imageResult(command.request))
	assertImageReply(t, awaitImageRead(t, ctx, done), command.request.RequestId)
}

// QA: HOST-05, BP-08, SVC-05/10; local session fencing, not real reconnect or preflight publication.
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
	done := beginImageRead(ctx, registry, "agent")
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
	if reply := awaitImageRead(t, ctx, done); reply.result != nil ||
		!errors.Is(reply.err, errs.New(errs.KindWorkloadImageResolutionUnavailable, "")) {
		t.Fatalf("replaced lookup = %v", reply)
	}
}

// QA: BP-08, SVC-05/10; real Controller loop over an in-memory stream, not gRPC or Docker.
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
	done := beginImageRead(ctx, registry, testAgentID)
	var requestID string
	select {
	case message := <-stream.sent:
		request := message.GetResolveWorkloadImages()
		if request == nil {
			t.Fatal("missing image request")
		}
		if len(request.Selectors) != 1 || request.Selectors[0].GetRequestedReference() != "app:dev" {
			t.Fatalf("wire selectors = %v", request.Selectors)
		}
		requestID = request.RequestId
		stream.received <- imageResult(request)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	assertImageReply(t, awaitImageRead(t, ctx, done), requestID)
	cancel()
	select {
	case err := <-serverDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not stop")
	}
}

// QA: BP-08, SVC-05/10; local error classification, late-response fencing and slot reuse only.
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
	var retired *agentpb.AgentMessage
	for _, mode := range []string{"malformed", "canceled", "retry"} {
		requestCtx, stop := context.WithCancel(ctx)
		done := beginImageRead(requestCtx, r, "agent")
		var command *imageCommand
		select {
		case command = <-s.state.imageCommands:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if retired != nil {
			s.acceptImageResult(retired)
			select {
			case reply := <-done:
				t.Fatalf("retired request satisfied %s: %v", mode, reply)
			default:
			}
		}
		var wantErr error
		switch mode {
		case "malformed":
			message := imageResult(command.request)
			message.GetWorkloadImageResolutionResult().GetSuccess().Resolutions = nil
			s.acceptImageResult(message)
			wantErr = errs.New(errs.KindValidationFailed, "")
		case "canceled":
			stop()
			wantErr = context.Canceled
		case "retry":
			s.acceptImageResult(imageResult(command.request))
		}
		reply := awaitImageRead(t, ctx, done)
		if mode == "retry" {
			assertImageReply(t, reply, command.request.RequestId)
		} else if reply.result != nil || !errors.Is(reply.err, wantErr) {
			t.Fatalf("%s lookup = %v, want %v", mode, reply, wantErr)
		}
		stop()
		retired = imageResult(command.request)
	}
}

func beginImageRead(ctx context.Context, registry *Registry, agentID string) <-chan imageReply {
	done := make(chan imageReply, 1)
	go func() {
		result, err := registry.ResolveWorkloadImages(ctx, agentID, imageSelectors())
		done <- imageReply{result: result, err: err}
	}()
	return done
}

func awaitImageRead(t *testing.T, ctx context.Context, done <-chan imageReply) imageReply {
	t.Helper()
	select {
	case reply := <-done:
		return reply
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return imageReply{}
	}
}

func assertImageReply(t *testing.T, reply imageReply, requestID string) {
	t.Helper()
	rows := reply.result.GetSuccess().GetResolutions()
	if reply.err != nil || reply.result.GetRequestId() != requestID || len(rows) != 1 ||
		rows[0].GetSelector().GetRequestedReference() != "app:dev" ||
		rows[0].GetLocalImageId() != "sha256:"+strings.Repeat("a", 64) {
		t.Fatalf("image reply = %v, want exact request/selector/local image", reply)
	}
}
