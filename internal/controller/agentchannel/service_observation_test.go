package agentchannel

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func servingTargets() []*agentpb.ServiceObservationTarget {
	return []*agentpb.ServiceObservationTarget{{
		EnvironmentId: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", ServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ReleaseId: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV", PlanId: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		RenderGeneration: 3, ComposeName: "api-blue", RuntimeRole: "slot", Slot: "blue",
	}}
}

func servingResult(request *agentpb.ObserveServices) *agentpb.AgentMessage {
	return &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ServiceObservationResult{
		ServiceObservationResult: &agentpb.ServiceObservationResult{RequestId: request.RequestId,
			Observations: []*agentpb.ServiceObservationRow{{
				ServiceId: request.Targets[0].ServiceId, ReleaseId: request.Targets[0].ReleaseId,
				Outcome: &agentpb.ServiceObservationRow_Replicas{Replicas: &agentpb.ServiceReplicaCounts{Healthy: 1}},
			}},
		},
	}}
}

func beginServiceRead(ctx context.Context, registry *Registry, agentID string) <-chan observationReply {
	done := make(chan observationReply, 1)
	go func() {
		result, err := registry.ObserveServices(ctx, agentID, servingTargets())
		done <- observationReply{result: result, err: err}
	}()
	return done
}

func awaitServiceRead(t *testing.T, ctx context.Context, done <-chan observationReply) observationReply {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return observationReply{}
	}
}

func nextServiceCommand(t *testing.T, ctx context.Context, session *Session) *observationCommand {
	t.Helper()
	select {
	case command := <-session.state.observationCommands:
		return command
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return nil
	}
}

// QA: OBS-01/03/04; local exchange isolation, five-second deadline and typed result, not live health.
// Rationale: the read has its own slot even with zero Task capacity and active
// image lookup; only its exact raw ULID may complete the pending exchange.
func TestServiceObservationCorrelationAndIndependence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r := NewRegistry()
	s, err := r.Open(ctx, "agent", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.RecordReady(time.Now(), 0, "1.0.0"); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	done := beginServiceRead(ctx, r, "agent")
	command := nextServiceCommand(t, ctx, s)
	if ids.Validate(ids.KindOperation, "op_"+command.request.RequestId) != nil {
		t.Fatal("not a raw ULID")
	}
	if deadline, ok := command.ctx.Deadline(); !ok || deadline.Before(started.Add(5*time.Second)) ||
		deadline.After(time.Now().Add(5*time.Second)) {
		t.Fatalf("exchange deadline = %v, want its own five-second bound", deadline)
	}
	if _, err := r.ObserveServices(ctx, "agent", servingTargets()); !errors.Is(
		err,
		errs.New(errs.KindResourceInUse, ""),
	) {
		t.Fatalf("concurrent read=%v", err)
	}
	imageDone := make(chan error, 1)
	go func() { _, err := r.ResolveWorkloadImages(ctx, "agent", imageSelectors()); imageDone <- err }()
	select {
	case image := <-s.state.imageCommands:
		s.acceptImageResult(imageResult(image.request))
	case <-ctx.Done():
		t.Fatal("observation occupied image slot")
	}
	select {
	case err := <-imageDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stale := servingResult(command.request)
	stale.GetServiceObservationResult().RequestId = "01ARZ3NDEKTSV4RRFFQ69G5FAW"
	s.acceptObservationResult(ctx, stale)
	select {
	case result := <-done:
		t.Fatalf("stale result accepted: %v", result)
	default:
	}
	s.acceptObservationResult(ctx, servingResult(command.request))
	assertServingReply(t, awaitServiceRead(t, ctx, done), command.request.RequestId)
	if err := s.RecordReady(time.Now(), 0, "1.0.0"); err != nil {
		t.Fatal("observation closed session")
	}
}

// QA: OBS-04, HOST-05; local request/session fencing and replacement read, not public freshness.
// Rationale: equal-generation reconnection is still a different authenticated
// session. Old results cannot satisfy either the retired or replacement read.
func TestServiceObservationRejectsReplacementSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r := NewRegistry()
	old, err := r.Open(ctx, "agent", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	done := beginServiceRead(ctx, r, "agent")
	command := nextServiceCommand(t, ctx, old)
	next, err := r.Open(ctx, "agent", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	old.acceptObservationResult(ctx, servingResult(command.request))
	if reply := awaitServiceRead(t, ctx, done); !errors.Is(reply.err, errs.New(errs.KindStorageUnavailable, "")) ||
		reply.result != nil {
		t.Fatalf("retired session reply = %v", reply)
	}
	done = beginServiceRead(ctx, r, "agent")
	replacement := nextServiceCommand(t, ctx, next)
	if replacement.request.RequestId == command.request.RequestId {
		t.Fatal("correlation reused")
	}
	next.acceptObservationResult(ctx, servingResult(command.request))
	old.acceptObservationResult(ctx, servingResult(replacement.request))
	select {
	case <-done:
		t.Fatal("old session or request satisfied replacement")
	default:
	}
	next.acceptObservationResult(ctx, servingResult(replacement.request))
	assertServingReply(t, awaitServiceRead(t, ctx, done), replacement.request.RequestId)
}

// QA: OBS-01/04; malformed machine evidence is rejected locally, not Docker observation or serving-source races.
// Rationale: a correctly correlated but malformed result fails the read without
// exposing partial counts or closing unrelated Agent traffic.
func TestServiceObservationRejectsMalformedResults(t *testing.T) {
	for _, mode := range []string{"outer unknown", "wrong Release", "missing row"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			r := NewRegistry()
			s, err := r.Open(ctx, "agent", 1)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			done := beginServiceRead(ctx, r, "agent")
			command := nextServiceCommand(t, ctx, s)
			message := servingResult(command.request)
			switch mode {
			case "outer unknown":
				message.ProtoReflect().SetUnknown([]byte{0xF8, 0x07, 1})
			case "wrong Release":
				message.GetServiceObservationResult().Observations[0].ReleaseId = "dep_wrong"
			case "missing row":
				message.GetServiceObservationResult().Observations = nil
			}
			s.acceptObservationResult(ctx, message)
			reply := awaitServiceRead(t, ctx, done)
			if !errors.Is(reply.err, errs.New(errs.KindValidationFailed, "")) || reply.result != nil {
				t.Fatalf("malformed read=%v", reply)
			}
			if err := s.RecordReady(time.Now(), 1, "1.0.0"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func nextServiceWire(t *testing.T, ctx context.Context, stream *liveStream) *agentpb.ControllerMessage {
	t.Helper()
	select {
	case message := <-stream.sent:
		return message
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return nil
	}
}

// QA: OBS-03/04; Controller loop over an in-memory stream, not gRPC or worker cleanup.
// Rationale: the actual stream loop must send explicit cancellation, keep the
// connection, and serve a later read without a Task store or reconnect retry.
func TestServiceObservationThroughControllerStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	r := NewRegistry()
	stream := newLiveStream(ctx)
	stream.received <- authenticateMessage(testAgentID, testToken(4))
	serverDone := make(chan error, 1)
	go func() { serverDone <- New(authorizedAuthenticator(), r, nil, nil).Connect(stream) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-serverDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		case <-time.After(time.Second):
			t.Error("Controller stream did not stop")
		}
	})
	if nextServiceWire(t, ctx, stream).GetConfigUpdate() == nil {
		t.Fatal("configuration must follow authentication")
	}
	readCtx, readCancel := context.WithCancel(ctx)
	defer readCancel()
	done := beginServiceRead(readCtx, r, testAgentID)
	request := nextServiceWire(t, ctx, stream).GetObserveServices()
	if request == nil {
		t.Fatal("missing observation request")
	}
	if len(request.Targets) != 1 || !proto.Equal(request.Targets[0], servingTargets()[0]) {
		t.Fatalf("wire observation targets = %v", request.Targets)
	}
	readCancel()
	if reply := awaitServiceRead(t, ctx, done); !errors.Is(reply.err, context.Canceled) || reply.result != nil {
		t.Fatalf("cancel=%v", reply)
	}
	if cancelled := nextServiceWire(t, ctx, stream).GetCancelServiceObservation(); cancelled.GetRequestId() != request.RequestId {
		t.Fatal("missing correlated cancellation")
	}
	stream.received <- servingResult(request)
	done = beginServiceRead(ctx, r, testAgentID)
	next := nextServiceWire(t, ctx, stream).GetObserveServices()
	if next == nil || next.RequestId == request.RequestId {
		t.Fatal("missing fresh read")
	}
	stream.received <- servingResult(next)
	assertServingReply(t, awaitServiceRead(t, ctx, done), next.RequestId)
}

// QA: OBS-03/04, HOST-05; local offline/input/cancellation errors, not a real connection outage.
// Rationale: invalid, already canceled and disconnected reads must fail with
// the expected cause and cannot return observation evidence.
func TestServiceObservationUnavailableWithoutConnection(t *testing.T) {
	r := NewRegistry()
	if result, err := r.ObserveServices(context.Background(), "missing", servingTargets()); !errors.Is(
		err,
		errs.New(errs.KindStorageUnavailable, ""),
	) ||
		result != nil {
		t.Fatal("offline read succeeded")
	}
	if _, err := r.ObserveServices(context.Background(), "missing", nil); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.ObserveServices(ctx, "missing", servingTargets()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// QA: OBS-03/04; explicitly ordered local send selection, not a network cancellation race.
// Rationale: when cancellation races a new read, the stream sends the old
// cancellation first even if it selects the new command before the cancel case.
func TestServiceObservationCancelPrecedesReplacementRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r := NewRegistry()
	s, err := r.Open(ctx, "agent", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	readCtx, readCancel := context.WithCancel(ctx)
	defer readCancel()
	done := beginServiceRead(readCtx, r, "agent")
	old := nextServiceCommand(t, ctx, s)
	exchange := &observationExchange{active: old}
	readCancel()
	if reply := awaitServiceRead(t, ctx, done); !errors.Is(reply.err, context.Canceled) {
		t.Fatal(reply.err)
	}
	done = beginServiceRead(ctx, r, "agent")
	next := nextServiceCommand(t, ctx, s)
	stream := newLiveStream(ctx)
	if err := exchange.send(ctx, stream, s, next); err != nil {
		t.Fatal(err)
	}
	if nextServiceWire(t, ctx, stream).GetCancelServiceObservation().GetRequestId() != old.request.RequestId {
		t.Fatal("replacement preceded cancellation")
	}
	if nextServiceWire(t, ctx, stream).GetObserveServices().GetRequestId() != next.request.RequestId {
		t.Fatal("missing replacement")
	}
	s.acceptObservationResult(ctx, servingResult(next.request))
	assertServingReply(t, awaitServiceRead(t, ctx, done), next.request.RequestId)
}

func assertServingReply(t *testing.T, reply observationReply, requestID string) {
	t.Helper()
	want := &agentpb.ServiceObservationResult{RequestId: requestID, Observations: []*agentpb.ServiceObservationRow{{
		ServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", ReleaseId: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Outcome: &agentpb.ServiceObservationRow_Replicas{Replicas: &agentpb.ServiceReplicaCounts{Healthy: 1}},
	}}}
	if reply.err != nil || !proto.Equal(reply.result, want) {
		t.Fatalf("observation reply = %v, want exact serving identity and counts", reply)
	}
}
