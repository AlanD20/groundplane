package agent

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type serviceObserverFunc func(context.Context, *agentpb.ObserveServices) (*agentpb.ServiceObservationResult, error)

func (observe serviceObserverFunc) Observe(
	ctx context.Context,
	request *agentpb.ObserveServices,
) (*agentpb.ServiceObservationResult, error) {
	return observe(ctx, request)
}

func serviceReadRequest() *agentpb.ObserveServices {
	return &agentpb.ObserveServices{
		RequestId: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Targets: []*agentpb.ServiceObservationTarget{{
			EnvironmentId: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", ServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			ReleaseId: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV", PlanId: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			RenderGeneration: 3, ComposeName: "api-blue", RuntimeRole: "slot", Slot: "blue",
		}},
	}
}

func successfulServiceRead(
	_ context.Context,
	request *agentpb.ObserveServices,
) (*agentpb.ServiceObservationResult, error) {
	result := unavailableObservation(request)
	result.Observations[0].Outcome = &agentpb.ServiceObservationRow_Replicas{
		Replicas: &agentpb.ServiceReplicaCounts{Healthy: 1},
	}
	return result, nil
}

type serviceCaptureStream struct {
	agentStream
	results chan *agentpb.ServiceObservationResult
}

func (stream *serviceCaptureStream) Send(message *agentpb.AgentMessage) error {
	if result := message.GetServiceObservationResult(); result != nil {
		stream.results <- proto.CloneOf(result)
		return nil
	}
	return stream.agentStream.Send(message)
}

// Rationale: ordinary read failure must return unavailable on the authenticated
// stream, not disconnect, create a Task, or consume worker-pool capacity.
func TestServiceObservationClientStream(t *testing.T) {
	for _, mode := range []string{"success", "image busy", "failure", "malformed", "unconfigured"} {
		t.Run(mode, func(t *testing.T) {
			request := serviceReadRequest()
			stream := newFakeStream(configMessage(60, 2), &agentpb.ControllerMessage{
				Payload: &agentpb.ControllerMessage_ObserveServices{ObserveServices: request},
			})
			client := newTestClient(t, bytes.Repeat([]byte{0x31}, agentprotocol.RawTokenBytes), stream)
			if mode == "image busy" {
				stream.receive = []*agentpb.ControllerMessage{stream.receive[0], {
					Payload: &agentpb.ControllerMessage_ResolveWorkloadImages{ResolveWorkloadImages: imageRequest()},
				}, stream.receive[1]}
				images := &blockingImageResolver{started: make(chan struct{}), stopped: make(chan struct{})}
				if err := client.SetWorkloadImageResolver(images); err != nil {
					t.Fatal(err)
				}
			}
			observer := serviceObserverFunc(
				func(ctx context.Context, request *agentpb.ObserveServices) (*agentpb.ServiceObservationResult, error) {
					if mode == "failure" {
						return nil, errs.New(errs.KindInternal, "private Docker diagnostic")
					}
					result, err := successfulServiceRead(ctx, request)
					if mode == "malformed" {
						result.RequestId = "wrong"
					}
					return result, err
				},
			)
			if mode != "unconfigured" {
				if err := client.SetServiceObserver(observer); err != nil {
					t.Fatal(err)
				}
			}
			connect := client.connect
			results := make(chan *agentpb.ServiceObservationResult, 1)
			client.connect = func(ctx context.Context, path string) (agentStream, io.Closer, error) {
				connected, closer, err := connect(ctx, path)
				return &serviceCaptureStream{agentStream: connected, results: results}, closer, err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			done := make(chan error, 1)
			go func() { done <- client.Run(ctx) }()
			t.Cleanup(func() {
				cancel()
				if err := <-done; err != nil {
					t.Error(err)
				}
			})
			select {
			case result := <-results:
				if err := serviceobservation.ValidateResult(request, result); err != nil {
					t.Fatal(err)
				}
				if result.Observations[0].GetUnavailable() != (mode != "success" && mode != "image busy") {
					t.Fatalf("wrong result: %v", result)
				}
			case <-ctx.Done():
				t.Fatal("observation did not return")
			}
			if err := client.SetServiceObserver(observer); err == nil {
				t.Fatal("observer changed after start")
			}
			sent := stream.sentMessages()
			if len(sent) != 2 || sent[0].GetAuthenticate() == nil || sent[1].GetReady().GetCapacity() != 2 {
				t.Fatalf("observation changed Task capacity or traffic: %v", sent)
			}
		})
	}
}

// Rationale: one busy read is bounded independently of image work. A stale
// cancel cannot stop it; its matching cancel joins it and discards late output.
func TestServiceObservationWorkerCancellation(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	observer := serviceObserverFunc(
		func(ctx context.Context, request *agentpb.ObserveServices) (*agentpb.ServiceObservationResult, error) {
			close(started)
			<-ctx.Done()
			close(stopped)
			return successfulServiceRead(ctx, request)
		},
	)
	session := newObservationSession(observer)
	defer session.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	request := serviceReadRequest()
	if result, err := session.start(ctx, request); err != nil || result != nil {
		t.Fatalf("start=%v %v", result, err)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	second := proto.CloneOf(request)
	second.RequestId = "01ARZ3NDEKTSV4RRFFQ69G5FAW"
	if result, err := session.start(ctx, second); err != nil || !result.Observations[0].GetUnavailable() {
		t.Fatalf("busy read was admitted: %v %v", result, err)
	}
	message := &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_CancelServiceObservation{
		CancelServiceObservation: &agentpb.CancelServiceObservation{RequestId: second.RequestId},
	}}
	if handled, err := session.handle(ctx, nil, message); !handled || err != nil {
		t.Fatal(err)
	}
	select {
	case <-stopped:
		t.Fatal("stale cancellation stopped worker")
	default:
	}
	message.GetCancelServiceObservation().RequestId = request.RequestId
	if handled, err := session.handle(ctx, nil, message); !handled || err != nil {
		t.Fatal(err)
	}
	select {
	case <-stopped:
	default:
		t.Fatal("cancel did not join worker")
	}
	select {
	case <-session.outputs:
		t.Fatal("cancelled result remained usable")
	default:
	}
	session.observer = serviceObserverFunc(successfulServiceRead)
	if _, err := session.start(ctx, second); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-session.outputs:
		if result.RequestId != second.RequestId || result.Observations[0].GetReplicas().GetHealthy() != 1 {
			t.Fatal(result)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

// Rationale: timed-out reads cannot return valid-looking late counts; the
// observer receives the five-second cap or the caller's earlier deadline.
func TestServiceObservationDeadlineDiscardsLateCounts(t *testing.T) {
	observer := serviceObserverFunc(
		func(ctx context.Context, request *agentpb.ObserveServices) (*agentpb.ServiceObservationResult, error) {
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > serviceobservation.Timeout {
				t.Error("missing read deadline")
			}
			<-ctx.Done()
			return successfulServiceRead(ctx, request)
		},
	)
	session := newObservationSession(observer)
	defer session.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := session.start(ctx, serviceReadRequest()); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-session.outputs:
		if !result.Observations[0].GetUnavailable() {
			t.Fatal("late success escaped")
		}
	case <-time.After(time.Second):
		t.Fatal("read did not expire")
	}
}

// Rationale: outer unknown fields and malformed cancellation are rejected
// before they can acquire or release the read-only worker slot.
func TestServiceObservationRejectsUnknownEnvelope(t *testing.T) {
	session := newObservationSession(nil)
	defer session.Close()
	message := &agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_ObserveServices{ObserveServices: serviceReadRequest()},
	}
	message.ProtoReflect().SetUnknown([]byte{0xF8, 0x07, 1})
	if _, err := session.handle(context.Background(), nil, message); err == nil {
		t.Fatal("unknown envelope accepted")
	}
	message = &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_CancelServiceObservation{
		CancelServiceObservation: &agentpb.CancelServiceObservation{RequestId: "wrong"},
	}}
	if _, err := session.handle(context.Background(), nil, message); err == nil {
		t.Fatal("malformed cancel accepted")
	}
}
