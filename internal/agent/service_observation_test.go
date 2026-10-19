package agent

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
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
			ReleaseId: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV", RuntimeRole: "slot", Slot: "blue",
			Workloads: []*agentpb.ServiceWorkloadObservation{
				{PlanId: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV", RenderGeneration: 3, ComposeName: "api-blue"},
			},
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
	result.Observations[0].Containers = []*agentpb.ServiceContainerObservation{
		{Id: "container-1", Name: "api-1", State: "running", Health: "healthy", Replica: 1},
	}
	return result, nil
}

// QA: OBS-03/04; in-memory request cancellation/correlation only, not source-revision or Agent-generation fences.
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

// QA: OBS-03; local caller deadline and late-result replacement only, not Controller 15-second expiry or refresh loops.
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

// QA: OBS-03/04; local envelope validation only, not Controller correlation or serving-source rechecks.
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
