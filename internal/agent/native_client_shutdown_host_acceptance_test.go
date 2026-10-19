//go:build agenthostacceptance

package agent

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// TASK-90 / HOST-05: process cancellation, not a Controller Shutdown frame,
// must close the real stream and join a worker waiting for Start. No effect is
// authorized, and the supplied Controller peer does not qualify real settlement.
func TestNativeClientCancellationJoinsWorkAndClosesStream(t *testing.T) {
	f := newNativeWorkersHostFixture(t, 1)
	delivery := f.assignment(t)
	attempt, err := workexecution.DecodeAgentAttempt(delivery.Attempt, f.workers.identity)
	if err != nil {
		t.Fatal(err)
	}
	token := bytes.Repeat([]byte{0x55}, agentprotocol.RawTokenBytes)
	started := make(chan struct{})
	results := make(chan error, 1)
	server := &nativeClientProtocolServer{results: results,
		connect: func(stream agentpb.AgentChannel_ConnectServer) error {
			if err := nativeClientHandshake(stream, f.workers.identity, token, 1); err != nil {
				return err
			}
			if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ScriptWorkAssignment{
				ScriptWorkAssignment: proto.CloneOf(delivery)}}); err != nil {
				return err
			}
			request, err := stream.Recv()
			if err != nil || !proto.Equal(request.GetWorkStartRequest().GetAttempt(), delivery.Attempt) {
				return errs.New(errs.KindStateConflict, "Client never requested the captured Start")
			}
			close(started)
			<-stream.Context().Done()
			return stream.Context().Err()
		}}
	client := newNativeProtocolClient(t, f, server, token)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	select {
	case <-started:
	case err := <-done:
		t.Fatalf("Client exited before Start: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("Client never reached Start")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil || client.executionWorkers != nil {
			t.Fatalf("Client did not shut down cleanly: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Client cancellation did not join execution")
	}
	if err := <-results; !errors.Is(err, context.Canceled) {
		t.Fatalf("Controller stream did not close on cancellation: %v", err)
	}
	exclusive, err := f.ownership.Begin(attempt)
	if err != nil {
		t.Fatalf("joined Client retained active process ownership: %v", err)
	}
	if err := exclusive.Close(); err != nil {
		t.Fatal(err)
	}
}
