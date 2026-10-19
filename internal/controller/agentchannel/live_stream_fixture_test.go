package agentchannel

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc"
)

// A plain stream peer: it never synthesizes startup, readiness or receipts.
type liveStream struct {
	grpc.ServerStream
	ctx      context.Context
	received chan *agentpb.AgentMessage
	sent     chan *agentpb.ControllerMessage
}

func newLiveStream(ctx context.Context) *liveStream {
	return &liveStream{ctx: ctx, received: make(chan *agentpb.AgentMessage, 1),
		sent: make(chan *agentpb.ControllerMessage, 2)}
}

func (stream *liveStream) Context() context.Context { return stream.ctx }
func (stream *liveStream) Send(message *agentpb.ControllerMessage) error {
	select {
	case <-stream.ctx.Done():
		return stream.ctx.Err()
	case stream.sent <- message:
		return nil
	}
}
func (stream *liveStream) Recv() (*agentpb.AgentMessage, error) {
	select {
	case <-stream.ctx.Done():
		return nil, stream.ctx.Err()
	case message := <-stream.received:
		return message, nil
	}
}

// Queries use actual native registration and complete the replay phase, but do
// not advertise Ready or grant execution. Authentication remains a supplied port.
func nativeConnectedQueryChannel(t *testing.T, parent context.Context) (*Registry, *liveStream, string) {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	server, _, authenticate := nativeConnectFixture(t)
	stream := newLiveStream(ctx)
	stream.received <- authenticate
	done := make(chan error, 1)
	go func() { done <- server.Connect(stream) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		case <-time.After(time.Second):
			t.Error("native query channel did not stop")
		}
	})
	if nextServiceWire(t, ctx, stream).GetConfigUpdate() == nil ||
		nextServiceWire(t, ctx, stream).GetWorkExecutorRegistration() == nil {
		t.Fatal("native query channel did not authenticate and register")
	}
	stream.received <- nativeConnectReplayComplete()
	if !nextServiceWire(t, ctx, stream).GetWorkJournalReplayAck().GetComplete() {
		t.Fatal("native query channel did not acknowledge completed replay")
	}
	return server.sessions, stream, authenticate.GetAuthenticate().AgentId
}
