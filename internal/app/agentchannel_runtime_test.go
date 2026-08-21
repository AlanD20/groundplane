package app

import (
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Rationale: the native Controller must own the local gRPC socket before Agent
// persistence exists, while missing authentication must reject every stream.
func TestAgentChannelRuntimeServesUDSAndFailsClosedWithoutAuthenticator(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "agent.sock")
	listening := make(chan struct{})
	runtime := newAgentChannelRuntime(nil, nil)
	runtime.listen = func(context.Context) (net.Listener, error) {
		listener, err := net.Listen("unix", socketPath)
		if err == nil {
			close(listening)
		}
		return listener, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(ctx) }()
	select {
	case <-listening:
	case err := <-runDone:
		t.Fatalf("Run() before listener start = %v", err)
	case <-time.After(time.Second):
		cancel()
		t.Fatal("timed out waiting for Agent channel listener")
	}

	dialCtx, dialCancel := context.WithTimeout(context.Background(), time.Second)
	defer dialCancel()
	connection, err := grpc.NewClient(
		"unix://"+socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("create Agent channel client: %v", err)
	}
	defer connection.Close()
	stream, err := agentpb.NewAgentChannelClient(connection).Connect(dialCtx)
	if err != nil {
		t.Fatalf("connect Agent channel: %v", err)
	}
	sendErr := stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_Authenticate{
		Authenticate: &agentpb.Authenticate{
			AgentId: "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			Token:   make([]byte, 32),
		},
	}})
	if sendErr != nil && !errors.Is(sendErr, io.EOF) {
		t.Fatalf("send Authenticate: %v", sendErr)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.Internal {
		t.Fatalf("Connect() error = %v, want fail-closed Internal", err)
	}

	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() after cancellation = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Agent channel shutdown")
	}
}

// Rationale: dependency cancellation sentinels under a live Controller are
// dependency failures, not caller cancellation, and must remain private.
func TestAgentChannelRuntimeClassifiesLiveListenerFailure(t *testing.T) {
	t.Parallel()

	cause := context.Canceled
	runtime := &agentChannelRuntime{
		registry: agentchannel.NewRegistry(),
		listen: func(context.Context) (net.Listener, error) {
			return nil, cause
		},
		newServer: func(*agentchannel.Registry) agentChannelGRPCServer { return nil },
	}
	err := runtime.Run(context.Background())
	if !errors.Is(err, errs.New(errs.KindInternal, "")) || !errors.Is(err, cause) {
		t.Fatalf("Run() error = %v, want Internal preserving private cause", err)
	}
}
