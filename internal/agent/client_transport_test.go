package agent

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// QA: HOST-05; pure backoff bounds only, not elapsed reconnect timing or restored authenticated Ready.
// Rationale: reconnect delay must grow exponentially without exceeding its
// ceiling, while equal jitter prevents synchronized reconnect storms.
func TestAgentChannelReconnectDelayIsBoundedAndJittered(t *testing.T) {
	for _, test := range []struct {
		name    string
		attempt uint
		minimum time.Duration
		maximum time.Duration
	}{
		{name: "initial", attempt: 0, minimum: 50 * time.Millisecond, maximum: 100 * time.Millisecond},
		{name: "second", attempt: 1, minimum: 100 * time.Millisecond, maximum: 200 * time.Millisecond},
		{name: "capped", attempt: 63, minimum: 2500 * time.Millisecond, maximum: 5 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			window := test.maximum - test.minimum
			if got := agentChannelReconnectDelay(test.attempt, 0); got != test.minimum {
				t.Fatalf("minimum delay = %s, want %s", got, test.minimum)
			}
			if got := agentChannelReconnectDelay(test.attempt, uint64(window)); got != test.maximum {
				t.Fatalf("maximum delay = %s, want %s", got, test.maximum)
			}
		})
	}
}

// QA: HOST-05; pre-cancelled wait only, not cancellation during a live randomized delay.
// Rationale: process cancellation must interrupt a pending reconnect delay so
// shutdown never waits for the backoff ceiling.
func TestAgentChannelReconnectWaitHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitForAgentChannelReconnect(ctx, 63); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForAgentChannelReconnect() error = %v, want context cancellation", err)
	}
}

// QA: TASK-07; local Unix-socket gRPC receive boundary, not Controller enqueue fairness or durable event bounds.
// Rationale: the Agent must receive the complete 5 MiB Controller envelope
// while rejecting the first byte beyond that transport boundary.
func TestConnectGRPCEnforcesExactReceiveMessageLimit(t *testing.T) {
	for _, test := range []struct {
		name     string
		size     int
		wantCode codes.Code
	}{
		{name: "boundary", size: 5 * 1024 * 1024, wantCode: codes.OK},
		{name: "one_byte_over", size: 5*1024*1024 + 1, wantCode: codes.ResourceExhausted},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := &agentChannelClientTransportServer{response: sizedControllerMessage(t, test.size)}
			socketPath := startAgentChannelClientTransportServer(t, server)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			stream, connection, err := connectGRPC(ctx, socketPath)
			if err != nil {
				t.Fatalf("connectGRPC() error = %v", err)
			}
			defer connection.Close()
			if err := stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_Ready{
				Ready: &agentpb.Ready{Capacity: 1, Version: "test"},
			}}); err != nil {
				t.Fatalf("send request: %v", err)
			}
			message, err := stream.Recv()
			if status.Code(err) != test.wantCode {
				t.Fatalf("receive %d-byte Controller message = %v, want %s", test.size, err, test.wantCode)
			}
			if err == nil && proto.Size(message) != test.size {
				t.Fatalf("Controller message size = %d, want %d", proto.Size(message), test.size)
			}
		})
	}
}

// QA: TASK-07; local Unix-socket gRPC send boundary, not Controller validation or durable storage bounds.
// Rationale: the Agent must send the complete 16 MiB envelope while rejecting
// the first byte beyond that transport boundary.
func TestConnectGRPCEnforcesExactSendMessageLimit(t *testing.T) {
	for _, test := range []struct {
		name     string
		size     int
		wantCode codes.Code
	}{
		{name: "boundary", size: 16 * 1024 * 1024, wantCode: codes.OK},
		{name: "one_byte_over", size: 16*1024*1024 + 1, wantCode: codes.ResourceExhausted},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := &agentChannelClientTransportServer{received: make(chan int, 1)}
			socketPath := startAgentChannelClientTransportServer(t, server)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			stream, connection, err := connectGRPC(ctx, socketPath)
			if err != nil {
				t.Fatalf("connectGRPC() error = %v", err)
			}
			defer connection.Close()
			sendErr := stream.Send(sizedAgentMessage(t, test.size))
			if status.Code(sendErr) != test.wantCode {
				t.Fatalf("send %d-byte Agent message = %v, want %s", test.size, sendErr, test.wantCode)
			}
			if sendErr == nil {
				select {
				case size := <-server.received:
					if size != test.size {
						t.Fatalf("server received %d-byte Agent message, want %d", size, test.size)
					}
				case <-ctx.Done():
					t.Fatal("timed out waiting for exact-boundary Agent message")
				}
			}
		})
	}
}

type agentChannelClientTransportServer struct {
	agentpb.UnimplementedAgentChannelServer
	response *agentpb.ControllerMessage
	received chan int
}

func (server *agentChannelClientTransportServer) Connect(stream agentpb.AgentChannel_ConnectServer) error {
	message, err := stream.Recv()
	if err != nil {
		return err
	}
	if server.received != nil {
		server.received <- proto.Size(message)
	}
	if server.response != nil {
		return stream.Send(server.response)
	}
	return nil
}

func startAgentChannelClientTransportServer(
	t *testing.T,
	server agentpb.AgentChannelServer,
) string {
	t.Helper()
	socketPath := shortUnixSocketPath(t)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen on Agent channel socket: %v", err)
	}
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(16*1024*1024+1),
		grpc.MaxSendMsgSize(5*1024*1024+1),
	)
	agentpb.RegisterAgentChannelServer(grpcServer, server)
	serveDone := make(chan error, 1)
	go func() { serveDone <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		if err := <-serveDone; err != nil {
			t.Errorf("serve Agent channel: %v", err)
		}
	})
	return socketPath
}

func shortUnixSocketPath(t *testing.T) string {
	t.Helper()
	// Keep the test socket inside the worktree so repository-managed temporary
	// state never spills into the system temporary directory. The short prefix
	// leaves room for the worktree path under Unix's socket path limit.
	directory, err := os.MkdirTemp(os.TempDir(), "gp-u-")
	if err != nil {
		t.Fatalf("create short Unix socket directory: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(directory); err != nil {
			t.Errorf("remove short Unix socket directory: %v", err)
		}
	})
	return filepath.Join(directory, "agent.sock")
}

func sizedControllerMessage(t *testing.T, target int) *agentpb.ControllerMessage {
	t.Helper()
	payload := strings.Repeat("x", target)
	message := &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ConfigUpdate{
		ConfigUpdate: &agentpb.ConfigUpdate{AgentConfig: &agentpb.AgentConfig{
			PullIntervalSeconds: 1,
			MaxConcurrentTasks:  1,
			Labels:              []*agentpb.AgentLabel{{Key: "payload", Value: payload}},
		}},
	}}
	for proto.Size(message) > target {
		payload = payload[:len(payload)-(proto.Size(message)-target)]
		message.GetConfigUpdate().GetAgentConfig().Labels[0].Value = payload
	}
	if proto.Size(message) != target {
		t.Fatalf("cannot construct %d-byte Controller message; got %d", target, proto.Size(message))
	}
	return message
}

func sizedAgentMessage(t *testing.T, target int) *agentpb.AgentMessage {
	t.Helper()
	payload := make([]byte, target)
	message := &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_WorkJournalReplayRequest{
		WorkJournalReplayRequest: &agentpb.WorkJournalReplayRequest{Report: payload},
	}}
	for proto.Size(message) > target {
		payload = payload[:len(payload)-(proto.Size(message)-target)]
		message.GetWorkJournalReplayRequest().Report = payload
	}
	if proto.Size(message) != target {
		t.Fatalf("cannot construct %d-byte Agent message; got %d", target, proto.Size(message))
	}
	return message
}
