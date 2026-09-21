package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const agentChannelTransportTestAgentID = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV"

// Rationale: the native Controller must own the local gRPC socket before Agent
// persistence exists, while missing authentication must reject every stream.
func TestAgentChannelRuntimeServesUDSAndFailsClosedWithoutAuthenticator(t *testing.T) {
	socketPath := shortUnixSocketPath(t)
	listening := make(chan struct{})
	runtime := New(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
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
	runtime := &Runtime{
		Registry: agentchannel.NewRegistry(),
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

// Rationale: the Controller must accept the complete 16 MiB Agent envelope
// while rejecting the first byte beyond that transport boundary.
func TestAgentChannelRuntimeEnforcesExactReceiveMessageLimit(t *testing.T) {
	if agentChannelControllerMaximumReceiveMessageBytes != 16*1024*1024 {
		t.Fatalf(
			"Controller receive limit = %d, want 16 MiB",
			agentChannelControllerMaximumReceiveMessageBytes,
		)
	}

	for _, test := range []struct {
		name     string
		size     int
		wantCode codes.Code
	}{
		{name: "boundary", size: 16 * 1024 * 1024, wantCode: codes.Unauthenticated},
		{name: "one_byte_over", size: 16*1024*1024 + 1, wantCode: codes.ResourceExhausted},
	} {
		t.Run(test.name, func(t *testing.T) {
			authenticator := &agentChannelTransportAuthenticator{config: transportTestAgentConfig()}
			socketPath := startAgentChannelTransportRuntime(t, authenticator)
			connection, err := grpc.NewClient(
				"unix://"+socketPath,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(test.size)),
			)
			if err != nil {
				t.Fatalf("create Agent channel client: %v", err)
			}
			defer connection.Close()
			stream, err := agentpb.NewAgentChannelClient(connection).Connect(context.Background())
			if err != nil {
				t.Fatalf("connect Agent channel: %v", err)
			}
			if err := stream.Send(sizedAgentAuthenticationMessage(t, test.size)); err != nil {
				t.Fatalf("send %d-byte Agent message: %v", test.size, err)
			}
			if _, err := stream.Recv(); status.Code(err) != test.wantCode {
				t.Fatalf("receive after %d-byte Agent message = %v, want %s", test.size, err, test.wantCode)
			}
		})
	}
}

// Rationale: the Controller must send the complete 5 MiB envelope while
// rejecting the first byte beyond that transport boundary.
func TestAgentChannelRuntimeEnforcesExactSendMessageLimit(t *testing.T) {
	if agentChannelControllerMaximumSendMessageBytes != 5*1024*1024 {
		t.Fatalf(
			"Controller send limit = %d, want 5 MiB",
			agentChannelControllerMaximumSendMessageBytes,
		)
	}

	for _, test := range []struct {
		name     string
		size     int
		wantCode codes.Code
	}{
		{name: "boundary", size: 5 * 1024 * 1024, wantCode: codes.OK},
		{name: "one_byte_over", size: 5*1024*1024 + 1, wantCode: codes.ResourceExhausted},
	} {
		t.Run(test.name, func(t *testing.T) {
			authenticator := &agentChannelTransportAuthenticator{config: sizedAgentConfig(t, test.size)}
			socketPath := startAgentChannelTransportRuntime(t, authenticator)
			connection, err := grpc.NewClient(
				"unix://"+socketPath,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(test.size)),
			)
			if err != nil {
				t.Fatalf("create Agent channel client: %v", err)
			}
			defer connection.Close()
			stream, err := agentpb.NewAgentChannelClient(connection).Connect(context.Background())
			if err != nil {
				t.Fatalf("connect Agent channel: %v", err)
			}
			if err := stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_Authenticate{
				Authenticate: &agentpb.Authenticate{
					AgentId: agentChannelTransportTestAgentID,
					Token:   make([]byte, 32),
				},
			}}); err != nil {
				t.Fatalf("send Authenticate: %v", err)
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

type agentChannelTransportAuthenticator struct {
	config *agentpb.AgentConfig
}

func (authenticator *agentChannelTransportAuthenticator) Authenticate(
	context.Context,
	string,
	agentchannel.Token,
) (agentchannel.Authorization, error) {
	return agentchannel.Authorization{Generation: 1, Config: authenticator.config}, nil
}

func (authenticator *agentChannelTransportAuthenticator) Configuration(
	context.Context,
	string,
	uint64,
) (*agentpb.AgentConfig, error) {
	return authenticator.config, nil
}

func startAgentChannelTransportRuntime(
	t *testing.T,
	authenticator agentchannel.Authenticator,
) string {
	t.Helper()
	socketPath := shortUnixSocketPath(t)
	listening := make(chan struct{})
	runtime := New(authenticator, nil, nil, nil, nil, nil, nil, nil, nil, nil)
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
		cancel()
		t.Fatalf("Run() before listener start = %v", err)
	case <-time.After(time.Second):
		cancel()
		t.Fatal("timed out waiting for Agent channel listener")
	}
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-runDone:
			if err != nil {
				t.Errorf("Run() after cancellation = %v, want nil", err)
			}
		case <-time.After(time.Second):
			t.Error("timed out waiting for Agent channel shutdown")
		}
	})
	return socketPath
}

func shortUnixSocketPath(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".tmp"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp(root, "gp-uds-")
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

func sizedAgentAuthenticationMessage(t *testing.T, target int) *agentpb.AgentMessage {
	t.Helper()
	token := make([]byte, target)
	message := &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_Authenticate{
		Authenticate: &agentpb.Authenticate{AgentId: agentChannelTransportTestAgentID, Token: token},
	}}
	for proto.Size(message) > target {
		token = token[:len(token)-(proto.Size(message)-target)]
		message.GetAuthenticate().Token = token
	}
	if proto.Size(message) != target {
		t.Fatalf("cannot construct %d-byte Agent message; got %d", target, proto.Size(message))
	}
	return message
}

func sizedAgentConfig(t *testing.T, target int) *agentpb.AgentConfig {
	t.Helper()
	payload := strings.Repeat("x", target)
	config := transportTestAgentConfig()
	config.Labels = map[string]string{"payload": payload}
	message := &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ConfigUpdate{
		ConfigUpdate: &agentpb.ConfigUpdate{AgentConfig: config},
	}}
	for proto.Size(message) > target {
		payload = payload[:len(payload)-(proto.Size(message)-target)]
		config.Labels["payload"] = payload
	}
	if proto.Size(message) != target {
		t.Fatalf("cannot construct %d-byte Controller message; got %d", target, proto.Size(message))
	}
	return config
}

func transportTestAgentConfig() *agentpb.AgentConfig {
	return &agentpb.AgentConfig{PullIntervalSeconds: 1, MaxConcurrentTasks: 1}
}
