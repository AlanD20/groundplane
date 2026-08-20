// Package agent is the execution plane: an authenticated gRPC stream client
// driving a bounded worker pool for Controller-assigned tasks.
package agent

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type agentStream interface {
	Send(*agentpb.AgentMessage) error
	Recv() (*agentpb.ControllerMessage, error)
	CloseSend() error
}

type streamConnector func(context.Context, string) (agentStream, io.Closer, error)

// Client owns one authenticated, single-use Controller stream. Identity and
// credential material are deliberately private and are never logged.
type Client struct {
	socketPath string
	agentID    string
	token      [agentprotocol.RawTokenBytes]byte
	logger     *slog.Logger
	connect    streamConnector

	mu          sync.Mutex
	started     bool
	pool        *WorkerPool
	workersDone <-chan struct{}
}

func NewClient(socketPath, agentID string, token []byte, logger *slog.Logger) (*Client, error) {
	if socketPath != agentprotocol.SocketPath {
		return nil, errs.New(errs.KindValidationFailed, "agent: invalid Controller socket path")
	}
	if err := ids.Validate(ids.KindAgent, agentID); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "agent: invalid Agent id")
	}
	if len(token) != agentprotocol.RawTokenBytes {
		return nil, errs.Newf(
			errs.KindValidationFailed,
			"agent: channel token must contain exactly %d decoded bytes",
			agentprotocol.RawTokenBytes,
		)
	}
	if logger == nil {
		return nil, errs.New(errs.KindValidationFailed, "agent: logger is required")
	}

	client := &Client{
		socketPath: socketPath,
		agentID:    agentID,
		logger:     logger,
		connect:    connectGRPC,
	}
	copy(client.token[:], token)
	return client, nil
}

// Run authenticates, requires the initial runtime configuration, starts the
// worker pool, and maintains Ready heartbeats until shutdown or cancellation.
func (c *Client) Run(ctx context.Context) error {
	token, err := c.takeToken()
	if err != nil {
		return err
	}
	defer clear(token[:])
	if err := ctx.Err(); err != nil {
		return nil
	}

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, connection, err := c.connect(streamCtx, c.socketPath)
	if err != nil {
		return transportError(ctx, "agent: connect to Controller")
	}
	defer func() {
		// Best effort: stream teardown cannot supersede the primary Run result.
		_ = stream.CloseSend()
		// Best effort: connection teardown cannot supersede the primary Run result.
		_ = connection.Close()
	}()

	authToken := append([]byte(nil), token[:]...)
	clear(token[:])
	authenticate := &agentpb.Authenticate{AgentId: c.agentID, Token: authToken}
	message := &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_Authenticate{Authenticate: authenticate}}
	if err := stream.Send(message); err != nil {
		clear(authToken)
		authenticate.Token = nil
		return transportError(ctx, "agent: send authentication")
	}
	clear(authToken)
	authenticate.Token = nil

	initial, err := stream.Recv()
	if err != nil {
		return transportError(ctx, "agent: receive initial configuration")
	}
	config := initial.GetConfigUpdate().GetAgentConfig()
	if config == nil {
		return errs.New(errs.KindInternal, "agent: Controller did not send configuration first")
	}
	if config.PullIntervalSeconds <= 0 || config.MaxConcurrentTasks <= 0 {
		return errs.New(errs.KindInternal, "agent: Controller sent invalid initial configuration")
	}

	pullInterval := time.Duration(config.PullIntervalSeconds) * time.Second
	c.pool = NewWorkerPool(int(config.MaxConcurrentTasks), runner.New(c.logger), c.logger)
	workersDone := make(chan struct{})
	c.workersDone = workersDone
	go func() {
		defer close(workersDone)
		c.pool.Run(streamCtx)
	}()
	defer func() {
		cancel()
		<-workersDone
	}()
	if err := c.sendReady(stream); err != nil {
		return transportError(ctx, "agent: send readiness")
	}

	ticker := time.NewTicker(pullInterval)
	defer ticker.Stop()
	received := make(chan receiveResult, 1)
	receiveNext(streamCtx, stream, received)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := c.sendReady(stream); err != nil {
				return transportError(ctx, "agent: send readiness")
			}
		case result := <-received:
			if result.err != nil {
				return transportError(ctx, "agent: receive Controller message")
			}
			shutdown, err := c.handleControllerMessage(result.message)
			if err != nil {
				return err
			}
			if shutdown {
				return nil
			}
			receiveNext(streamCtx, stream, received)
		}
	}
}

func (c *Client) takeToken() ([agentprotocol.RawTokenBytes]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return [agentprotocol.RawTokenBytes]byte{}, errs.New(errs.KindInternal, "agent: client has already started")
	}
	c.started = true
	token := c.token
	clear(c.token[:])
	return token, nil
}

func (c *Client) sendReady(stream agentStream) error {
	return stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_Ready{
		Ready: &agentpb.Ready{Capacity: int32(c.pool.Capacity())},
	}})
}

func (c *Client) handleControllerMessage(message *agentpb.ControllerMessage) (bool, error) {
	if assignment := message.GetTaskAssignment(); assignment != nil {
		steps := make([]adapters.Step, 0, len(assignment.Steps))
		for _, step := range assignment.Steps {
			steps = append(steps, adapters.Step{Op: adapters.StepOp(step.Op), Params: step.Params})
		}
		c.pool.Submit(Assignment{TaskID: assignment.TaskId, Steps: steps})
		return false, nil
	}
	if abort := message.GetTaskAbort(); abort != nil {
		c.pool.Abort(abort.TaskId)
		return false, nil
	}
	if message.GetShutdown() != nil {
		return true, nil
	}
	if message.GetConfigUpdate() != nil {
		return false, errs.New(errs.KindNotImplemented, "agent: live configuration update is not implemented")
	}
	return false, errs.New(errs.KindInternal, "agent: Controller sent an empty message")
}

type receiveResult struct {
	message *agentpb.ControllerMessage
	err     error
}

func receiveNext(ctx context.Context, stream agentStream, result chan<- receiveResult) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		message, err := stream.Recv()
		select {
		case result <- receiveResult{message: message, err: err}:
		case <-ctx.Done():
		}
	}()
	return done
}

func connectGRPC(ctx context.Context, socketPath string) (agentStream, io.Closer, error) {
	connection, err := grpc.NewClient(
		"passthrough:///groundplane-agent",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", socketPath)
		}),
	)
	if err != nil {
		return nil, nil, err
	}
	stream, err := agentpb.NewAgentChannelClient(connection).Connect(ctx)
	if err != nil {
		// Best effort: preserve the stream creation failure.
		_ = connection.Close()
		return nil, nil, err
	}
	return stream, connection, nil
}

func transportError(ctx context.Context, message string) error {
	if ctx.Err() != nil {
		return nil
	}
	return errs.New(errs.KindInternal, message)
}
