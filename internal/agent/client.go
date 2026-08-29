// Package agent is the execution plane: an authenticated gRPC stream client
// driving a bounded worker pool for Controller-assigned tasks.
package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	agentChannelAgentMaximumReceiveMessageBytes = 5 * 1024 * 1024
	agentChannelAgentMaximumSendMessageBytes    = 16 * 1024 * 1024
	agentChannelReconnectInitialCeiling         = 100 * time.Millisecond
	agentChannelReconnectMaximumCeiling         = 5 * time.Second
)

type agentStream interface {
	Send(*agentpb.AgentMessage) error
	Recv() (*agentpb.ControllerMessage, error)
	CloseSend() error
}

type streamConnector func(context.Context, string) (agentStream, io.Closer, error)

type reconnectWaiter func(context.Context, uint) error

// Client owns one authenticated, single-use Controller stream. Identity and
// credential material are deliberately private and are never logged.
type Client struct {
	socketPath string
	agentID    string
	token      [agentprotocol.RawTokenBytes]byte
	volumeRoot string
	logger     *slog.Logger
	connect    streamConnector
	reconnect  reconnectWaiter

	mu                     sync.Mutex
	started                bool
	pool                   *WorkerPool
	workersDone            <-chan struct{}
	compose                *ComposeRuntime
	environmentDirectories *EnvironmentDirectoryRuntime
	materializer           *MaterializationRuntime
	componentActions       ComponentActionRuntime
	hostResolution         HostResolutionRuntime
	scriptRuntime          ScriptRuntime
	logs                   *logManager
}

func (c *Client) SetComponentActionRuntime(runtime ComponentActionRuntime) error {
	if c == nil || runtime == nil {
		return errs.New(errs.KindValidationFailed, "agent: Component action runtime is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.pool != nil {
		return errs.New(errs.KindStateConflict, "agent: Component action runtime cannot change after start")
	}
	c.componentActions = runtime
	return nil
}

func (c *Client) SetHostResolutionRuntime(runtime HostResolutionRuntime) error {
	if c == nil || runtime == nil {
		return errs.New(errs.KindValidationFailed, "agent: host resolution runtime is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.pool != nil {
		return errs.New(errs.KindStateConflict, "agent: host resolution runtime cannot change after start")
	}
	c.hostResolution = runtime
	return nil
}

func (c *Client) SetScriptRuntime(runtime ScriptRuntime) error {
	if c == nil || runtime == nil {
		return errs.New(errs.KindValidationFailed, "agent: Script runtime is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.pool != nil {
		return errs.New(errs.KindStateConflict, "agent: Script runtime cannot change after start")
	}
	c.scriptRuntime = runtime
	return nil
}

func NewClient(
	socketPath string,
	agentID string,
	token []byte,
	volumeRoot string,
	logger *slog.Logger,
) (*Client, error) {
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
	if err := environmentpath.ValidateRoot(volumeRoot); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "agent: invalid volume root policy")
	}

	client := &Client{
		socketPath: socketPath,
		agentID:    agentID,
		volumeRoot: volumeRoot,
		logger:     logger,
		connect:    connectGRPC,
		reconnect:  waitForAgentChannelReconnect,
		logs:       newLogManager(nil),
	}
	copy(client.token[:], token)
	return client, nil
}

func NewClientWithRuntimes(
	socketPath string,
	agentID string,
	token []byte,
	volumeRoot string,
	logger *slog.Logger,
	compose *ComposeRuntime,
	environmentDirectories *EnvironmentDirectoryRuntime,
	materializer *MaterializationRuntime,
) (*Client, error) {
	if compose == nil || environmentDirectories == nil || materializer == nil {
		return nil, errs.New(errs.KindValidationFailed, "agent: execution runtimes are required")
	}
	client, err := NewClient(socketPath, agentID, token, volumeRoot, logger)
	if err != nil {
		return nil, err
	}
	client.compose = compose
	client.environmentDirectories = environmentDirectories
	client.materializer = materializer
	return client, nil
}

func NewClientWithLogReader(
	socketPath string,
	agentID string,
	token []byte,
	volumeRoot string,
	logger *slog.Logger,
	compose *ComposeRuntime,
	environmentDirectories *EnvironmentDirectoryRuntime,
	materializer *MaterializationRuntime,
	logReader agentprotocol.LogReader,
) (*Client, error) {
	if logReader == nil {
		return nil, errs.New(errs.KindValidationFailed, "agent: log reader is required")
	}
	client, err := NewClientWithRuntimes(
		socketPath,
		agentID,
		token,
		volumeRoot,
		logger,
		compose,
		environmentDirectories,
		materializer,
	)
	if err != nil {
		return nil, err
	}
	client.logs = newLogManager(logReader)
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
	if c.reconnect == nil {
		return errs.New(errs.KindInternal, "agent: reconnect waiter is not configured")
	}

	var reconnectAttempt uint
	for {
		reconnect, runErr := c.runSession(ctx, &token)
		if runErr != nil || !reconnect {
			return runErr
		}
		c.logger.WarnContext(
			ctx,
			"agent channel interrupted; reconnecting",
			"attempt",
			reconnectAttempt+1,
		)
		if waitErr := c.reconnect(ctx, reconnectAttempt); waitErr != nil {
			if ctx.Err() != nil {
				return nil
			}
			return errs.New(errs.KindInternal, "agent: reconnect wait failed")
		}
		if reconnectAttempt < 63 {
			reconnectAttempt++
		}
	}
}

func (c *Client) runSession(
	ctx context.Context,
	token *[agentprotocol.RawTokenBytes]byte,
) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, nil
	}

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, connection, err := c.connect(streamCtx, c.socketPath)
	if err != nil {
		return agentChannelTransportResult(ctx, err, "agent: connect to Controller")
	}
	defer func() {
		// Best effort: stream teardown cannot supersede the primary Run result.
		_ = stream.CloseSend()
		// Best effort: connection teardown cannot supersede the primary Run result.
		_ = connection.Close()
	}()

	authToken := append([]byte(nil), token[:]...)
	authenticate := &agentpb.Authenticate{AgentId: c.agentID, Token: authToken}
	message := &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_Authenticate{Authenticate: authenticate}}
	if err := stream.Send(message); err != nil {
		clear(authToken)
		authenticate.Token = nil
		return agentChannelTransportResult(ctx, err, "agent: send authentication")
	}
	clear(authToken)
	authenticate.Token = nil

	initial, err := stream.Recv()
	if err != nil {
		return agentChannelTransportResult(ctx, err, "agent: receive initial configuration")
	}
	config := initial.GetConfigUpdate().GetAgentConfig()
	if config == nil {
		return false, errs.New(errs.KindInternal, "agent: Controller did not send configuration first")
	}
	if !validRuntimeConfig(config) {
		return false, errs.New(errs.KindInternal, "agent: Controller sent invalid initial configuration")
	}

	pullInterval := time.Duration(config.PullIntervalSeconds) * time.Second
	config = proto.Clone(config).(*agentpb.AgentConfig)
	poolCancel, workersDone := c.startWorkerPool(streamCtx, int(config.MaxConcurrentTasks))
	defer func() {
		poolCancel()
		<-workersDone
	}()
	if err := c.sendReady(stream); err != nil {
		return agentChannelTransportResult(ctx, err, "agent: send readiness")
	}

	ticker := time.NewTicker(pullInterval)
	defer ticker.Stop()
	received := make(chan receiveResult, 1)
	receiveNext(streamCtx, stream, received)
	for {
		select {
		case <-ctx.Done():
			return false, nil
		case <-ticker.C:
			if err := c.sendReady(stream); err != nil {
				return agentChannelTransportResult(ctx, err, "agent: send readiness")
			}
		case result := <-received:
			if result.err != nil {
				return agentChannelTransportResult(ctx, result.err, "agent: receive Controller message")
			}
			if update := result.message.GetConfigUpdate(); update != nil {
				next := update.GetAgentConfig()
				if !validRuntimeConfig(next) {
					return false, errs.New(errs.KindInternal, "agent: Controller sent invalid live configuration")
				}
				if c.pool.Capacity() != int(config.MaxConcurrentTasks) {
					return false, errs.New(
						errs.KindStateConflict,
						"agent: live configuration arrived before the worker pool drained",
					)
				}
				poolCancel()
				<-workersDone
				config = proto.Clone(next).(*agentpb.AgentConfig)
				poolCancel, workersDone = c.startWorkerPool(streamCtx, int(config.MaxConcurrentTasks))
				pullInterval = time.Duration(config.PullIntervalSeconds) * time.Second
				ticker.Reset(pullInterval)
				receiveNext(streamCtx, stream, received)
				if err := c.sendReady(stream); err != nil {
					return agentChannelTransportResult(
						ctx,
						err,
						"agent: send readiness after configuration update",
					)
				}
				continue
			}
			if acknowledgement := result.message.GetBackupCheckpointAck(); acknowledgement != nil {
				if err := c.pool.AcceptBackupCheckpointAck(acknowledgement); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			if acknowledgement := result.message.GetScriptCheckpointAck(); acknowledgement != nil {
				if err := c.pool.AcceptScriptCheckpointAck(acknowledgement); err != nil {
					return false, err
				}
				receiveNext(streamCtx, stream, received)
				continue
			}
			shutdown, err := c.handleControllerMessage(streamCtx, result.message)
			if err != nil {
				return false, err
			}
			if shutdown {
				return false, nil
			}
			receiveNext(streamCtx, stream, received)
		case output := <-c.pool.Outputs():
			if output.ScriptCheckpoint != nil {
				if output.BackupCheckpoint != nil || output.Progress != nil || output.Result != nil {
					return false, errs.New(errs.KindInternal, "agent: worker returned an invalid output union")
				}
				if err := c.sendScriptCheckpoint(stream, output.ScriptCheckpoint); err != nil {
					return agentChannelTransportResult(ctx, err, "agent: send Script checkpoint")
				}
				continue
			}
			if output.BackupCheckpoint != nil {
				if output.Progress != nil || output.Result != nil || output.ScriptCheckpoint != nil {
					return false, errs.New(errs.KindInternal, "agent: worker returned an invalid output union")
				}
				if err := c.sendBackupCheckpoint(stream, output.BackupCheckpoint); err != nil {
					return agentChannelTransportResult(ctx, err, "agent: send Backup checkpoint")
				}
				continue
			}
			if output.Progress != nil {
				if output.Result != nil {
					return false, errs.New(errs.KindInternal, "agent: worker returned an invalid output union")
				}
				if err := c.sendTaskEvent(stream, *output.Progress); err != nil {
					return agentChannelTransportResult(ctx, err, "agent: send task event")
				}
				continue
			}
			if output.Result == nil {
				return false, errs.New(errs.KindInternal, "agent: worker returned an empty output")
			}
			if err := c.sendTaskAck(stream, *output.Result); err != nil {
				return agentChannelTransportResult(ctx, err, "agent: send task acknowledgement")
			}
			if err := c.sendReady(stream); err != nil {
				return agentChannelTransportResult(ctx, err, "agent: send readiness")
			}
		case message := <-c.logs.Outputs():
			if err := stream.Send(message); err != nil {
				return agentChannelTransportResult(ctx, err, "agent: send log message")
			}
		}
	}
}

func (c *Client) sendBackupCheckpoint(
	stream agentStream,
	request *agentpb.BackupCheckpointRequest,
) error {
	return stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_BackupCheckpointRequest{
		BackupCheckpointRequest: proto.Clone(request).(*agentpb.BackupCheckpointRequest),
	}})
}

func (c *Client) sendScriptCheckpoint(
	stream agentStream,
	request *agentpb.ScriptCheckpointRequest,
) error {
	return stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ScriptCheckpointRequest{
		ScriptCheckpointRequest: proto.Clone(request).(*agentpb.ScriptCheckpointRequest),
	}})
}

func (c *Client) startWorkerPool(ctx context.Context, size int) (context.CancelFunc, <-chan struct{}) {
	poolCtx, cancel := context.WithCancel(ctx)
	var pool *WorkerPool
	if c.compose == nil && c.environmentDirectories == nil && c.materializer == nil {
		pool = NewWorkerPool(size, c.volumeRoot, runner.New(c.logger), c.logger)
	} else {
		pool = NewWorkerPoolWithRuntimes(
			size,
			c.volumeRoot,
			runner.New(c.logger),
			c.logger,
			c.compose,
			c.environmentDirectories,
			c.materializer,
		)
	}
	if c.componentActions != nil {
		pool.SetComponentActionRuntime(c.componentActions)
	}
	if c.hostResolution != nil {
		pool.SetHostResolutionRuntime(c.hostResolution)
	}
	if c.scriptRuntime != nil {
		pool.SetScriptRuntime(c.scriptRuntime)
	}
	c.pool = pool
	done := make(chan struct{})
	c.workersDone = done
	go func() {
		defer close(done)
		pool.Run(poolCtx)
	}()
	return cancel, done
}

func validRuntimeConfig(config *agentpb.AgentConfig) bool {
	if config == nil || config.PullIntervalSeconds <= 0 || config.MaxConcurrentTasks <= 0 {
		return false
	}
	for key, value := range config.Labels {
		if !utf8.ValidString(key) || strings.IndexByte(key, 0) >= 0 ||
			!utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
			return false
		}
	}
	return true
}

func (c *Client) sendTaskEvent(stream agentStream, progress TaskProgress) error {
	state := agentpb.TaskState_TASK_STATE_UNSPECIFIED
	switch progress.State {
	case TaskProgressRunning:
		state = agentpb.TaskState_TASK_STATE_RUNNING
	case TaskProgressCompleted:
		state = agentpb.TaskState_TASK_STATE_COMPLETED
	case TaskProgressFailed:
		state = agentpb.TaskState_TASK_STATE_FAILED
	case TaskProgressTimedOut:
		state = agentpb.TaskState_TASK_STATE_TIMED_OUT
	case TaskProgressAborted:
		state = agentpb.TaskState_TASK_STATE_ABORTED
	default:
		return errs.New(errs.KindInternal, "agent: worker returned an invalid progress state")
	}
	return stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_TaskEvent{
		TaskEvent: &agentpb.TaskEvent{
			AssignmentId: progress.AssignmentID,
			TaskId:       progress.TaskID, PlanHash: append([]byte(nil), progress.PlanHash[:]...),
			StepId: progress.StepID, Attempt: progress.Attempt, Ordinal: progress.Ordinal,
			State: state, Chunk: append([]byte(nil), progress.Chunk...),
		},
	}})
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
		Ready: &agentpb.Ready{Capacity: int32(c.pool.Capacity()), Version: version.Value},
	}})
}

func (c *Client) handleControllerMessage(ctx context.Context, message *agentpb.ControllerMessage) (bool, error) {
	if subscription := message.GetLogSubscribe(); subscription != nil {
		c.logs.Subscribe(ctx, proto.Clone(subscription).(*agentpb.LogSubscribe))
		return false, nil
	}
	if cancellation := message.GetLogCancel(); cancellation != nil {
		c.logs.Cancel(cancellation.GetRequestId())
		return false, nil
	}
	if assignment := message.GetTaskAssignment(); assignment != nil {
		if assignment.Deadline == nil || assignment.Deadline.CheckValid() != nil {
			return false, errs.New(errs.KindInternal, "agent: Controller sent an invalid task deadline")
		}
		defer clearScriptArtifacts(assignment.ScriptArtifacts)
		return false, c.pool.Submit(ctx, Assignment{
			AssignmentID: assignment.AssignmentId,
			TaskID:       assignment.TaskId, OperationID: assignment.OperationId,
			RetryOf: assignment.RetryOf, Plan: assignment.Plan, ScriptArtifacts: assignment.ScriptArtifacts,
			ScriptCheckpoint: assignment.ScriptCheckpoint,
			Deadline:         assignment.Deadline.AsTime(),
		})
	}
	if abort := message.GetTaskAbort(); abort != nil {
		return false, c.pool.Abort(ctx, abort.TaskId, abort.AssignmentId)
	}
	if transfer := message.GetMaterializationTransfer(); transfer != nil {
		return false, c.pool.AcceptMaterializationTransfer(ctx, transfer)
	}
	if transfer := message.GetManagedConfigTransfer(); transfer != nil {
		return false, c.pool.AcceptManagedConfigTransfer(ctx, transfer)
	}
	if transfer := message.GetBackupSecretSlotTransfer(); transfer != nil {
		chunk := transfer.GetChunk()
		if chunk != nil {
			defer func() {
				clear(chunk.Content)
				chunk.Content = nil
			}()
		}
		return false, c.pool.AcceptBackupSecretSlotTransfer(ctx, transfer)
	}
	if message.GetShutdown() != nil {
		return true, nil
	}
	return false, errs.New(errs.KindInternal, "agent: Controller sent an empty message")
}

func (c *Client) sendTaskAck(stream agentStream, result TaskResult) error {
	if (result.Compose == nil) == (result.EnvironmentDirectory == nil) {
		return errs.New(errs.KindInternal, "agent: worker returned an invalid task result union")
	}
	terminal := agentpb.TaskTerminal_TASK_TERMINAL_UNSPECIFIED
	switch result.Terminal {
	case TaskTerminalCompleted:
		terminal = agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED
	case TaskTerminalFailed:
		terminal = agentpb.TaskTerminal_TASK_TERMINAL_FAILED
	case TaskTerminalTimedOut:
		terminal = agentpb.TaskTerminal_TASK_TERMINAL_TIMED_OUT
	case TaskTerminalAborted:
		terminal = agentpb.TaskTerminal_TASK_TERMINAL_ABORTED
	default:
		return errs.New(errs.KindInternal, "agent: worker returned an invalid terminal state")
	}
	acknowledgement := &agentpb.TaskAck{
		AssignmentId: result.AssignmentID,
		TaskId:       result.TaskID, PlanHash: append([]byte(nil), result.PlanHash[:]...), Terminal: terminal,
		ExitCode: result.ExitCode,
	}
	if result.Compose != nil {
		acknowledgement.Result = &agentpb.TaskAck_ComposeResult{
			ComposeResult: proto.Clone(result.Compose).(*agentpb.ComposeTaskResult),
		}
	} else {
		acknowledgement.Result = &agentpb.TaskAck_EnvironmentDirectoryResult{
			EnvironmentDirectoryResult: proto.Clone(result.EnvironmentDirectory).(*agentpb.EnvironmentDirectoryTaskResult),
		}
	}
	return stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_TaskAck{TaskAck: acknowledgement}})
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
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(agentChannelAgentMaximumReceiveMessageBytes),
			grpc.MaxCallSendMsgSize(agentChannelAgentMaximumSendMessageBytes),
		),
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

func agentChannelTransportResult(ctx context.Context, cause error, message string) (bool, error) {
	if ctx.Err() != nil {
		return false, nil
	}
	if errors.Is(cause, io.EOF) {
		return true, nil
	}
	switch status.Code(cause) {
	case codes.Canceled, codes.DeadlineExceeded, codes.Unavailable:
		return true, nil
	default:
		return false, errs.New(errs.KindInternal, message)
	}
}

func waitForAgentChannelReconnect(ctx context.Context, attempt uint) error {
	delay := agentChannelReconnectDelay(attempt, rand.Uint64())
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func agentChannelReconnectDelay(attempt uint, jitter uint64) time.Duration {
	ceiling := agentChannelReconnectInitialCeiling
	for current := uint(0); current < attempt && ceiling < agentChannelReconnectMaximumCeiling; current++ {
		if ceiling > agentChannelReconnectMaximumCeiling/2 {
			ceiling = agentChannelReconnectMaximumCeiling
			break
		}
		ceiling *= 2
	}
	minimum := ceiling / 2
	window := ceiling - minimum
	return minimum + time.Duration(jitter%uint64(window+1))
}
